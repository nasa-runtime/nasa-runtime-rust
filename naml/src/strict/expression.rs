use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use nabase::env::relaxed_env_key;
use serde_json::{Map, Value};

use super::{
    ConfigError, ConfigPath, EnvironmentSnapshot, ErrorKind, LoadPolicy, Result, ValueHint,
};

enum Token {
    Literal(String),
    Reference {
        key: String,
        fallback: Option<Vec<Token>>,
        raw: String,
    },
}

/// 表达式结果与实际访问的字段依赖，默认表示不打印值。
pub struct Resolution {
    /// 已完整求值的候选配置，显式访问时由调用方保护原值。
    pub tree: Value,
    /// 求值字段到实际访问的配置字段集合，不包含未采用的默认分支。
    pub dependencies: BTreeMap<ConfigPath, BTreeSet<ConfigPath>>,
    /// 求值字段到实际访问的环境键集合，不包含环境原值。
    pub environment: BTreeMap<ConfigPath, BTreeSet<String>>,
}

impl fmt::Debug for Resolution {
    /// 业务作用：避免解析结果在诊断中暴露配置正文。
    /// 参数说明：`f` 是格式化目标。
    /// 返回：仅报告依赖规模。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Resolution")
            .field("fields", &self.dependencies.len())
            .finish_non_exhaustive()
    }
}

/// 业务作用：在独立候选树中解析表达式，失败时不修改调用方输入。
/// 参数说明：`tree` 为原始完整树；`environment` 为固定环境；`policy` 为受信规则。
/// 返回：完整解析结果或安全错误。
pub fn resolve(
    tree: &Value,
    environment: &EnvironmentSnapshot,
    policy: &LoadPolicy,
) -> Result<Resolution> {
    validate_tree(tree, policy)?;
    let mut evaluator = Evaluator::new(tree, environment, policy);
    let value = evaluator.node(&ConfigPath::default())?;
    Ok(Resolution {
        tree: value,
        dependencies: evaluator.dependencies,
        environment: evaluator.accessed_environment,
    })
}

/// 业务作用：只解析引导字段及其真实依赖，不提前求值等待 overlay 的业务叶子。
/// 参数说明：`tree` 为原始合并树；`paths` 为所需字段；其余参数固定解析环境。
/// 返回：按请求路径给出的完整引导值，未请求字段不会被传递为已解析值。
pub fn resolve_selected(
    tree: &Value,
    paths: &[ConfigPath],
    environment: &EnvironmentSnapshot,
    policy: &LoadPolicy,
) -> Result<BTreeMap<ConfigPath, Value>> {
    validate_tree(tree, policy)?;
    let mut evaluator = Evaluator::new(tree, environment, policy);
    paths
        .iter()
        .map(|path| evaluator.node(path).map(|value| (path.clone(), value)))
        .collect()
}

struct Evaluator<'a> {
    tree: &'a Value,
    environment: &'a EnvironmentSnapshot,
    policy: &'a LoadPolicy,
    active: BTreeSet<ConfigPath>,
    stack: Vec<ConfigPath>,
    cache: BTreeMap<ConfigPath, Value>,
    dependencies: BTreeMap<ConfigPath, BTreeSet<ConfigPath>>,
    accessed_environment: BTreeMap<ConfigPath, BTreeSet<String>>,
    visits: usize,
    expanded: usize,
    compatible: bool,
    compatible_paths: BTreeMap<String, ConfigPath>,
    literal_inputs: Option<&'a BTreeSet<ConfigPath>>,
}

impl<'a> Evaluator<'a> {
    /// 业务作用：为单个候选创建独立依赖和展开预算。
    /// 参数说明：`tree`、`environment`、`policy` 均在本轮固定。
    /// 返回：尚未访问字段的求值状态。
    fn new(tree: &'a Value, environment: &'a EnvironmentSnapshot, policy: &'a LoadPolicy) -> Self {
        Self {
            tree,
            environment,
            policy,
            active: BTreeSet::new(),
            stack: Vec::new(),
            cache: BTreeMap::new(),
            dependencies: BTreeMap::new(),
            accessed_environment: BTreeMap::new(),
            visits: 0,
            expanded: 0,
            compatible: false,
            compatible_paths: BTreeMap::new(),
            literal_inputs: None,
        }
    }

    /// 业务作用：按节点身份解析依赖并在复制文本之前计入展开预算。
    /// 参数说明：`path` 为当前字段。
    /// 返回：已经完成求值的节点，不会再次把结果当成表达式源码。
    fn node(&mut self, path: &ConfigPath) -> Result<Value> {
        self.policy.check_cancelled()?;
        self.visits = self
            .visits
            .checked_add(1)
            .ok_or_else(|| ConfigError::new(ErrorKind::Limit))?;
        if self.visits > self.policy.limits.dependency_visits
            || path.0.len() > self.policy.limits.depth
            || self.active.len() > self.policy.limits.depth
        {
            return Err(ConfigError::new(ErrorKind::Limit).at(path));
        }
        if let Some(cached) = self.cache.get(path) {
            let size = cached.as_str().map_or(0, str::len);
            self.charge(size, path)?;
            return Ok(self.cache[path].clone());
        }
        if !self.active.insert(path.clone()) {
            let mut error = ConfigError::new(ErrorKind::Cycle).at(path);
            let first = self.stack.iter().position(|node| node == path).unwrap_or(0);
            error.cycle.extend(self.stack[first..].iter().cloned());
            error.cycle.push(path.clone());
            return Err(error);
        }
        self.stack.push(path.clone());
        let raw = path
            .get(self.tree)
            .ok_or_else(|| ConfigError::new(ErrorKind::Unresolved).at(path))?;
        let value = match raw {
            Value::Object(map) => {
                let mut output = Map::new();
                for key in map.keys() {
                    output.insert(key.clone(), self.node(&path.key(key))?);
                }
                Value::Object(output)
            }
            Value::Array(values) => {
                let mut output = Vec::new();
                for index in 0..values.len() {
                    output.push(self.node(&path.index(index))?);
                }
                Value::Array(output)
            }
            Value::String(text) => {
                let literal =
                    !self.policy.placeholders || self.policy.hint(path) == Some(ValueHint::Literal);
                if literal
                    || self
                        .literal_inputs
                        .is_some_and(|paths| paths.iter().any(|root| path.starts_with(root)))
                {
                    self.charge(text.len(), path)?;
                    Value::String(text.clone())
                } else {
                    let tokens = parse_tokens(text, self.policy, self.compatible)
                        .map_err(|error| error.at(path))?;
                    self.tokens(&tokens, path, 0, true)?
                }
            }
            value => value.clone(),
        };
        let value = self.apply_hint(value, path)?;
        self.active.remove(path);
        self.stack.pop();
        if !value.is_object() && !value.is_array() {
            self.cache.insert(path.clone(), value.clone());
        }
        Ok(value)
    }

    /// 业务作用：逐段拼接文本，整值引用保持被引用标量的类型。
    /// 参数说明：`tokens` 是已校验语法；`path` 是当前节点；`depth` 为默认分支深度；`whole` 表示外层允许整值标量推断。
    /// 返回：选中分支的值，未使用分支不触发依赖访问。
    fn tokens(
        &mut self,
        tokens: &[Token],
        path: &ConfigPath,
        depth: usize,
        whole: bool,
    ) -> Result<Value> {
        if depth > self.policy.limits.depth {
            return Err(ConfigError::new(ErrorKind::Limit).at(path));
        }
        if let [Token::Reference { key, fallback, raw }] = tokens {
            return self.reference(key, fallback.as_deref(), raw, path, depth, whole);
        }
        let mut output = String::new();
        for token in tokens {
            let text = match token {
                Token::Literal(text) => {
                    self.charge(text.len(), path)?;
                    text.clone()
                }
                Token::Reference { key, fallback, raw } => scalar_text(
                    self.reference(key, fallback.as_deref(), raw, path, depth, false)?,
                    path,
                )?,
            };
            let next = output
                .len()
                .checked_add(text.len())
                .ok_or_else(|| ConfigError::new(ErrorKind::Limit).at(path))?;
            if next > self.policy.limits.string_bytes {
                return Err(ConfigError::new(ErrorKind::Limit).at(path));
            }
            output.push_str(&text);
        }
        Ok(Value::String(output))
    }

    /// 业务作用：保持配置、原样环境、规范化环境和默认值的既有优先级。
    /// 参数说明：`key` 为引用键；`fallback` 为默认分支；`raw` 用于合法未命中保留；`path` 为当前字段；`depth` 为分支深度；`whole` 表示整值引用。
    /// 返回：标量命中；禁止容器隐式字符串化，自引用只跳过树候选。
    fn reference(
        &mut self,
        key: &str,
        fallback: Option<&[Token]>,
        raw: &str,
        path: &ConfigPath,
        depth: usize,
        whole: bool,
    ) -> Result<Value> {
        let reference = if self.compatible {
            // 兼容入口按原映射遍历顺序处理点分名称冲突，自引用仍按原拼接名称跳过。
            if compatible_key(path)? == key {
                None
            } else {
                self.compatible_paths.get(key).cloned().or_else(|| {
                    ConfigPath::parse(key).ok().filter(|path| {
                        path.0
                            .iter()
                            .any(|part| matches!(part, super::PathSegment::Index(_)))
                    })
                })
            }
        } else {
            Some(ConfigPath::parse(key).map_err(|error| error.at(path))?)
                .filter(|reference| reference != path)
        };
        if let Some(reference) = reference {
            if let Some(value) = reference.get(self.tree) {
                let scalar = !value.is_null() && !value.is_array() && !value.is_object();
                if !scalar && !self.compatible {
                    return Err(ConfigError::new(ErrorKind::Binding).at(path));
                }
                if scalar {
                    self.dependencies
                        .entry(path.clone())
                        .or_default()
                        .insert(reference.clone());
                    let resolved = self.node(&reference)?;
                    if resolved.is_null() || resolved.is_object() || resolved.is_array() {
                        return Err(ConfigError::new(ErrorKind::Binding).at(path));
                    }
                    return Ok(resolved);
                }
            }
        }
        if self.policy.environment_fallback {
            let relaxed = relaxed_env_key(key);
            for candidate in [key, relaxed.as_str()] {
                if let Some(value) = self
                    .environment
                    .fallback(candidate)
                    .map_err(|error| error.at(path))?
                {
                    self.charge(value.len(), path)?;
                    self.accessed_environment
                        .entry(path.clone())
                        .or_default()
                        .insert(candidate.into());
                    return Ok(if self.compatible && whole {
                        crate::placeholder::parse_scalar(value)
                    } else {
                        Value::String(value.into())
                    });
                }
            }
        }
        if let Some(tokens) = fallback {
            let value = self.tokens(tokens, path, depth + 1, whole)?;
            return Ok(match value {
                Value::String(text)
                    if self.compatible && whole && !matches!(tokens, [Token::Reference { .. }]) =>
                {
                    // 默认分支的源码空白已在解析时裁剪，引用命中值的空白不能在求值后再次丢弃。
                    // 单个嵌套引用已决定其类型，只有默认文本继续使用兼容标量推断。
                    crate::placeholder::parse_scalar(&text)
                }
                value => value,
            });
        }
        if self.policy.preserve_unresolved {
            self.charge(raw.len(), path)?;
            return Ok(Value::String(raw.into()));
        }
        Err(ConfigError::new(ErrorKind::Unresolved).at(path))
    }

    /// 业务作用：在每次复制文本前限制单值和候选累计展开量。
    /// 参数说明：`bytes` 为新增字节数；`path` 为当前字段。
    /// 返回：预算内成功，超限时不继续扩展。
    fn charge(&mut self, bytes: usize, path: &ConfigPath) -> Result<()> {
        self.expanded = self
            .expanded
            .checked_add(bytes)
            .ok_or_else(|| ConfigError::new(ErrorKind::Limit).at(path))?;
        if bytes > self.policy.limits.string_bytes
            || self.expanded > self.policy.limits.expanded_bytes
        {
            Err(ConfigError::new(ErrorKind::Limit).at(path))
        } else {
            Ok(())
        }
    }

    /// 业务作用：仅按受信字段提示转换文本，不从字段名猜测密码或端口。
    /// 参数说明：`value` 是完成解析的节点；`path` 用于查找策略。
    /// 返回：提示允许的值类型，显式 JSON 失败不会退回普通字符串。
    fn apply_hint(&self, value: Value, path: &ConfigPath) -> Result<Value> {
        match (self.policy.hint(path), value) {
            (Some(ValueHint::String), Value::String(text)) => Ok(Value::String(text)),
            (Some(ValueHint::String), _) => Err(ConfigError::new(ErrorKind::Binding).at(path)),
            (Some(ValueHint::Scalar), Value::String(text)) => {
                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                    if value.is_boolean() || value.is_number() {
                        return Ok(value);
                    }
                }
                Ok(Value::String(text))
            }
            (Some(ValueHint::Json), Value::String(text)) => {
                let document = super::SourceDocument::new(
                    "field",
                    crate::ConfigFormat::Json,
                    format!("{{\"value\":{text}}}"),
                );
                let parsed = document
                    .parse(0, self.policy)
                    .map_err(|error| error.at(path))?;
                Ok(parsed.value["value"].clone())
            }
            (_, value) => Ok(value),
        }
    }
}

/// 业务作用：仅允许标量参与文本拼接，避免悄悄复制复杂结构。
/// 参数说明：`value` 为引用结果；`path` 为引用位置。
/// 返回：不附加 JSON 字符串引号的文本。
fn scalar_text(value: Value, path: &ConfigPath) -> Result<String> {
    match value {
        Value::String(text) => Ok(text),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        _ => Err(ConfigError::new(ErrorKind::Binding).at(path)),
    }
}

/// 业务作用：完整校验表达式结构，包括不参与求值的默认分支。
/// 参数说明：`text` 为原始字段；`policy` 限制语法开销；`compatible` 保留旧入口的字面键查找。
/// 返回：带字面量保护的语法节点。
fn parse_tokens(text: &str, policy: &LoadPolicy, compatible: bool) -> Result<Vec<Token>> {
    let mut position = 0;
    let mut budget = SyntaxBudget {
        nodes: policy.limits.nodes,
        raw_bytes: policy.limits.expanded_bytes,
        preserve: policy.preserve_unresolved,
        compatible,
    };
    sequence(
        text,
        &mut position,
        false,
        0,
        policy.limits.depth,
        &mut budget,
    )
}

struct SyntaxBudget {
    nodes: usize,
    raw_bytes: usize,
    preserve: bool,
    compatible: bool,
}
impl SyntaxBudget {
    /// 业务作用：在语法节点分配前扣除共享数量预算。
    /// 参数说明：无。
    /// 返回：仍有预算时成功，超限立即拒绝。
    fn node(&mut self) -> Result<()> {
        self.nodes = self
            .nodes
            .checked_sub(1)
            .ok_or_else(|| ConfigError::new(ErrorKind::Limit))?;
        Ok(())
    }
}

/// 业务作用：用嵌套边界解析默认值，普通文本的右括号保持字面含义。
/// 参数说明：`text` 是原文；`position` 是游标；`nested` 表示右括号结束当前分支；`depth/limit` 限制栈深度；`budget` 约束节点与原文副本。
/// 返回：当前序列；兼容默认分支先裁剪源码两侧空白，未闭合或动态 key 被拒绝。
fn sequence(
    text: &str,
    position: &mut usize,
    nested: bool,
    depth: usize,
    limit: usize,
    budget: &mut SyntaxBudget,
) -> Result<Vec<Token>> {
    if depth > limit {
        return Err(ConfigError::new(ErrorKind::Limit));
    }
    let mut tokens = Vec::new();
    let mut literal = String::new();
    while *position < text.len() {
        let rest = &text[*position..];
        if nested && rest.starts_with('}') {
            break;
        }
        if rest.starts_with("$${") {
            let start = *position + 1;
            *position += 3;
            let mut balance = 1usize;
            while *position < text.len() && balance > 0 {
                if text[*position..].starts_with("${") {
                    balance += 1;
                    *position += 2;
                } else {
                    let ch = text[*position..]
                        .chars()
                        .next()
                        .ok_or_else(|| ConfigError::new(ErrorKind::Expression))?;
                    if ch == '}' {
                        balance -= 1;
                    }
                    *position += ch.len_utf8();
                }
                if balance > limit {
                    return Err(ConfigError::new(ErrorKind::Limit));
                }
            }
            if balance != 0 {
                return Err(ConfigError::new(ErrorKind::Expression));
            }
            literal.push_str(&text[start..*position]);
        } else if rest.starts_with("${") {
            if !literal.is_empty() {
                budget.node()?;
                tokens.push(Token::Literal(std::mem::take(&mut literal)));
            }
            let start = *position;
            *position += 2;
            let key_start = *position;
            while *position < text.len() && !matches!(text.as_bytes()[*position], b':' | b'}') {
                if matches!(text.as_bytes()[*position], b'$' | b'{') {
                    return Err(ConfigError::new(ErrorKind::Expression));
                }
                *position += 1;
            }
            let key = text[key_start..*position].trim().to_string();
            if budget.compatible {
                if key.is_empty() || key.len() > 4096 {
                    return Err(ConfigError::new(ErrorKind::Expression));
                }
            } else {
                ConfigPath::parse(&key)?;
            }
            let fallback = if text.as_bytes().get(*position) == Some(&b':') {
                *position += 1;
                Some(sequence(text, position, true, depth + 1, limit, budget)?)
            } else {
                None
            };
            if text.as_bytes().get(*position) != Some(&b'}') {
                return Err(ConfigError::new(ErrorKind::Expression));
            }
            *position += 1;
            budget.node()?;
            let raw = if budget.preserve {
                budget.raw_bytes = budget
                    .raw_bytes
                    .checked_sub(*position - start)
                    .ok_or_else(|| ConfigError::new(ErrorKind::Limit))?;
                text[start..*position].into()
            } else {
                String::new()
            };
            tokens.push(Token::Reference { key, fallback, raw });
        } else {
            let ch = rest
                .chars()
                .next()
                .ok_or_else(|| ConfigError::new(ErrorKind::Expression))?;
            literal.push(ch);
            *position += ch.len_utf8();
        }
    }
    if !literal.is_empty() {
        budget.node()?;
        tokens.push(Token::Literal(literal));
    }
    if nested && budget.compatible {
        // 只裁剪默认分支边缘的源码字面量，整值和内嵌位置共享规则；随后取得的环境和树值保持原文。
        if let Some(Token::Literal(text)) = tokens.first_mut() {
            let start = text.len() - text.trim_start().len();
            text.drain(..start);
        }
        if let Some(Token::Literal(text)) = tokens.last_mut() {
            text.truncate(text.trim_end().len());
        }
        tokens.retain(|token| !matches!(token, Token::Literal(text) if text.is_empty()));
    }
    Ok(tokens)
}

/// 业务作用：为旧入口保留整值环境与默认值的标量推断，同时复用嵌套语法和依赖边界。
/// 参数说明：`tree` 为候选；`environment` 与 `policy` 固定本轮输入。
/// 返回：完整解析结果，不更改树命中字符串的类型。
pub(crate) fn resolve_compatible(
    tree: &Value,
    environment: &EnvironmentSnapshot,
    policy: &LoadPolicy,
) -> Result<Resolution> {
    validate_tree_keys(tree, policy, true)?;
    let mut evaluator = Evaluator::new(tree, environment, policy);
    evaluator.compatible = true;
    evaluator.compatible_paths = compatible_paths(tree, policy)?;
    let value = evaluator.node(&ConfigPath::default())?;
    Ok(Resolution {
        tree: value,
        dependencies: evaluator.dependencies,
        environment: evaluator.accessed_environment,
    })
}

/// 业务作用：在分配候选和语法节点前校验直接树输入及合并后的整体规模。
/// 参数说明：`tree` 是来源树；`policy` 限制结构深度、节点和文本。
/// 返回：预算内成功，过深或过大的树不会进入求值。
fn validate_tree(tree: &Value, policy: &LoadPolicy) -> Result<()> {
    validate_tree_keys(tree, policy, false)
}

/// 业务作用：独立校验树规模与键模型，使严格键约束不改变兼容入口的可接受字段。
/// 参数说明：`tree/policy` 固定输入和预算；`compatible` 允许原有字面键但仍限制规模。
/// 返回：结构与规模合法时成功，严格入口另外拒绝含糊的字段名称。
fn validate_tree_keys(tree: &Value, policy: &LoadPolicy, compatible: bool) -> Result<()> {
    let mut stack = vec![(tree, 0usize)];
    let mut nodes = 0usize;
    let mut bytes = 0usize;
    while let Some((value, depth)) = stack.pop() {
        policy.check_cancelled()?;
        nodes = nodes
            .checked_add(1)
            .ok_or_else(|| ConfigError::new(ErrorKind::Limit))?;
        if nodes > policy.limits.nodes || depth > policy.limits.depth {
            return Err(ConfigError::new(ErrorKind::Limit));
        }
        match value {
            Value::Object(map) => {
                if nodes.saturating_add(stack.len()).saturating_add(map.len()) > policy.limits.nodes
                {
                    return Err(ConfigError::new(ErrorKind::Limit));
                }
                for (key, value) in map {
                    if key.len() > 4096 {
                        return Err(ConfigError::new(ErrorKind::Limit));
                    }
                    if !compatible
                        && (key.is_empty()
                            || key.contains(['.', '[', ']'])
                            || key.chars().any(char::is_control))
                    {
                        return Err(ConfigError::new(ErrorKind::PathConflict));
                    }
                    bytes = bytes.saturating_add(key.len());
                    stack.push((value, depth + 1));
                }
            }
            Value::Array(values) => {
                if nodes
                    .saturating_add(stack.len())
                    .saturating_add(values.len())
                    > policy.limits.nodes
                {
                    return Err(ConfigError::new(ErrorKind::Limit));
                }
                stack.extend(values.iter().map(|value| (value, depth + 1)));
            }
            Value::String(text) => {
                if text.len() > policy.limits.string_bytes {
                    return Err(ConfigError::new(ErrorKind::Limit));
                }
                bytes = bytes.saturating_add(text.len());
            }
            _ => {}
        }
        if bytes > policy.limits.output_bytes {
            return Err(ConfigError::new(ErrorKind::Limit));
        }
    }
    Ok(())
}

/// 业务作用：复用兼容入口的点分名称身份，包括自引用与同名路径的覆盖判定。
/// 参数说明：`path` 是树中真实节点的位置，字段名不会被重新拆分。
/// 返回：有界的旧式名称；超限时停止构建索引。
fn compatible_key(path: &ConfigPath) -> Result<String> {
    let mut name = String::new();
    for part in &path.0 {
        match part {
            super::PathSegment::Key(key) => {
                if !name.is_empty() {
                    name.push('.');
                }
                name.push_str(key);
            }
            super::PathSegment::Index(index) => {
                use std::fmt::Write;
                write!(name, "[{index}]").expect("string formatting");
            }
        }
        if name.len() > 4096 {
            return Err(ConfigError::new(ErrorKind::Limit));
        }
    }
    Ok(name)
}

/// 业务作用：保留旧入口的标量点分映射，名称冲突由原对象遍历中后遇到的字段覆盖。
/// 参数说明：`tree` 为已校验规模的原树；`policy` 限制索引文本总量。
/// 返回：旧式名称到真实结构路径的索引；数组仍不进入旧式映射。
fn compatible_paths(tree: &Value, policy: &LoadPolicy) -> Result<BTreeMap<String, ConfigPath>> {
    let mut result = BTreeMap::new();
    let mut stack = vec![(ConfigPath::default(), tree)];
    let mut bytes = 0usize;
    while let Some((path, value)) = stack.pop() {
        policy.check_cancelled()?;
        match value {
            Value::Object(map) => {
                for (key, value) in map.iter().rev() {
                    stack.push((path.key(key), value));
                }
            }
            Value::String(_) | Value::Number(_) | Value::Bool(_) => {
                let name = compatible_key(&path)?;
                bytes = bytes.saturating_add(name.len());
                if bytes > policy.limits.output_bytes {
                    return Err(ConfigError::new(ErrorKind::Limit));
                }
                result.insert(name, path);
            }
            _ => {}
        }
    }
    Ok(result)
}

/// 业务作用：将环境覆盖视为已取得的原始文本，只允许受信类型提示进行转换。
/// 参数说明：`tree/environment/policy` 是固定候选输入；`literal_inputs` 标识环境覆盖子树。
/// 返回：不会再次解释环境内容的完整树。
pub(super) fn resolve_overlaid(
    tree: &Value,
    environment: &EnvironmentSnapshot,
    policy: &LoadPolicy,
    literal_inputs: &BTreeSet<ConfigPath>,
) -> Result<Resolution> {
    validate_tree(tree, policy)?;
    let mut evaluator = Evaluator::new(tree, environment, policy);
    evaluator.literal_inputs = Some(literal_inputs);
    let value = evaluator.node(&ConfigPath::default())?;
    Ok(Resolution {
        tree: value,
        dependencies: evaluator.dependencies,
        environment: evaluator.accessed_environment,
    })
}

/// 业务作用：引导依赖闭包也保留环境原文，避免先后阶段采用不同解释规则。
/// 参数说明：`paths` 是必要引导字段；其余参数为固定候选与环境覆盖位置。
/// 返回：仅请求字段的已解析投影。
pub(super) fn resolve_selected_overlaid(
    tree: &Value,
    paths: &[ConfigPath],
    environment: &EnvironmentSnapshot,
    policy: &LoadPolicy,
    literal_inputs: &BTreeSet<ConfigPath>,
) -> Result<BTreeMap<ConfigPath, Value>> {
    validate_tree(tree, policy)?;
    let mut evaluator = Evaluator::new(tree, environment, policy);
    evaluator.literal_inputs = Some(literal_inputs);
    paths
        .iter()
        .map(|path| evaluator.node(path).map(|value| (path.clone(), value)))
        .collect()
}
