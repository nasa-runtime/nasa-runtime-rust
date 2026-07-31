//! `${a.b.c}` / `${a.b.c:default}` 占位符解析;config crate 自身不支持该语义。
//!
//! 流程:合并后的 `serde_json::Value` 树 → 多趟替换(最多 10 趟,支持链式引用)→
//! 终扫兜底(树里仍残留 `${` 即返回 Err)。作用于【所有字符串叶子】,可引用【任意点分路径键】。
//!
//! 每个 `${key}` 的解析顺序
//! 1. 配置树点分 key(**当前路径自身除外**,见 [`resolve_placeholder_value`] 的自引用说明);
//! 2. 系统环境变量原样 key;
//! 3. relaxed 环境变量 key(`.`/`-` 转 `_` 后大写,如 `${server.port}` → `SERVER_PORT`);
//! 4. `${key:default}` 形态 → 用默认值;
//! 5. 仍未命中 → **返回 Err(fail-fast)**,不再原样保留——原样保留会把问题推迟成更隐蔽的
//!    反序列化错误或运行期脏值,启动阶段就该把「配错/漏配」暴露出来。
//!
//! 这是占位符 fallback,不是配置覆盖层:不走也不影响 `APP__...` 前缀的环境变量覆盖规则
//! (`${server.port}` 对应 `SERVER_PORT`,不是 `APP__SERVER__PORT`——占位符是「按名取值」,
//! 覆盖层是「按结构注入」,两套命名规则刻意不混用)。
//!
//! 整值占位符(整个字符串只有一个占位符,如 `port: ${server.port:8080}`)的结果类型化规则
//!:**树命中直接沿用被引用节点的原始 JSON 类型**(字符串 `"8080"`
//! 复制后仍是字符串,number/bool 仍是 number/bool——token/ID 这类「形似数字的字符串」不得被
//! 猜成 number);env/default 只有字符串来源,才按 YAML 标量猜类型(number/bool/string),
//! 否则 `"8080"` 字符串灌不进 `u16` 字段。字符串内嵌占位符(如 `url: http://x:${port}/api`)
//! 永远保持字符串拼接语义。

use nabase::env::relaxed_env_key;
use std::collections::HashMap;

/// 最多解析趟数:每趟用上一趟结果重建「键 → 值」映射,支持链式引用逐层解开。
/// 10 趟对真实配置绰绰有余;超过仍在变化 → 疑似循环引用,交给终扫报错。
const MAX_ROUNDS: usize = 10;

/// 一个 `${...}` 占位符的结构化拆解。
/// 只按【第一个】`:` 分割 key 与 default;default 里的 `/`、`-`、`.`、`_`、`:` 都是普通字符
/// (`${server.context-path:/fore-rest}` 的默认值就是字面量 `/fore-rest`,`/` 不是路径语义、
/// `-` 不是分隔语义——默认值是「用户想要的最终字符串内容」,组件不得二次解释)。
struct PlaceholderSpec<'a> {
    /// 原文(含 `${}`),只用于错误信息定位。
    raw: &'a str,
    /// 点分 key(两端已 trim)。
    key: &'a str,
    /// 默认值(两端已 trim;要保留首尾空格用引号)。`None` = 无默认值形态 `${key}`。
    default: Option<&'a str>,
}

impl<'a> PlaceholderSpec<'a> {
    /// 从 `${` 与 `}` 之间的内容解析。`inner` 是 `${` 后、`}` 前的原文。
    ///
    /// # 参数
    /// - `raw`: 待解析的原始字符串、字节或配置值。
    /// - `inner`: 已解析的内部值或被包装对象。
    fn parse(raw: &'a str, inner: &'a str) -> Self {
        match inner.find(':') {
            Some(i) => Self {
                raw,
                key: inner[..i].trim(),
                default: Some(inner[i + 1..].trim()),
            },
            None => Self {
                raw,
                key: inner.trim(),
                default: None,
            },
        }
    }
}

/// 占位符命中来源:树命中携带【原始 JSON Value】保住类型;
/// env / default 天然只有字符串来源,类型化时才允许按标量猜。
enum ResolvedValue {
    Tree(serde_json::Value),
    Env(String),
    Default(String),
}

impl ResolvedValue {
    /// 字符串内嵌位置的取值:无论来源一律转字符串拼接。
    /// 树命中的 String 取原文(`Value::to_string` 会带 JSON 引号,不可用);number/bool 走 to_string。
    fn into_string(self) -> String {
        match self {
            ResolvedValue::Tree(serde_json::Value::String(s)) => s,
            ResolvedValue::Tree(v) => v.to_string(),
            ResolvedValue::Env(s) | ResolvedValue::Default(s) => s,
        }
    }

    /// 整值位置的取值:树命中直接沿用原始 Value(类型保真,——`token: "8080"` 被
    /// `${token}` 引用后必须仍是字符串);env/default 只有字符串来源,按 YAML 标量类型化。
    fn into_value(self) -> serde_json::Value {
        match self {
            ResolvedValue::Tree(v) => v,
            ResolvedValue::Env(s) | ResolvedValue::Default(s) => parse_scalar(&s),
        }
    }
}

/// 在 Value 树上【原地】解析占位符,最多内部设定趟数(到稳定为止)。
///
/// # 参数
/// - `tree`: 待解析的配置 Value 树,函数会原地替换其中的 `${...}` 占位符。
///
/// 稳定判定:某趟没有发生任何替换即视为稳定,提前进入终扫。
/// 失败路径有两条,都必须 `Err`、不能原样保留,否则启动后才炸:
/// 1. 键在树/env 都不存在且无默认值 → 当趟立即报错(含互换型循环 `a:${b}`+`b:${a}`——
///    值互换一趟后变成自引用,被自引用跳过规则拦下,**不保证跑满 10 趟**);
/// 2. 跑满趟数仍有残留(奇数长度轮转循环、未闭合 `${`、嵌套默认值)→ 终扫兜底报错。
///
/// 共享库只返回结构化错误,由应用启动入口 `?` 传播后退出,不直接 `process::exit`。
pub fn resolve_placeholders(tree: &mut serde_json::Value) -> anyhow::Result<()> {
    resolve_placeholders_inner(tree, false)
}

/// 在 Value 树上解析占位符,但未命中的占位符保留为字面量。
///
/// 用途:调用方的配置里同时存在「加载期配置占位符」和「运行期 DSL 变量」时,前者照常解析,
/// 后者保留给调用方自己的运行期解释器处理。
///
///
/// # 参数
/// - `tree`: 配置 Value 树。
pub fn resolve_placeholders_preserving_unresolved(
    tree: &mut serde_json::Value,
) -> anyhow::Result<()> {
    resolve_placeholders_inner(tree, true)
}
///
/// # 参数
/// - `tree`: 配置 Value 树。
/// - `preserve_unresolved`: 是否保留未解析占位符供后续 overlay 继续处理。
fn resolve_placeholders_inner(
    tree: &mut serde_json::Value,
    preserve_unresolved: bool,
) -> anyhow::Result<()> {
    for _ in 0..MAX_ROUNDS {
        // 每趟都用【当前树】重建映射:前一趟的替换可能让更多引用变得可解析(链式)。
        let mut map = HashMap::new();
        flatten(tree, "", &mut map);
        if !replace_in_tree(tree, "", &map, preserve_unresolved)? {
            break; // 本趟无替换 → 已稳定。
        }
    }
    if preserve_unresolved {
        Ok(())
    } else {
        // 终扫兜底:无论是提前稳定还是跑满趟数,残留 `${` 一律报错。
        scan_unresolved(tree, "")
    }
}

/// 终扫:定位第一个仍含 `${` 的字符串叶子并报错。
/// 走到这里说明多趟替换没能消掉它——轮转型循环引用(值在多个键间轮转,每趟都是「树命中」)、
/// 未闭合 `${`,或嵌套默认值等第一版不支持的写法。
///
/// # 参数
/// - `v`: 待转换的值。
/// - `path`: 当前字符串叶子在配置树中的点分路径,用于错误定位。
fn scan_unresolved(v: &serde_json::Value, path: &str) -> anyhow::Result<()> {
    match v {
        serde_json::Value::Object(m) => {
            for (k, val) in m {
                scan_unresolved(val, &join_path(path, k))?;
            }
        }
        serde_json::Value::Array(a) => {
            for (i, val) in a.iter().enumerate() {
                scan_unresolved(val, &format!("{path}[{i}]"))?;
            }
        }
        serde_json::Value::String(s) => {
            anyhow::ensure!(
                !s.contains("${"),
                "yml: 占位符 {MAX_ROUNDS} 轮后仍未解析 at `{path}`: `{s}`(疑似循环引用、未闭合 `${{` 或嵌套默认值;嵌套默认值第一版不支持)"
            );
        }
        _ => {}
    }
    Ok(())
}

/// 把 Value 树压平成「点分路径 → 原始标量 Value」映射(:保留 string/number/bool
/// 的原始类型,整值引用时不得把字符串 `"8080"` 猜成 number)。
/// 数组不进映射(无法用点分路径引用数组元素),但数组里字符串叶子的占位符仍会被
/// [`replace_in_tree`] 替换——两边职责分开:flatten 只建可被引用的标量映射。
///
/// # 参数
/// - `v`: 待转换的值。
/// - `prefix`: 当前节点在配置树中的点分路径前缀。
/// - `out`: 输出缓冲区,用于收集解析结果。
fn flatten(v: &serde_json::Value, prefix: &str, out: &mut HashMap<String, serde_json::Value>) {
    match v {
        serde_json::Value::Object(m) => {
            for (k, val) in m {
                flatten(val, &join_path(prefix, k), out);
            }
        }
        serde_json::Value::String(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::Bool(_) => {
            out.insert(prefix.to_string(), v.clone());
        }
        _ => {}
    }
}

/// 拼接点分路径(根前缀为空时不加点)。
///
/// # 参数
/// - `prefix`: 父节点在配置树中的点分路径。
/// - `key`: 当前子节点的配置 key。
fn join_path(prefix: &str, key: &str) -> String {
    if prefix.is_empty() {
        key.to_string()
    } else {
        format!("{prefix}.{key}")
    }
}

/// 遍历 Value 树,对每个字符串叶子做占位符替换;返回本趟是否发生过替换。
/// `path` = 当前节点的点分路径:整值占位符需要它做自引用判定,错误信息需要它定位。
///
/// # 参数
/// - `v`: 待转换的值。
/// - `path`: 当前节点在配置树中的点分路径,用于自引用判定和错误定位。
/// - `map`: 当前函数读取或更新的键值映射。
/// - `preserve_unresolved`: 是否保留未解析占位符供后续 overlay 继续处理。
fn replace_in_tree(
    v: &mut serde_json::Value,
    path: &str,
    map: &HashMap<String, serde_json::Value>,
    preserve_unresolved: bool,
) -> anyhow::Result<bool> {
    let mut changed = false;
    match v {
        serde_json::Value::Object(m) => {
            for (k, val) in m.iter_mut() {
                changed |= replace_in_tree(val, &join_path(path, k), map, preserve_unresolved)?;
            }
        }
        serde_json::Value::Array(a) => {
            // 数组元素不参与 flatten,路径只为错误定位(`items[0]` 风格)。
            for (i, val) in a.iter_mut().enumerate() {
                changed |= replace_in_tree(val, &format!("{path}[{i}]"), map, preserve_unresolved)?;
            }
        }
        serde_json::Value::String(s) => {
            // 先试整值占位符:命中则整节点换成保真/类型化后的标量——
            // 否则 `port: ${server.port:8080}` 解析成字符串 "8080",u16 字段反序列化必失败。
            if let Some(new) = substitute_whole_value(s, path, map, preserve_unresolved)? {
                if *v != new {
                    *v = new;
                    changed = true;
                }
            } else {
                let (new, c) = substitute_string(s, path, map, preserve_unresolved)?;
                if c && *s != new {
                    *s = new;
                    changed = true;
                }
            }
        }
        _ => {}
    }
    Ok(changed)
}

/// 整值占位符判定 + 替换:整个字符串恰好是一个 `${...}`(第一个 `}` 即最后一个字符)。
/// 返回 `Ok(None)` = 不是整值形态,走字符串内嵌路径;`Ok(Some(v))` = 解析出的标量。
///
/// # 参数
/// - `raw`: 待解析的原始字符串、字节或配置值。
/// - `current_path`: 当前配置值的点分路径,用于跳过自引用。
/// - `map`: 当前函数读取或更新的键值映射。
/// - `preserve_unresolved`: 是否保留未解析占位符供后续 overlay 继续处理。
fn substitute_whole_value(
    raw: &str,
    current_path: &str,
    map: &HashMap<String, serde_json::Value>,
    preserve_unresolved: bool,
) -> anyhow::Result<Option<serde_json::Value>> {
    let Some(inner_start) = raw.strip_prefix("${") else {
        return Ok(None);
    };
    // 占位符终止于【第一个】`}`(与内嵌扫描一致);它必须恰好是字符串最后一个字符才算整值。
    let Some(end) = inner_start.find('}') else {
        return Ok(None); // 未闭合:原样保留,交给终扫报错。
    };
    if end + 1 != inner_start.len() {
        return Ok(None); // `}` 后还有内容 → 内嵌形态。
    }
    let spec = PlaceholderSpec::parse(raw, &inner_start[..end]);
    let resolved = match resolve_placeholder_value(&spec, current_path, map) {
        Ok(v) => v,
        Err(e) if preserve_unresolved => {
            tracing::debug!(placeholder = spec.raw, path = current_path, error = %e, "preserve unresolved yml placeholder");
            return Ok(Some(serde_json::Value::String(spec.raw.to_string())));
        }
        Err(e) => return Err(e),
    };
    // 树命中类型保真、env/default 标量类型化(见 ResolvedValue::into_value)。
    // 若值本身仍含 `${`(链式引用中间态),它必然是字符串,留待下一趟继续解析。
    Ok(Some(resolved.into_value()))
}

/// 字符串内嵌占位符替换(`url: http://x:${server.port}/api` 形态)。
/// 结果永远是字符串拼接,即使替换值看起来像数字也不类型化。
/// 返回 (新串, 是否替换过)。`${` `}` 都是 ASCII,按字节切片安全。
///
/// # 参数
/// - `s`: 要解析的输入字符串。
/// - `current_path`: 当前配置值的点分路径,用于错误定位和自引用判定。
/// - `map`: 当前函数读取或更新的键值映射。
/// - `preserve_unresolved`: 是否保留未解析占位符供后续 overlay 继续处理。
fn substitute_string(
    s: &str,
    current_path: &str,
    map: &HashMap<String, serde_json::Value>,
    preserve_unresolved: bool,
) -> anyhow::Result<(String, bool)> {
    let mut result = String::with_capacity(s.len());
    let mut rest = s;
    let mut changed = false;
    while let Some(start) = rest.find("${") {
        result.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        match after.find('}') {
            Some(end) => {
                let inner = &after[..end];
                let raw_len = start + 2 + end + 1;
                let raw = &rest[start..raw_len];
                let spec = PlaceholderSpec::parse(raw, inner);
                // 未命中且无默认值时 resolve 直接 Err:键是否存在于树/环境变量是跨趟稳定的,
                // 本趟解不开的键后面趟也解不开,立即失败能给出最准的定位。
                match resolve_placeholder_value(&spec, current_path, map) {
                    Ok(val) => result.push_str(&val.into_string()),
                    Err(e) if preserve_unresolved => {
                        tracing::debug!(placeholder = spec.raw, path = current_path, error = %e, "preserve unresolved yml placeholder");
                        result.push_str(spec.raw);
                    }
                    Err(e) => return Err(e),
                }
                changed = true;
                rest = &after[end + 1..];
            }
            // 没有闭合 `}`:剩余原样输出;若因此残留 `${`,由终扫统一报错。
            None => {
                result.push_str("${");
                rest = after;
                break;
            }
        }
    }
    result.push_str(rest);
    Ok((result, changed))
}

/// 按固定优先级解析一个占位符,返回【带来源】的值
/// 配置树(跳过自身)→ 原样环境变量 → relaxed 环境变量 → 默认值 → Err。
///
/// # 参数
/// - `spec`: 占位符表达式内容,包含 key 和可选默认值。
/// - `current_path`: 当前配置值的点分路径,用于判断 `${key}` 是否引用自身。
/// - `map`: 当前函数读取或更新的键值映射。
fn resolve_placeholder_value(
    spec: &PlaceholderSpec<'_>,
    current_path: &str,
    map: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<ResolvedValue> {
    // 自引用跳过:`server.port: ${server.port:8080}` 这种「字段引用自己」是合法写法。
    // flatten 后 map 里 `server.port` 的值恰是这个占位符原文,
    // 若允许命中就会拿自己替换自己、永远走不到环境变量和默认值——所以 key 等于当前
    // 路径时必须跳过配置树,直接查 env / default。引用【其它】键则正常命中。
    if spec.key != current_path {
        if let Some(v) = map.get(spec.key) {
            return Ok(ResolvedValue::Tree(v.clone()));
        }
    }

    // 环境变量 fallback:先原样 key(兼容 `${LOCAL__NETWORK__IP}` 旧脚本形态),再 relaxed key。
    // relaxed = `.`/`-` 转 `_` 后大写(`${server.port}` → `SERVER_PORT`):这是「按名取值」的
    // 命名映射,与 `APP__...` 覆盖层的「按结构注入」规则刻意不同,不做双下划线。
    let relaxed = relaxed_env_key(spec.key);
    if let Ok(v) = std::env::var(spec.key) {
        return Ok(ResolvedValue::Env(v));
    }
    if let Ok(v) = std::env::var(&relaxed) {
        return Ok(ResolvedValue::Env(v));
    }

    // 默认值:`${key:default}` 的兜底,可为空串(`${server.name:}` → "")。
    if let Some(d) = spec.default {
        return Ok(ResolvedValue::Default(d.to_string()));
    }

    // 全部未命中:fail-fast。原样保留只会把「漏配」推迟成隐蔽的
    // 反序列化错误或运行期脏值;错误里给全尝试过的查找项,让用户一眼定位。
    anyhow::bail!(
        "yml: 未解析占位符 `{}` at `{}`; tried config key `{}`, env `{}`, env `{}`{}",
        spec.raw,
        current_path,
        spec.key,
        spec.key,
        relaxed,
        if spec.key == current_path {
            "(配置树命中因自引用被跳过)"
        } else {
            ""
        }
    )
}

/// env / default 字符串来源的标量类型化
/// 引号包裹 → 字符串(去引号,`"8080"` 保持字符串);未加引号的整数/浮点 → number;
/// `true`/`false` → bool;**其它一律原样字符串**——`/fore-rest` 这类含 `/`、`-` 的值
/// 属于「其它」,必须原样输出,`/` 不是路径语义、`-` 不是分隔语义。
/// (树命中不走此函数:原始 Value 类型直接保真,。)
///
/// # 参数
/// - `raw`: 待解析的原始字符串、字节或配置值。
fn parse_scalar(raw: &str) -> serde_json::Value {
    // 显式引号 = 用户强制字符串语义(即使内容像数字);同时也是保留首尾空格的手段。
    if raw.len() >= 2 {
        let b = raw.as_bytes();
        if (b[0] == b'"' && b[raw.len() - 1] == b'"')
            || (b[0] == b'\'' && b[raw.len() - 1] == b'\'')
        {
            return serde_json::Value::String(raw[1..raw.len() - 1].to_string());
        }
    }
    // 整数:要求 to_string 往返一致,避免 "007"/"+1" 这类被静默改写内容(改写即丢信息,当字符串)。
    if let Ok(i) = raw.parse::<i64>() {
        if i.to_string() == raw {
            return serde_json::Value::Number(i.into());
        }
    }
    // 浮点:只认 `123.45` 简单形态(不认 1e3/.5/NaN——那些在配置里更可能是字符串本意)。
    if is_simple_float(raw) {
        if let Some(n) = raw
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
        {
            return serde_json::Value::Number(n);
        }
    }
    match raw {
        "true" => serde_json::Value::Bool(true),
        "false" => serde_json::Value::Bool(false),
        _ => serde_json::Value::String(raw.to_string()),
    }
}

/// `-?digits.digits` 简单浮点判定(恰好一个 `.`,两侧都有数字)。
///
/// # 参数
/// - `s`: 要解析的输入字符串。
fn is_simple_float(s: &str) -> bool {
    let s = s.strip_prefix('-').unwrap_or(s);
    let mut parts = s.splitn(2, '.');
    let (int, frac) = (parts.next().unwrap_or(""), parts.next());
    match frac {
        Some(f) => {
            !int.is_empty()
                && !f.is_empty()
                && int.bytes().all(|b| b.is_ascii_digit())
                && f.bytes().all(|b| b.is_ascii_digit())
        }
        None => false,
    }
}
