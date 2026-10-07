//! 兼容入口使用有界表达式解析；整值环境回退和默认值保留既有标量推断规则。
//! 新应用需要原始文本与目标类型绑定时使用 `strict` 模块。

/// 业务作用：事务式解析完整树，保持配置值、原样环境、规范化环境及默认值的命中顺序。
/// 参数说明：`tree` 为候选配置；成功后整体替换，失败保留原树。
/// 返回：嵌套默认值与标量引用全部完成；未命中、循环或超限返回安全错误。
pub fn resolve_placeholders(tree: &mut serde_json::Value) -> anyhow::Result<()> {
    resolve_with_environment(
        tree,
        false,
        &crate::strict::EnvironmentSnapshot::capture_compatible(),
    )
}

/// 业务作用：解析加载期表达式并保留合法但未命中的引用，供调用方后续解释。
/// 参数说明：`tree` 为候选树。
/// 返回：完整成功才替换输入；非法语法与循环仍被拒绝。
pub fn resolve_placeholders_preserving_unresolved(
    tree: &mut serde_json::Value,
) -> anyhow::Result<()> {
    resolve_with_environment(
        tree,
        true,
        &crate::strict::EnvironmentSnapshot::capture_compatible(),
    )
}

/// 业务作用：让兼容加载器的覆盖层和表达式共享一次环境观察。
/// 参数说明：`tree` 为候选；`preserve` 控制合法未命中保留；`environment` 是固定快照。
/// 返回：完整候选或安全错误，不读取真实环境。
pub(crate) fn resolve_with_environment(
    tree: &mut serde_json::Value,
    preserve: bool,
    environment: &crate::strict::EnvironmentSnapshot,
) -> anyhow::Result<()> {
    let policy = crate::strict::LoadPolicy {
        preserve_unresolved: preserve,
        ..Default::default()
    };
    *tree = crate::strict::resolve_compatible(tree, environment, &policy)?.tree;
    Ok(())
}

/// 业务作用：env / default 字符串来源的标量类型化
/// 引号包裹 → 字符串(去引号,`"8080"` 保持字符串);未加引号的整数/浮点 → number;
/// `true`/`false` → bool;**其它一律原样字符串**——`/fore-rest` 这类含 `/`、`-` 的值
/// 属于「其它」,必须原样输出,`/` 不是路径语义、`-` 不是分隔语义。
/// (树命中不走此函数:原始 Value 类型直接保真,。)
///
/// # 参数
/// - `raw`: 待解析的原始字符串、字节或配置值。
pub(crate) fn parse_scalar(raw: &str) -> serde_json::Value {
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

/// 业务作用：`-?digits.digits` 简单浮点判定(恰好一个 `.`,两侧都有数字)。
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
