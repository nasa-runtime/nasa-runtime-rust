//! 只读迁移检查：保留单源配置默认值，生成命名源候选与必须由业务裁决的差异。

use crate::{KafkaConfig, NafkaError, Result};
use serde::Serialize;
use serde_json::{json, Value};

/// 已经解析的 topic、event 与消费组归属；广播需填写实际沿用的 group。
#[derive(Clone, Debug, Serialize)]
pub struct MigrationRoute {
    /// 原业务 topic。
    pub topic: String,
    /// 原业务事件名，不能用缺省值猜测。
    pub event: String,
    /// 原实际消费组，决定提交位置归属。
    pub group: String,
}

/// 默认值变化与本次候选采用值分别呈现，避免把显式配置误认成缺省行为。
#[derive(Clone, Debug, Serialize)]
pub struct MigrationDefaultChange {
    /// 配置的完整字段路径。
    pub field: String,
    /// 单源配置格式的缺省值。
    pub previous_default: Value,
    /// 当前配置的缺省值。
    pub current_default: Value,
    /// 保留原配置后候选采用的值。
    pub candidate_value: Value,
    /// 输入是否明确声明该字段。
    pub explicitly_configured: bool,
}

/// 需要业务核对的稳定决策项，不包含原始凭据或底层配置错误正文。
#[derive(Clone, Debug, Serialize)]
pub struct MigrationFinding {
    /// 稳定类别，供工具分类展示。
    pub code: String,
    /// 为真时不能把候选当成可直接使用的配置。
    pub blocking: bool,
    /// 当前业务合同及需要采取的明确动作。
    pub detail: String,
}

/// 独立的脱敏配置建议；本地校验通过不证明 broker 权限、业务等价或滚动兼容。
#[derive(Clone, Debug, Serialize)]
pub struct MigrationReport {
    /// 显式目标命名源。
    pub source_id: String,
    /// 可供人工评审的 kafkas 配置片段，敏感值以占位符代替。
    pub candidate: Value,
    /// 调用方提供的完整既有路由目录。
    pub routes: Vec<MigrationRoute>,
    /// 配置格式之间的默认差异。
    pub default_changes: Vec<MigrationDefaultChange>,
    /// 不能由检查器擅自决定的配置、路由和业务语义。
    pub findings: Vec<MigrationFinding>,
    /// 原始候选是否通过当前本地解析与配置约束；脱敏占位符不用于该校验。
    pub locally_valid: bool,
}

/// 业务作用：将单源配置转换为明确的命名源建议，冻结原有默认并列出业务决策项。
/// 参数说明：source_id 为目标规范名称，legacy 为单个 KafkaConfig 的结构化配置，routes 为已解析的既有路由。
/// 返回：脱敏候选、默认差异与本地校验；未知字段及无效路由拒绝。函数不构造运行时、连接 broker 或更改 group 位点。
pub fn inspect_legacy_config(
    source_id: &str,
    legacy: Value,
    routes: Vec<MigrationRoute>,
) -> Result<MigrationReport> {
    if source_id.is_empty()
        || source_id.len() > 64
        || !source_id.as_bytes()[0].is_ascii_lowercase()
        || !source_id
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-' || c == b'_')
        || !legacy.is_object()
        || routes.iter().any(|route| {
            crate::wire::is_blank_event(&route.event)
                || route.event.contains('\0')
                || route.topic.trim().is_empty()
                || route.topic.contains('\0')
                || crate::wire::validate_group(&route.group).is_err()
        })
    {
        return Err(NafkaError::Config("迁移来源、配置或路由目录无效".into()));
    }
    let mut defaults: Value = serde_json::from_str(include_str!("migration/source-defaults.json"))
        .map_err(|_| NafkaError::Config("Kafka 迁移配置格式不可用".into()))?;
    defaults["client_name"] = json!("default");
    defaults["bootstrap_servers"] = json!("");
    defaults["group_id"] = Value::Null;
    defaults["client_id_prefix"] = Value::Null;
    defaults["properties"] = json!({});
    let mut source = defaults.clone();
    merge(&mut source, &legacy)?;
    if legacy
        .get("client_name")
        .is_some_and(|value| !value.is_string())
    {
        return Err(NafkaError::Config("迁移输入的来源名称必须是字符串".into()));
    }
    let mut findings = vec![
        finding("STRICT_HEADERS", false, "候选显式选择保留头取末值；仍拒绝非法 UTF-8、null 或空事件。全部生产者规范化后再开启 strict_headers。"),
        finding("BOUNDED_EXECUTION", false, "输入、worker、DLT 和发布责任采用当前有界准入与安全水位；容量及失败时序需按业务负载确认。"),
        finding("MANAGED_ADMIN_READ_ONLY", false, "KafkaHandle.admin 始终只读；管理写入须按 client 显式配置 container.admin_write 并调用 administration。"),
        finding("MANAGED_LOOKUP", false, "kafkas 命名装配通过 app.kafka(name) 查找；不把单源默认入口或属性 consumer 的 client 声明静默重定向。"),
        finding("ROUTE_MEMBERSHIP", false, "保留完整 group 身份；广播切换算法不自动继承位点。所有组成员先具备新增 event 路由，再允许发布该事件。"),
        finding("TOMBSTONE_POLICY", false, "当前 typed tombstone 默认 Skip；需要转入死信或交给 nullable 业务时须明确选择策略，普通 JSON null 与 null payload 分别裁决。"),
    ];
    if source["client_name"].as_str() != Some(source_id) {
        findings.push(finding(
            "SOURCE_ID_CHANGED",
            false,
            "目标源名须同步更新 consumer 的 client 绑定和业务查找；group 不自动改名。",
        ));
    }
    source["client_name"] = json!(source_id);
    // 只冻结可表达的读取兼容项，不能重开已撤销的确认权或恢复不安全的提交顺序。
    source["behavior"]["strict_headers"] = json!(false);
    let current = crate::ProducerConfig::default();
    let behavior = crate::BehaviorConfig::default();
    let changed = [
        ("producer", "acks", json!(current.acks)),
        (
            "producer",
            "enable_idempotence",
            json!(current.enable_idempotence),
        ),
        (
            "producer",
            "queue_buffering_max_messages",
            json!(current.queue_buffering_max_messages),
        ),
        (
            "producer",
            "queue_buffering_max_kbytes",
            json!(current.queue_buffering_max_kbytes),
        ),
        (
            "behavior",
            "dlt_queue_max_bytes",
            json!(behavior.dlt_queue_max_bytes),
        ),
    ];
    let default_changes = changed
        .into_iter()
        .map(|(section, field, now)| MigrationDefaultChange {
            field: format!("{section}.{field}"),
            previous_default: defaults[section][field].clone(),
            current_default: now,
            candidate_value: if source[section][field].is_object()
                || source[section][field].is_array()
            {
                json!("<invalid>")
            } else {
                source[section][field].clone()
            },
            explicitly_configured: legacy.get(section).and_then(|v| v.get(field)).is_some(),
        })
        .collect();
    // 校验仅使用配置值；不通过 KafkaProxy::connect 探测，避免检查工具产生真实资源或线程。
    let locally_valid = serde_json::from_value::<KafkaConfig>(source.clone())
        .ok()
        .is_some_and(|config| config.validate().is_ok());
    if !locally_valid {
        findings.push(finding(
            "CONFIG_INVALID",
            true,
            "候选不满足当前本地配置约束；单独校验原配置后再部署，报告不回显失败输入。",
        ));
    }
    if redact(&mut source, false) {
        findings.push(finding(
            "SECRETS_REDACTED",
            true,
            "候选中的 <redacted> 必须由部署环境重新绑定；包括凭据、证书路径和原生 properties 值。",
        ));
    }
    Ok(MigrationReport {
        source_id: source_id.into(),
        candidate: json!({"kafkas": {source_id: source}}),
        routes,
        default_changes,
        findings,
        locally_valid,
    })
}

/// 业务作用：合并单源声明并拒绝固定配置结构中的未知键，原生扩展和 lane 表由实际配置解析器校验。
/// 参数说明：target 为独立默认树，supplied 为用户输入。
/// 返回：只更新候选；未知结构返回 Config，不改变输入或创建运行资源。
fn merge(target: &mut Value, supplied: &Value) -> Result<()> {
    if target.is_object() && !supplied.is_object() {
        return Err(NafkaError::Config("迁移输入的配置分组必须是对象".into()));
    }
    if let (Some(base), Some(values)) = (target.as_object_mut(), supplied.as_object()) {
        if base.is_empty() {
            base.extend(values.clone());
            return Ok(());
        }
        for (key, value) in values {
            let existing = base
                .get_mut(key)
                .ok_or_else(|| NafkaError::Config("迁移输入含未知配置字段".into()))?;
            merge(existing, value)?;
        }
    } else {
        *target = supplied.clone();
    }
    Ok(())
}

/// 业务作用：构造只包含静态业务语义的决策项，避免嵌入底层错误或凭据。
/// 参数说明：code 为稳定类别，blocking 标明部署前置项，detail 为固定说明。
/// 返回：独立且可序列化的报告条目。
fn finding(code: &str, blocking: bool, detail: &str) -> MigrationFinding {
    MigrationFinding {
        code: code.into(),
        blocking,
        detail: detail.into(),
    }
}

/// 业务作用：在报告导出前遮蔽凭据与任意原生扩展值，阻止检查输出成为配置泄漏通道。
/// 参数说明：value 为独立候选树，opaque 表示整个原生 properties 域均不可信。
/// 返回：是否实际遮蔽值；仅修改候选，不读取证书或连接外部服务。
fn redact(value: &mut Value, opaque: bool) -> bool {
    let mut changed = false;
    if let Some(map) = value.as_object_mut() {
        for (key, item) in map {
            let name = key.to_ascii_lowercase();
            let sensitive = opaque
                || name.contains("password")
                || name.contains("secret")
                || name.contains("token")
                || name.contains("jaas")
                || name.contains("username")
                || name.ends_with("location");
            if sensitive && !item.is_null() {
                *item = json!("<redacted>");
                changed = true;
            } else {
                changed |= redact(item, name == "properties");
            }
        }
    }
    changed
}
