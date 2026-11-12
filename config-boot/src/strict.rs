//! 严格来源计划复用 naml 的文件、模式、预算与绑定规则，配置中心只提供完整文本。

use crate::NacosBootstrap;
use naml::{
    strict::{ConfigLoader, ConfigPath, LoadedConfig, PreparedLoad, SourceDocument},
    ConfigFormat, YmlImport,
};
use nanacos::{ConfigBundle, ConfigRef, NacosConfigClient};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

/// 业务作用：只求值配置中心引导依赖，并将 nacos.imports 放在 yml.imports 之前。
///
/// 参数说明：
/// - `loader`：已固定环境、目录与读取权限的严格加载器，用于准备本地来源及求值引导依赖。
///
/// 返回：经过严格声明校验的计划和连接设置；不读取远端业务文档。
pub fn prepare(loader: &ConfigLoader) -> anyhow::Result<(PreparedLoad, NacosBootstrap)> {
    let mut plan = loader.prepare()?;
    let path = ConfigPath::parse("nacos.enabled")?;
    let enabled = if plan.contains(&path) {
        naml::strict::bind::<bool>(plan.bootstrap(std::slice::from_ref(&path))?[&path].clone())?
    } else {
        false
    };
    let section = ConfigPath::parse("nacos")?;
    let value = if enabled && plan.contains(&section) {
        plan.bootstrap(std::slice::from_ref(&section))?[&section].clone()
    } else {
        // 关闭远端能力时保护整段配置，只保留开关提示，避免无关连接表达式仍被求值。
        plan = plan
            .protect(section)
            .with_hint(path, naml::strict::ValueHint::Scalar);
        json!({"enabled":false})
    };
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("config-boot: invalid nacos declaration"))?;
    anyhow::ensure!(
        object.keys().all(|key| matches!(
            key.as_str(),
            "enabled"
                | "server_addr"
                | "namespace"
                | "group"
                | "app_name"
                | "discovery_ip"
                | "username"
                | "password"
                | "file_extension"
                | "imports"
        )),
        "config-boot: unknown nacos field"
    );
    if let Some(imports) = object.get("imports") {
        let imports = imports
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("config-boot: invalid nacos imports"))?;
        anyhow::ensure!(
            imports.len() <= loader.load_policy().limits.declarations,
            "config-boot: import limit exceeded"
        );
        for item in imports {
            let map = item
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("config-boot: invalid nacos import"))?;
            anyhow::ensure!(
                map.keys().all(|key| matches!(
                    key.as_str(),
                    "data_id" | "group" | "optional" | "file_extension"
                )) && map.get("optional").is_some_and(Value::is_boolean),
                "config-boot: invalid nacos import fields"
            );
            for key in ["data_id", "group", "file_extension"] {
                if key == "data_id" || map.contains_key(key) {
                    anyhow::ensure!(
                        map.get(key)
                            .and_then(Value::as_str)
                            .is_some_and(|value| !value.trim().is_empty()),
                        "config-boot: empty or non-text nacos identity"
                    );
                }
            }
        }
    }
    let boot: NacosBootstrap = naml::strict::bind(value)?;
    plan = plan.prepend_imports(boot.nacos_imports())?;
    let refs = references(&plan, &boot)?;
    // 开关与远端来源必须一致，不能在关闭状态访问 Nacos，也不能把空的远端声明视为启用成功。
    anyhow::ensure!(
        enabled != refs.is_empty(),
        "config-boot: nacos enablement and imports disagree"
    );
    Ok((plan, boot))
}

/// 业务作用：在网络请求前归一化并检查重复的远端身份。
///
/// 参数说明：
/// - `plan`：已经固定导入顺序的来源计划，不在此处重新读取或重排声明。
/// - `boot`：引导配置，提供远端引用的默认分组和内容格式。
///
/// 返回：无重复的有序远端引用，不解释 data_id 中的文件通配符。
pub fn references(plan: &PreparedLoad, boot: &NacosBootstrap) -> anyhow::Result<Vec<ConfigRef>> {
    let refs = crate::nacos_refs(plan.imports(), &boot.group, boot.file_extension.as_deref())?;
    let mut identities = BTreeSet::new();
    for reference in &refs {
        // 同一身份重复出现会使包匹配和覆盖次序产生歧义，在网络访问前统一拒绝。
        anyhow::ensure!(
            !reference.data_id.trim().is_empty()
                && identities.insert((reference.group.clone(), reference.data_id.clone())),
            "config-boot: duplicate or empty nacos identity"
        );
    }
    Ok(refs)
}

/// 业务作用：首拉只取得远端文本，随后由 naml 按完整计划一次合并和求值。
///
/// 参数说明：
/// - `plan`：本轮已准备的本地来源与导入顺序，消费后生成完整候选。
/// - `client`：已经完成连接与鉴权的配置中心客户端，负责获取声明的远端文本。
/// - `boot`：本轮冻结的引导设置，提供远端引用的默认分组和格式。
///
/// 返回：来源齐全、格式与身份一致的完整候选；任何网络错误保持失败语义。
pub async fn load(
    plan: PreparedLoad,
    client: &NacosConfigClient,
    boot: &NacosBootstrap,
) -> anyhow::Result<LoadedConfig> {
    let bundle = client.fetch_many(&references(&plan, boot)?).await?;
    assemble(plan, &bundle, boot)
}

/// 业务作用：将订阅包与声明逐一匹配，拒绝多余、乱序或格式不一致的材料。
///
/// 参数说明：
/// - `plan`：本轮固定的本地来源和导入顺序，决定候选装配次序。
/// - `bundle`：按声明顺序取得的远端文档包，仅允许可选来源明确缺失。
/// - `boot`：提供默认分组和内容格式的引导设置，必须与准备计划时一致。
///
/// 返回：本地与远端共同装配的完整候选，可选缺失只允许明确没有文档。
pub fn assemble(
    plan: PreparedLoad,
    bundle: &ConfigBundle,
    boot: &NacosBootstrap,
) -> anyhow::Result<LoadedConfig> {
    let refs = references(&plan, boot)?;
    let mut refs = refs.iter();
    let mut docs = bundle.documents.iter().peekable();
    let mut remote = BTreeMap::new();
    for (index, import) in plan.imports().iter().enumerate() {
        if let YmlImport::Nacos(_) = import {
            let expected = refs
                .next()
                .ok_or_else(|| anyhow::anyhow!("config-boot: invalid source plan"))?;
            let Some(doc) = docs.peek().filter(|doc| {
                doc.source.data_id == expected.data_id && doc.source.group == expected.group
            }) else {
                // 只有声明允许缺失时才能继续，必需来源缺失或乱序不得生成不完整候选。
                anyhow::ensure!(
                    expected.optional,
                    "config-boot: required remote source missing"
                );
                continue;
            };
            let format =
                ConfigFormat::from_extension(expected.file_extension.as_deref().unwrap_or("yaml"))?;
            // 文档携带的格式不能覆盖本地声明，否则同一文本可能被按不同语义解析。
            anyhow::ensure!(
                doc.source
                    .file_extension
                    .as_deref()
                    .map(ConfigFormat::from_extension)
                    .transpose()?
                    .is_some_and(|actual| actual == format),
                "config-boot: remote format mismatch"
            );
            remote.insert(
                index,
                SourceDocument::new(
                    format!("nacos-source-{index}"),
                    format,
                    doc.content.as_bytes(),
                ),
            );
            docs.next();
        }
    }
    // 拒绝计划之外的剩余材料，防止无声明的远端文档进入候选配置。
    anyhow::ensure!(
        docs.next().is_none(),
        "config-boot: unexpected remote source"
    );
    Ok(plan.finish(&remote)?)
}
