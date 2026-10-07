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
/// 参数说明：`loader` 已固定环境、目录与读取权限。
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
    anyhow::ensure!(
        enabled != refs.is_empty(),
        "config-boot: nacos enablement and imports disagree"
    );
    Ok((plan, boot))
}

/// 业务作用：在网络请求前归一化并检查重复的远端身份。
/// 参数说明：`plan` 为固定顺序；`boot` 提供缺省分组和格式。
/// 返回：无重复的有序远端引用，不解释 data_id 中的文件通配符。
pub fn references(plan: &PreparedLoad, boot: &NacosBootstrap) -> anyhow::Result<Vec<ConfigRef>> {
    let refs = crate::nacos_refs(plan.imports(), &boot.group, boot.file_extension.as_deref())?;
    let mut identities = BTreeSet::new();
    for reference in &refs {
        anyhow::ensure!(
            !reference.data_id.trim().is_empty()
                && identities.insert((reference.group.clone(), reference.data_id.clone())),
            "config-boot: duplicate or empty nacos identity"
        );
    }
    Ok(refs)
}

/// 业务作用：首拉只取得远端文本，随后由 naml 按完整计划一次合并和求值。
/// 参数说明：`plan` 是同一轮已经读取的引导；`client` 为已鉴权客户端；`boot` 为冻结连接设置。
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
/// 参数说明：`plan` 是本轮固定引导；`bundle` 仅携带已取得的文档；`boot` 为默认分组和格式。
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
                anyhow::ensure!(
                    expected.optional,
                    "config-boot: required remote source missing"
                );
                continue;
            };
            let format =
                ConfigFormat::from_extension(expected.file_extension.as_deref().unwrap_or("yaml"))?;
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
    anyhow::ensure!(
        docs.next().is_none(),
        "config-boot: unexpected remote source"
    );
    Ok(plan.finish(&remote)?)
}
