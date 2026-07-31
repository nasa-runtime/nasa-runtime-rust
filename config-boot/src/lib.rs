//! 配置引导装配层。
//!
//! 负责把本地分层配置、远端配置引用和配置中心裸文本拼装成有序 overlay，
//! 供业务在启动期一次性加载完整配置。
// ============================================================================
// config-boot —— yml 配置加载 × nacos 配置中心的【引导胶水】。
//
// 经门面 `nasa::naml::nacos` 暴露。分工:
//   · yml   只认本地文件 + overlay + 占位符(零 Nacos 认知);
//   · nacos 只认 data_id/group/裸文本(零 yml 认知,file_extension 只随文档透传、不解释);
//   本 crate 在两者之上做:import→ConfigRef 映射、**file_extension 格式解析(优先级 + 缺省默认 yaml)**、
//   按 import 顺序拉取拼有序 overlay。
//
// 【格式契约】远端 Nacos 内容按本地 file_extension(snake_case,Rust 风格)决定格式:
//   优先级:import 条目 file_extension > 全局 default_file_extension(= NacosBootstrap.file_extension,即 nacos.file_extension)。
//   都没配 → 默认 yaml。本地 file import 的格式从文件后缀推断。
//
// 【分组 / 格式默认由 app 传入】default_group、default_file_extension 由 app 从引导配置提供
//   (app 本就有这些值),故本 crate 无需窥探 nacos client 内部,yml/nacos 边界不破。
//
// 启动:load_bootstrap_checked → resolve_imports → connect_config_client → resolve_ordered_overlays_for_bootstrap → load_with_overlays。
// 热刷新:watch_many_channel 推来 ConfigBundle(含 source.file_extension)→ assemble_overlays_from_bundle → load_with_overlays。
// ============================================================================
#![forbid(unsafe_code)]

use std::path::Path;

use anyhow::Context;
use naml::{ConfigFormat, NacosImport, YmlImport, YmlLoader, YmlOverlay};
use nanacos::{ConfigBundle, ConfigRef, NacosConfigClient, NacosProps};

/// 把 yml 中性的 [`NacosImport`] 映射成 nacos [`ConfigRef`],并**解析出具体 group 与 file_extension**。
///
/// - 分组:`import.group` 非空 → 用它;否则回退 `default_group`。
/// - 格式:`import.file_extension` 非空 → 用它;否则回退 `default_file_extension`(NacosBootstrap.file_extension);
///   **都没配 → 默认 `yaml`**;显式配了非受支持格式 → `Err`(见 [`ConfigFormat::from_extension`])。
///
/// # 参数
/// - `n`: 长度、数量或循环次数。
/// - `default_group`: 未显式指定 group 时使用的默认分组。
/// - `default_file_extension`: 远端配置未显式声明格式时使用的默认扩展名。
fn to_config_ref(
    n: &NacosImport,
    default_group: &str,
    default_file_extension: Option<&str>,
) -> anyhow::Result<ConfigRef> {
    let group = n
        .group
        .clone()
        .filter(|g| !g.trim().is_empty())
        .unwrap_or_else(|| default_group.to_string());

    let ext = n
        .file_extension
        .as_deref()
        .filter(|e| !e.trim().is_empty())
        .or_else(|| default_file_extension.filter(|e| !e.trim().is_empty()));
    let ext = match ext {
        // 显式配了就校验受支持格式(如 file_extension: xml 仍 fail-fast)。
        Some(e) => {
            ConfigFormat::from_extension(e)?;
            e.trim().to_string()
        }
        // 缺省默认 yaml(不配置 file_extension 时按 yaml 解析)。
        None => "yaml".to_string(),
    };

    Ok(ConfigRef {
        data_id: n.data_id.clone(),
        group: Some(group),
        optional: n.optional,
        file_extension: Some(ext),
    })
}

/// 抽出 imports 里全部 Nacos 引用(分组/格式已解析成具体值、保序)。供 `watch_many_channel` 注册。
/// 任一 Nacos 缺格式 / 格式非法 → `Err`(fail-fast)。
///
/// # 参数
/// - `imports`: 已解析出的本地 file 和 Nacos import 列表。
/// - `default_group`: import 未显式指定 group 时使用的默认 Nacos 分组。
/// - `default_file_extension`: import 未显式指定格式时使用的全局默认格式。
pub fn nacos_refs(
    imports: &[YmlImport],
    default_group: &str,
    default_file_extension: Option<&str>,
) -> anyhow::Result<Vec<ConfigRef>> {
    imports
        .iter()
        .filter_map(|i| match i {
            YmlImport::Nacos(n) => Some(to_config_ref(n, default_group, default_file_extension)),
            YmlImport::File { .. } => None,
        })
        .collect()
}

// 【已删除】旧 `resolve_imports_or_single_data_id`(单配置 data_id fallback)。
// 新入口只有 nacos.imports / yml.imports,旧 fallback 会绕过 reject_legacy_config_fields 守卫,故删除不保留、不经门面暴露。
// app 用 `resolve_imports`(强类型 nacos.imports + yml.imports,内含守卫)。

/// 按 import 声明顺序拉取/读取,拼成有序 overlay(启动期用):
///   file → 读本地(格式按后缀推断);nacos → `fetch_many` 批量拉;再按原 import 顺序交错还原。
///
/// # 参数
/// - `client`: 已连接的 Nacos 配置客户端,用于批量拉取远端配置。
/// - `imports`: 启动阶段最终生效的 import 列表。
/// - `default_group`: Nacos import 未指定 group 时使用的默认分组。
/// - `default_file_extension`: Nacos import 未指定格式时使用的全局默认格式。
pub async fn resolve_ordered_overlays(
    client: &NacosConfigClient,
    imports: &[YmlImport],
    default_group: &str,
    default_file_extension: Option<&str>,
) -> anyhow::Result<Vec<YmlOverlay>> {
    let refs = nacos_refs(imports, default_group, default_file_extension)?;
    let bundle = client.fetch_many(&refs).await?;
    assemble_overlays(imports, &bundle, default_group, default_file_extension).await
}

/// 从【已有的 `ConfigBundle`】(`watch_many_channel` 推来的)+ imports 重组有序 overlay(热刷新用:file 重读本地)。
///
/// # 参数
/// - `imports`: 热刷新时仍按该列表恢复 overlay 顺序。
/// - `bundle`: Nacos 监听端推来的配置包,只包含成功拉取的远端文档。
/// - `default_group`: Nacos import 未指定 group 时使用的默认分组。
/// - `default_file_extension`: Nacos import 未指定格式时使用的全局默认格式。
pub async fn assemble_overlays_from_bundle(
    imports: &[YmlImport],
    bundle: &ConfigBundle,
    default_group: &str,
    default_file_extension: Option<&str>,
) -> anyhow::Result<Vec<YmlOverlay>> {
    assemble_overlays(imports, bundle, default_group, default_file_extension).await
}

/// 核心拼装:按 import 顺序,file→读本地(格式按后缀)、nacos→从 bundle 取(格式来自 `source.file_extension`)。
///
/// bundle.documents 是「成功拉取项」按 refs 顺序排列的【子序列】(optional 缺失被跳过;必需缺失 `fetch_many` 已报错)。
/// 按 `(data_id, group)` 顺序消费即可保序还原。
///
/// # 参数
/// - `imports`: 按声明顺序解析出的远端或本地配置引用列表。
/// - `bundle`: 已经拉取的一组配置文档。
/// - `default_group`: 未显式指定 group 时使用的默认分组。
/// - `default_file_extension`: 远端配置未显式声明格式时使用的默认扩展名。
async fn assemble_overlays(
    imports: &[YmlImport],
    bundle: &ConfigBundle,
    default_group: &str,
    default_file_extension: Option<&str>,
) -> anyhow::Result<Vec<YmlOverlay>> {
    let mut docs = bundle.documents.iter();
    let mut next = docs.next();
    let mut overlays = Vec::with_capacity(imports.len());
    for imp in imports {
        match imp {
            // 本地 file: import 用 tokio::fs 异步读,避免阻塞 runtime worker(热刷新在 watch 任务里调)。
            YmlImport::File { path, optional } => match tokio::fs::read_to_string(path).await {
                Ok(content) => {
                    // 本地文件格式从后缀推断(本地 file 可推断)。
                    let format = file_format_from_path(path)?;
                    push_present_overlay(
                        &mut overlays,
                        path.display().to_string(),
                        content,
                        format,
                    );
                }
                Err(e) if *optional => {
                    tracing::warn!(path = %path.display(), error = %e, "config-boot: 可选本地 file import 读取失败,跳过")
                }
                Err(e) => {
                    return Err(anyhow::Error::from(e).context(format!(
                        "config-boot: 必需本地 file import {} 读取失败",
                        path.display()
                    )))
                }
            },
            YmlImport::Nacos(n) => {
                let expected = to_config_ref(n, default_group, default_file_extension)?;
                match next {
                    Some(doc)
                        if doc.source.data_id == expected.data_id
                            && doc.source.group == expected.group =>
                    {
                        // 文档格式来自 source.file_extension(nacos 随文档带回)。
                        let ext = doc.source.file_extension.as_deref().with_context(|| {
                            format!(
                                "config-boot: bundle 文档 {} 缺 file_extension",
                                doc.source.data_id
                            )
                        })?;
                        let format = ConfigFormat::from_extension(ext)?;
                        push_present_overlay(
                            &mut overlays,
                            format!("nacos:{}", doc.source.data_id),
                            doc.content.clone(),
                            format,
                        );
                        next = docs.next();
                    }
                    // bundle 无此份 → 可选且拉取失败被跳过的项(必需缺失已在 fetch_many 报错)。
                    _ => {}
                }
            }
        }
    }
    Ok(overlays)
}

/// 把「已 present(拉取/读取成功)」的内容压成 overlay。
///
/// - 内容为空 → warn + 跳过(空配置合并无意义,不算错)。
/// - 内容非空 → `required=true`(/:内容在手,格式错必须 fail-fast,不因源 optional 吞掉)。
///
/// # 参数
/// - `overlays`: 按优先级叠加的配置 overlay 列表。
/// - `name`: 业务名称、字段名或配置名,用于定位目标对象。
/// - `content`: 待处理的文本内容。
/// - `format`: 配置文本格式,决定使用 YAML、JSON 或 TOML parser。
fn push_present_overlay(
    overlays: &mut Vec<YmlOverlay>,
    name: String,
    content: String,
    format: ConfigFormat,
) {
    if content.trim().is_empty() {
        tracing::warn!(overlay = %name, "config-boot: 配置内容为空,跳过");
    } else {
        overlays.push(YmlOverlay::required(name, content, format));
    }
}

/// 本地文件按后缀推断格式(本地 file import 可从后缀推断);无扩展名 → `Err`。
///
/// # 参数
/// - `path`: 待加载的本地配置文件路径。
fn file_format_from_path(path: &Path) -> anyhow::Result<ConfigFormat> {
    match path.extension().and_then(|e| e.to_str()) {
        Some(ext) => ConfigFormat::from_extension(ext),
        None => anyhow::bail!(
            "config-boot: 本地文件 {} 无扩展名,无法推断格式(需 .yml/.yaml/.json/.toml)",
            path.display()
        ),
    }
}

// ════════════════════════════════════════════════════════════════════════════
// NacosBootstrap —— 全 app 共享的 Nacos 引导配置,取代各 app 手写的 NacosConfig。
//   连接参数 + 单/多配置 imports + 旧字段守卫 + 引导入口便利,全部收敛在这里。
// ════════════════════════════════════════════════════════════════════════════

/// nacos.group 缺省("DEFAULT_GROUP",与 NacosProps::default() 一致)。
fn default_group() -> String {
    "DEFAULT_GROUP".to_string()
}

/// import 条目 optional 缺省(true)。
fn default_true() -> bool {
    true
}

/// 全 app 共享的 Nacos 引导配置(serde 反序列化目标)。app 侧只声明 `pub nacos: nasa::naml::nanacos::NacosBootstrap`。
///
/// `Debug` 手写脱敏:`username`/`password` 只标记是否已设,不打印明文——app 侧常把此结构体嵌进自己的
/// 配置并在启动时 `tracing::info!(?cfg)`,derive(Debug) 会泄漏 Nacos 密码(公共库默认防御)。
#[derive(Clone, serde::Deserialize)]
pub struct NacosBootstrap {
    /// 总开关。false → 不连接、走本地 load()。
    #[serde(default)]
    pub enabled: bool,
    /// Nacos 地址(host:port,多个逗号分隔)。enabled=true 时非空(validate_connection_enabled 校验)。
    #[serde(default)]
    pub server_addr: String,
    /// 命名空间(public 填空串)。
    #[serde(default)]
    pub namespace: String,
    /// 默认分组(缺省 DEFAULT_GROUP)。
    #[serde(default = "default_group")]
    pub group: String,
    /// 应用名(注册/展示)。
    #[serde(default)]
    pub app_name: String,
    /// 注册中心对外 IP(仅服务发现 app 用;配置中心忽略)。
    #[serde(default)]
    pub discovery_ip: Option<String>,
    /// 鉴权用户名。
    #[serde(default)]
    pub username: String,
    /// 鉴权密码(建议走 APP__NACOS__PASSWORD)。
    #[serde(default)]
    pub password: String,
    /// imports 的全局默认内容格式。snake_case;缺省默认 yaml。
    #[serde(default)]
    pub file_extension: Option<String>,
    /// 纯 Nacos 多配置清单(替 yml.nacos.imports + 单配置 data_id);单配置 = 唯一一条 optional:false。
    #[serde(default)]
    pub imports: Vec<NacosImportSpec>,
}

impl std::fmt::Debug for NacosBootstrap {
    /// 输出启动配置的调试视图;username/password 只标记是否已配置,避免日志泄露凭据。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mark = |s: &str| if s.is_empty() { "<empty>" } else { "<set>" };
        f.debug_struct("NacosBootstrap")
            .field("enabled", &self.enabled)
            .field("server_addr", &self.server_addr)
            .field("namespace", &self.namespace)
            .field("group", &self.group)
            .field("app_name", &self.app_name)
            .field("discovery_ip", &self.discovery_ip)
            .field("username", &mark(&self.username))
            .field(
                "password",
                &if self.password.is_empty() {
                    "<empty>"
                } else {
                    "<redacted>"
                },
            )
            .field("file_extension", &self.file_extension)
            .field("imports", &self.imports)
            .finish()
    }
}

/// 单个 Nacos 导入项(强类型,替旧 yml.nacos.imports 的 map 条目)。
#[derive(Debug, Clone, serde::Deserialize)]
pub struct NacosImportSpec {
    /// 配置 dataId(如 tidb.yml)。
    pub data_id: String,
    /// 分组(缺 → 回退 NacosBootstrap.group)。
    #[serde(default)]
    pub group: Option<String>,
    /// 缺失是否可跳过(缺省 true;单配置迁移须显式 false)。
    #[serde(default = "default_true")]
    pub optional: bool,
    /// 本条内容格式(缺 → 回退全局 NacosBootstrap.file_extension;仍缺 → 默认 yaml)。snake_case。
    #[serde(default)]
    pub file_extension: Option<String>,
}

impl NacosBootstrap {
    /// enabled 路径连接前校验:enabled=false 或 server_addr 空都 Err;不检查 imports。
    ///
    pub fn validate_connection_enabled(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.enabled,
            "config-boot: nacos.enabled=false,不应连接配置中心(应走本地 load())"
        );
        anyhow::ensure!(
            !self.server_addr.trim().is_empty(),
            "config-boot: nacos.enabled=true 但 server_addr 为空"
        );
        Ok(())
    }

    /// 连接参数 → NacosProps(走 builder + with_discovery_ip_opt;不手写默认值)。
    pub fn to_props(&self) -> NacosProps {
        NacosProps::new(&self.server_addr)
            .with_namespace(&self.namespace)
            .with_group(&self.group)
            .with_app_name(&self.app_name)
            .with_auth(&self.username, &self.password)
            .with_discovery_ip_opt(self.discovery_ip.as_deref())
    }

    /// nacos.imports(强类型)→ 中性 YmlImport::Nacos 列表(保序)。
    pub fn nacos_imports(&self) -> Vec<YmlImport> {
        self.imports
            .iter()
            .map(|i| {
                YmlImport::Nacos(NacosImport {
                    data_id: i.data_id.clone(),
                    group: i.group.clone().filter(|g| !g.trim().is_empty()),
                    optional: i.optional,
                    file_extension: i.file_extension.clone().filter(|e| !e.trim().is_empty()),
                })
            })
            .collect()
    }
}

/// 旧字段迁移守卫:从【原始 boot_tree】精确检测旧 `yml.nacos.imports` / 顶层 `nacos.data_id`,命中即 `Err`。
///
/// ⚠️ 用精确路径匹配(`nacos.data_id` 是 nacos 段的直接子键、`yml.nacos.imports` 是 yml→nacos→imports),
/// 绝不能深搜 key 名——否则会把合法的 `nacos.imports[].data_id` 误判、拒掉所有新配置。
/// 必须查树而非 NacosBootstrap:结构体无 data_id 字段、serde 忽略未知字段,反序列化发现不了旧字段。
///
/// # 参数
/// - `boot_tree`: `YmlLoader::load_tree()` 生成的原始引导配置树。
pub fn reject_legacy_config_fields(boot_tree: &serde_json::Value) -> anyhow::Result<()> {
    if boot_tree
        .get("nacos")
        .and_then(|n| n.get("data_id"))
        .is_some()
    {
        anyhow::bail!(
            "config-boot: 检测到旧字段 nacos.data_id —— 单配置已并入 nacos.imports\
             (写成唯一一条 {{ data_id, optional: false }});请迁移"
        );
    }
    if boot_tree
        .get("yml")
        .and_then(|y| y.get("nacos"))
        .and_then(|n| n.get("imports"))
        .is_some()
    {
        anyhow::bail!(
            "config-boot: 检测到旧字段 yml.nacos.imports —— 已改为顶层 nacos.imports;\
             file/nacos 交错请用 yml.imports"
        );
    }
    Ok(())
}

/// app 引导入口便利:`load_tree` → `reject_legacy_config_fields` → 反序列化 `T`(复用同一棵树)。
///
/// 把旧字段守卫【焊死】进引导流程:app 的 `load_bootstrap()` 只需转调它,免重复 glue、守卫漏不掉。
/// env 已在 `load_tree` 叠加,故 `APP__NACOS__DATA_ID` 之类的 env 注入旧字段也会被拦。
///
/// # 参数
/// - `loader`: 缓存未命中时执行的回源加载闭包。
pub fn load_bootstrap_checked<T: serde::de::DeserializeOwned>(
    loader: &YmlLoader,
) -> anyhow::Result<T> {
    let tree = loader.load_tree()?;
    reject_legacy_config_fields(&tree)?;
    serde_json::from_value(tree).context("config-boot: 反序列化引导配置失败")
}

/// 解析最终 import 列表:`nacos.imports`(强类型,先)++ `yml.imports`(树,后)。
///
/// 内部先调 `reject_legacy_config_fields` 拦旧字段;`boot_tree` 须来自 `YmlLoader::load_tree()`(占位符已解析)。
///
/// # 参数
/// - `boot_tree`: 引导配置树,用于读取 `yml.imports` 并做旧字段守卫。
/// - `base_dir`: 本地 file import 的相对路径基准目录。
/// - `nacos`: 强类型 Nacos 引导配置,提供 `nacos.imports` 和默认分组/格式。
pub fn resolve_imports(
    boot_tree: &serde_json::Value,
    base_dir: &Path,
    nacos: &NacosBootstrap,
) -> anyhow::Result<Vec<YmlImport>> {
    reject_legacy_config_fields(boot_tree)?;
    let mut imports = nacos.nacos_imports();
    imports.extend(naml::parse_imports_from_tree(boot_tree, base_dir));
    Ok(imports)
}

/// enabled 路径校验最终 import 列表:至少一个 Nacos 项、每个 Nacos data_id 非空。
///
///
/// # 参数
/// - `nacos`: Nacos 连接或配置项。
/// - `imports`: 按声明顺序解析出的远端或本地配置引用列表。
pub fn validate_imports_enabled(
    _nacos: &NacosBootstrap,
    imports: &[YmlImport],
) -> anyhow::Result<()> {
    let nacos_count = imports
        .iter()
        .filter(|i| matches!(i, YmlImport::Nacos(_)))
        .count();
    anyhow::ensure!(
        nacos_count > 0,
        "config-boot: nacos.enabled=true 但最终 imports 无任何 Nacos 项(检查 nacos.imports / yml.imports)"
    );
    for i in imports {
        if let YmlImport::Nacos(n) = i {
            anyhow::ensure!(
                !n.data_id.trim().is_empty(),
                "config-boot: Nacos import 的 data_id 不能为空"
            );
        }
    }
    Ok(())
}

/// 连接配置中心:先 `validate_connection_enabled`,再 `to_props` 连接。
///
/// # 参数
/// - `nacos`: Nacos 引导配置,提供连接地址、命名空间、分组和鉴权信息。
pub async fn connect_config_client(nacos: &NacosBootstrap) -> anyhow::Result<NacosConfigClient> {
    nacos.validate_connection_enabled()?;
    NacosConfigClient::connect(&nacos.to_props()).await
}

/// 便利包装:group/file_extension 从 bootstrap 带入,调低层 [`resolve_ordered_overlays`]。
///
/// # 参数
/// - `client`: 已连接的 Nacos 配置客户端。
/// - `imports`: 启动阶段最终生效的 import 列表。
/// - `nacos`: Nacos 引导配置,提供默认 group 和 file_extension。
pub async fn resolve_ordered_overlays_for_bootstrap(
    client: &NacosConfigClient,
    imports: &[YmlImport],
    nacos: &NacosBootstrap,
) -> anyhow::Result<Vec<YmlOverlay>> {
    resolve_ordered_overlays(
        client,
        imports,
        &nacos.group,
        nacos.file_extension.as_deref(),
    )
    .await
}

/// 便利包装:热刷新时从已有 bundle 重组 overlay,group/file_extension 从 bootstrap 带入。
///
/// # 参数
/// - `imports`: 热刷新时仍按该列表恢复 overlay 顺序。
/// - `bundle`: Nacos 监听端推来的配置包。
/// - `nacos`: Nacos 引导配置,提供默认 group 和 file_extension。
pub async fn assemble_overlays_from_bundle_for_bootstrap(
    imports: &[YmlImport],
    bundle: &ConfigBundle,
    nacos: &NacosBootstrap,
) -> anyhow::Result<Vec<YmlOverlay>> {
    assemble_overlays_from_bundle(
        imports,
        bundle,
        &nacos.group,
        nacos.file_extension.as_deref(),
    )
    .await
}

/// 便利包装:最终 imports 的 Nacos 项 → `ConfigRef`(带 group/file_extension),供 `watch_many_channel`;格式缺失默认 yaml、显式非法格式仍 fail-fast。
///
/// # 参数
/// - `imports`: 已合并完成的最终 import 列表。
/// - `nacos`: Nacos 引导配置,提供默认 group 和 file_extension。
pub fn nacos_refs_for_bootstrap(
    imports: &[YmlImport],
    nacos: &NacosBootstrap,
) -> anyhow::Result<Vec<ConfigRef>> {
    nacos_refs(imports, &nacos.group, nacos.file_extension.as_deref())
}
