//! 本地与 Nacos 的有序配置装配和分阶段引导。
//!
//! [`strict::prepare`] 先求值连接所需的引导依赖，固定来源顺序；[`strict::load`] 首拉远端文本，
//! [`strict::assemble`] 接收已有完整包。最终复用 naml 的自然排序文件导入、嵌套表达式、
//! 来源校验与目标类型绑定，返回完整候选。兼容入口继续返回有序 overlay。
//!
//! `nacos.imports` 位于 `yml.imports` 之前，远端身份、格式和包顺序必须与声明一致。
//! 严格入口的可选缺失不能掩盖网络、鉴权或内容错误；Nacos data ID 不做文件通配符展开。
//! 本组件不发布应用视图，不持有 secret 或组件关闭权；准备、观察和发布由宿主负责。
// ============================================================================
// config-boot —— yml 配置加载 × nacos 配置中心的【引导胶水】。
//
// 经门面 `nasa::yml::nacos` 暴露。分工:
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

/// 严格来源计划的准备、首拉与完整包装配，拒绝身份、顺序和格式不一致的远端材料。
pub mod strict;

use std::path::Path;

use anyhow::Context;
use naml::{ConfigFormat, NacosImport, YmlImport, YmlLoader, YmlOverlay};
use nanacos::{ConfigBundle, ConfigRef, NacosConfigClient, NacosProps};

/// 业务作用：将中性的 Nacos 导入声明转换为远端引用，确定分组并验证内容格式。
///
/// 参数说明：
/// - `n`：单条 Nacos 导入声明，包含文档身份、可选性及条目级覆盖设置。
/// - `default_group`：条目未配置非空分组时使用的默认分组。
/// - `default_file_extension`：条目未配置非空格式时使用的默认格式；仍缺失则使用 yaml。
///
/// 返回：保留 data_id 与可选性、填入分组和格式的 ConfigRef；显式格式不受支持时返回错误，不执行网络读取。
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

/// 业务作用：按导入顺序提取 Nacos 引用，为配置首拉和订阅提供一致的分组与内容格式。
///
/// 参数说明：
/// - `imports`：已解析的本地与远端导入列表；本地文件项不进入返回列表。
/// - `default_group`：远端条目没有非空分组时使用的默认分组。
/// - `default_file_extension`：条目没有非空格式时使用的默认值；均缺失时使用 yaml。
///
/// 返回：保持相对顺序的远端引用列表；非法格式返回错误，此入口不检查重复身份或执行文档读取。
pub fn nacos_refs(
    imports: &[YmlImport],
    default_group: &str,
    default_file_extension: Option<&str>,
) -> anyhow::Result<Vec<ConfigRef>> {
    imports
        .iter()
        .filter_map(|i| match i {
            YmlImport::Nacos(n) => Some(to_config_ref(n, default_group, default_file_extension)),
            YmlImport::File {
                ..
            } => None,
        })
        .collect()
}

/// 业务作用：为 overlay 兼容入口首拉远端文档，再按声明顺序交错装配本地文件与远端配置。
///
/// 参数说明：
/// - `client`：已连接的 Nacos 客户端，负责获取全部远端引用。
/// - `imports`：决定 overlay 优先级的最终导入列表。
/// - `default_group`：远端条目未指定非空分组时使用的默认分组。
/// - `default_file_extension`：远端条目未指定格式时使用的默认格式；仍缺失时使用 yaml。
///
/// 返回：按序装配的 overlay；引用格式、远端获取或必需本地读取失败时返回错误，可选本地读取错误会跳过。
/// 此兼容入口不执行严格来源预算和包一致性校验；需要完整来源门禁时使用 strict::load。
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

/// 业务作用：为 overlay 兼容入口使用现有远端包重组配置，同时重新读取声明中的本地文件。
///
/// 参数说明：
/// - `imports`：决定装配顺序的本地与远端导入列表。
/// - `bundle`：调用方已取得的远端文档包，须自行保证必需来源完整且按引用顺序排列。
/// - `default_group`：远端条目未指定非空分组时使用的默认分组。
/// - `default_file_extension`：远端条目的默认格式，参与引用构造；实际解析格式取自包内文档。
///
/// 返回：兼容装配得到的 overlay 或读取、格式错误；不会验证包内必需文档完整性或拒绝多余文档。
/// 需要严格匹配来源、顺序和声明格式时使用 strict::assemble。
pub async fn assemble_overlays_from_bundle(
    imports: &[YmlImport],
    bundle: &ConfigBundle,
    default_group: &str,
    default_file_extension: Option<&str>,
) -> anyhow::Result<Vec<YmlOverlay>> {
    assemble_overlays(imports, bundle, default_group, default_file_extension).await
}

/// 业务作用：按兼容 overlay 规则交错装配本地文件与远端包，保留导入声明的优先级。
///
/// 参数说明：
/// - `imports`：按覆盖顺序排列的本地与远端引用。
/// - `bundle`：上游已校验的远端文档子序列，按 data_id 与分组顺序匹配。
/// - `default_group`：远端引用未声明分组时使用的默认分组。
/// - `default_file_extension`：构造预期引用时使用的默认格式；匹配文档后使用文档自身格式。
///
/// 返回：非空文档组成的必需 overlay；必需文件读取失败或已匹配文档格式非法时返回错误。
/// 可选文件读取失败、未匹配引用及剩余包文档会被跳过；此入口不提供严格来源完整性保证。
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
            YmlImport::File {
                path,
                optional,
            } => match tokio::fs::read_to_string(path).await {
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
                    // 兼容装配依赖上游证明完整性；此处只消费顺序匹配项，不自行判断缺失是否合法。
                    _ => {}
                }
            }
        }
    }
    Ok(overlays)
}

/// 业务作用：将已经取得的非空文档标记为必需 overlay，防止后续解析错误被来源可选性掩盖。
///
/// 参数说明：
/// - `overlays`：按覆盖顺序累积的配置列表，成功时在末尾追加。
/// - `name`：文档来源标识，用于 overlay 诊断与空内容告警。
/// - `content`：已经读取的文档文本，保留原内容供加载器解析。
/// - `format`：已确认的配置文本格式，决定后续解析器。
///
/// 返回：非空文本追加为 required overlay；全空白文本只告警并跳过，此处不解析文本。
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

/// 业务作用：本地文件按后缀推断格式(本地 file import 可从后缀推断);无扩展名 → `Err`。
///
/// 参数说明：
/// - `path`：本地导入文件路径，仅使用扩展名判断内容格式。
///
/// 返回：受支持的配置格式；扩展名缺失、不是 UTF-8 或不受支持时返回错误，不读取文件内容。
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
// NacosBootstrap 汇集连接参数与远端导入声明，由宿主选择严格来源计划或兼容 overlay 入口。
// ════════════════════════════════════════════════════════════════════════════

/// 业务作用：nacos.group 缺省("DEFAULT_GROUP",与 NacosProps::default() 一致)。
///
/// 参数说明：无。
///
/// 返回：Nacos 默认分组字符串，供反序列化未声明 group 的引导配置使用。
fn default_group() -> String {
    "DEFAULT_GROUP".to_string()
}

/// 业务作用：import 条目 optional 缺省(true)。
///
/// 参数说明：无。
///
/// 返回：true；未声明 optional 的导入允许文档缺失，必需文档须显式配置 false。
fn default_true() -> bool {
    true
}

/// Nacos 引导配置的反序列化目标，可通过 nasa::yml::nacos::NacosBootstrap 在业务配置中声明。
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
    /// 远端配置清单；只有一个必需文档时声明一条 optional 为 false 的导入。
    #[serde(default)]
    pub imports: Vec<NacosImportSpec>,
}

impl std::fmt::Debug for NacosBootstrap {
    /// 业务作用：输出启动配置的调试视图;username/password 只标记是否已配置,避免日志泄露凭据。
    ///
    /// 参数说明：
    /// - `f`：调试视图输出目标；用户名和密码只展示配置状态，其余引导字段按当前值展示。
    ///
    /// 返回：视图写入结果；不输出 username/password 明文，不对地址或其他字段进行额外脱敏。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mark = |s: &str| {
            if s.is_empty() {
                "<empty>"
            } else {
                "<set>"
            }
        };
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

/// 单个远端导入声明，包含文档身份、缺失策略及分组和格式覆盖值。
#[derive(Debug, Clone, serde::Deserialize)]
pub struct NacosImportSpec {
    /// 配置 dataId(如 tidb.yml)。
    pub data_id: String,
    /// 分组(缺 → 回退 NacosBootstrap.group)。
    #[serde(default)]
    pub group: Option<String>,
    /// 文档缺失是否可跳过，缺省 true；必需文档须显式声明 false。
    #[serde(default = "default_true")]
    pub optional: bool,
    /// 本条内容格式(缺 → 回退全局 NacosBootstrap.file_extension;仍缺 → 默认 yaml)。snake_case。
    #[serde(default)]
    pub file_extension: Option<String>,
}

impl NacosBootstrap {
    /// 业务作用：在配置中心建连前确认功能已启用且配置了非空地址，避免关闭状态仍触发远端访问。
    ///
    /// 参数说明：无。
    ///
    /// 返回：enabled 为 true 且地址去除空白后非空时成功；否则返回错误，不验证导入列表或实际网络可达性。
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

    /// 业务作用：把引导配置转换为 Nacos 客户端连接参数，复用客户端构造器的默认策略。
    ///
    /// 参数说明：无。
    ///
    /// 返回：包含地址、命名空间、分组、应用名、认证和可选发现 IP 的参数；不检查 enabled，也不建立连接。
    pub fn to_props(&self) -> NacosProps {
        NacosProps::new(&self.server_addr)
            .with_namespace(&self.namespace)
            .with_group(&self.group)
            .with_app_name(&self.app_name)
            .with_auth(&self.username, &self.password)
            .with_discovery_ip_opt(self.discovery_ip.as_deref())
    }

    /// 业务作用：将引导配置中的 Nacos 导入项转换为加载器使用的中性声明，保留原有覆盖顺序。
    ///
    /// 参数说明：无。
    ///
    /// 返回：同序的远端导入列表；全空白分组和格式转为缺省，data_id 与可选性原样保留，此处不验证身份或格式。
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

/// 业务作用：在反序列化前按精确路径拒绝不受支持的配置声明，避免未知字段被忽略后悄然改变导入范围。
///
/// 参数说明：
/// - `boot_tree`：已经装配的原始引导配置树，包含本地配置与环境覆盖结果。
///
/// 返回：未出现 nacos.data_id 或 yml.nacos.imports 时成功；任一路径存在即返回迁移提示错误，包括显式 null。
/// 只检查这两个完整路径，不拒绝合法 nacos.imports 条目内部的 data_id。
pub fn reject_legacy_config_fields(boot_tree: &serde_json::Value) -> anyhow::Result<()> {
    // 在强类型绑定忽略未知字段之前拒绝这两个完整路径，不能误伤导入项内部合法的 data_id。
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

/// 业务作用：在同一份本地引导树上检查不受支持的字段并绑定目标类型，统一宿主的兼容引导入口。
///
/// 参数说明：
/// - `loader`：已配置本地来源、环境覆盖和占位符规则的 YmlLoader，不是业务回源闭包。
///
/// 返回：通过字段守卫并成功反序列化的引导配置；加载、字段检查或类型绑定失败时返回错误，不连接 Nacos。
pub fn load_bootstrap_checked<T: serde::de::DeserializeOwned>(
    loader: &YmlLoader,
) -> anyhow::Result<T> {
    let tree = loader.load_tree()?;
    reject_legacy_config_fields(&tree)?;
    serde_json::from_value(tree).context("config-boot: 反序列化引导配置失败")
}

/// 业务作用：为兼容加载入口组合最终导入列表，使 nacos.imports 先于 yml.imports 参与覆盖。
///
/// 参数说明：
/// - `boot_tree`：占位符已解析的引导配置树，用于字段守卫和 yml.imports 提取。
/// - `base_dir`：本地导入相对路径的解析基准目录。
/// - `nacos`：与同一引导树对应的 Nacos 配置，提供排在列表前面的远端导入项。
///
/// 返回：按两组声明顺序拼接的导入列表；不受支持的字段返回错误，不执行远端读取或严格重复身份校验。
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

/// 业务作用：enabled 路径校验最终 import 列表:至少一个 Nacos 项、每个 Nacos data_id 非空。
///
/// 参数说明：
/// - `_nacos`：保留的引导配置参数；本函数不读取其 enabled 或连接设置，调用方应先选择启用路径。
/// - `imports`：最终导入列表，只检查其中 Nacos 项的存在性和 data_id 非空约束。
///
/// 返回：至少一个 Nacos 项且每项 data_id 去除空白后非空时成功；否则返回错误，不校验格式或重复身份。
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

/// 业务作用：连接配置中心:先 `validate_connection_enabled`,再 `to_props` 连接。
///
/// 参数说明：
/// - `nacos`：提供功能开关、地址、命名空间、分组、应用名和认证信息的引导配置。
///
/// 返回：校验并连接成功的配置客户端；未启用、地址为空或客户端连接失败时返回错误，不拉取业务文档。
pub async fn connect_config_client(nacos: &NacosBootstrap) -> anyhow::Result<NacosConfigClient> {
    nacos.validate_connection_enabled()?;
    NacosConfigClient::connect(&nacos.to_props()).await
}

/// 业务作用：使用同一份引导默认值执行兼容 overlay 首拉，减少宿主重复传递分组和格式。
///
/// 参数说明：
/// - `client`：已经建立的配置中心客户端，负责读取远端引用。
/// - `imports`：决定 overlay 顺序的最终导入列表。
/// - `nacos`：提供默认分组和内容格式的引导配置；此包装不再次检查 enabled。
///
/// 返回：resolve_ordered_overlays 的装配结果或错误，继承该兼容入口的来源校验边界。
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

/// 业务作用：使用引导默认值将现有远端包重组为兼容 overlay，供宿主热刷新流程使用。
///
/// 参数说明：
/// - `imports`：需要保持覆盖顺序的导入列表，本地文件在装配时重新读取。
/// - `bundle`：调用方已取得并校验来源完整性的远端文档包。
/// - `nacos`：提供默认分组和内容格式的引导配置。
///
/// 返回：assemble_overlays_from_bundle 的结果或错误；不新增严格包一致性校验，也不发布应用配置视图。
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

/// 业务作用：使用引导默认分组和格式提取最终远端引用，供宿主统一注册配置订阅。
///
/// 参数说明：
/// - `imports`：已经按优先级合并的本地与远端导入列表。
/// - `nacos`：提供默认分组和可选内容格式的引导配置。
///
/// 返回：保持相对顺序的 ConfigRef 列表；缺省格式为 yaml，非法格式返回错误，此入口不启动订阅。
pub fn nacos_refs_for_bootstrap(
    imports: &[YmlImport],
    nacos: &NacosBootstrap,
) -> anyhow::Result<Vec<ConfigRef>> {
    nacos_refs(imports, &nacos.group, nacos.file_extension.as_deref())
}
