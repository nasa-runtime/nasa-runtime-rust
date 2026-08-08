//! import 列表解析(对照)。
//!
//! Rust 侧不兼容其它生态的配置导入语法,用自己的 `yml` 段表达导入:
//!
//! - `yml.imports`:完整显式列表,可混合本地 `file:` 与远端 `nacos:`,表达力更强(交错保序)。
//!
//! 注:纯 Nacos 简写现由顶层 `nacos.imports`(强类型 `NacosImportSpec`,在 config-boot)表达,
//! 不再走 `yml.nacos.imports`;本 crate 只解析 `yml.imports`。
//!
//! 【边界:零 Nacos 依赖】本模块只把导入项解析成【中性】描述 [`YmlImport`]:
//!   `File` 已按 base_dir 解析成最终路径;`Nacos` 只带 data_id/group/optional 文本。
//!   真正「按描述去 Nacos 拉取」的胶水在 nasa 门面层(它把 [`NacosImport`] 映射成 `nacos::ConfigRef`)。
//!
//! 【前置:占位符已解析】传入的 `tree` 应已完成 `${}` 占位符解析(见 [`crate::resolve_placeholders`]):
//!   这样 `nacos:${server.name}.yml` 里的占位符在解析 import 前就已解开、算出真正的 dataId。

use std::path::{Path, PathBuf};

/// `yml.imports`(及 config-boot 的 `nacos.imports`)解析出的单个导入项(中性,零 Nacos 依赖)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum YmlImport {
    /// 本地文件 overlay。`path` 已按 base_dir 解析成最终路径(相对项已 join base_dir)。
    File {
        /// 本地配置文件绝对路径或已按 base_dir 归一后的路径。
        path: PathBuf,
        /// 缺失时是否可跳过。false → 文件不存在启动失败;true → 不存在跳过。
        optional: bool,
    },
    /// 远端 Nacos 配置(中性描述;门面层据此构造 `nacos::ConfigRef` 去拉取)。
    Nacos(NacosImport),
}

/// Nacos 导入的中性描述(不引用 `nacos` crate 的任何类型,保住 yml 对 Nacos 零认知)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NacosImport {
    /// 配置项 ID,如 `tidb.yml`、`fore-rest.yml`。
    pub data_id: String,
    /// 分组;`None` 表示用默认分组(门面层回退到 `NacosProps.group`)。
    pub group: Option<String>,
    /// 缺失时是否可跳过。false → 拉取失败启动失败;true → 失败 warn+skip。
    pub optional: bool,
    /// 本条声明的内容格式 `file_extension`(如 `yaml`/`json`);`None` → 门面层回退全局 `nacos.file_extension`(仍缺默认 yaml)。
    /// 中性字符串,yml 不解释;门面层(config-boot)按优先级解析并 fail-fast。
    pub file_extension: Option<String>,
}

/// 业务作用：从合并后的配置树解析 `yml.imports` 列表(对照)。
///
/// # 参数
/// - `tree`: 已合并并解析占位符后的配置 Value 树。
/// - `base_dir`: 本地 `file:` import 相对路径的基准目录。
///
/// 顺序 = 覆盖顺序(后加载覆盖先加载),file 与 nacos 交错保序。
/// `base_dir`:本地 `file:` 相对路径的基准目录(通常是主配置文件所在目录,默认 `zcf/`);
///   以 `/` 开头的路径按绝对路径处理,不 join base_dir。
///
/// 只读 `yml.imports`:纯 Nacos 简写已迁到顶层 `nacos.imports`(config-boot 的 `NacosImportSpec`),
/// 本 crate 不再解析 `yml.nacos.imports`。
///
/// 无法识别的条目(未知前缀、缺 file/nacos 的 map)会 warn 并跳过,不误当成本地文件或 Nacos。
pub fn parse_imports_from_tree(tree: &serde_json::Value, base_dir: &Path) -> Vec<YmlImport> {
    let mut out = Vec::new();

    // yml.imports:file/nacos 混合列表(声明顺序即覆盖顺序,file 与 nacos 交错保序)。
    if let Some(arr) = tree
        .get("yml")
        .and_then(|y| y.get("imports"))
        .and_then(|i| i.as_array())
    {
        for entry in arr {
            if let Some(imp) = parse_mixed_entry(entry, base_dir) {
                out.push(imp);
            }
        }
    }

    out
}

/// 业务作用：解析 `yml.imports` 的一项:字符串(`file:`/`optional:file:`/`nacos:`/`optional:nacos:`)或
/// map(`{file, optional?}` / `{nacos, group?, optional?}`)。
///
/// # 参数
/// - `entry`: `yml.imports` 中的一条字符串或 map 声明。
/// - `base_dir`: 相对配置导入路径的基准目录。
fn parse_mixed_entry(entry: &serde_json::Value, base_dir: &Path) -> Option<YmlImport> {
    match entry {
        serde_json::Value::String(s) => parse_import_string(s, base_dir),
        serde_json::Value::Object(_) => parse_import_map(entry, base_dir),
        _ => {
            tracing::warn!(entry = %entry, "yml: yml.imports 条目类型无法识别,跳过");
            None
        }
    }
}

/// 业务作用：字符串条目解析。
///
/// ⚠️【optional 默认值:字符串形式与 map 形式【故意不同】】
///   - 字符串形式:**无 `optional:` 前缀 = required**(如 `file:common.yml` 是必需)。
///     `optional:` 前缀表示可缺;不写就是必需。
///   - map 形式:**省略 `optional` = optional(true)**(见 [`read_optional_default_true`])。
///
/// # 参数
/// - `raw`: 待解析的原始字符串、字节或配置值。
/// - `base_dir`: 相对配置导入路径的基准目录。
fn parse_import_string(raw: &str, base_dir: &Path) -> Option<YmlImport> {
    let s = raw.trim();
    // `optional:` 前缀决定可缺性;不带前缀的字符串形式默认 required(false)。
    let (rest, optional) = match s.strip_prefix("optional:") {
        Some(r) => (r.trim(), true),
        None => (s, false),
    };

    if let Some(path) = rest.strip_prefix("file:") {
        return file_import(path.trim(), base_dir, optional);
    }
    if let Some(data_id) = rest.strip_prefix("nacos:") {
        let data_id = data_id.trim();
        if data_id.is_empty() {
            tracing::warn!(entry = %raw, "yml: nacos: 条目缺 data_id,跳过");
            return None;
        }
        return Some(YmlImport::Nacos(NacosImport {
            data_id: data_id.to_string(),
            group: None,
            optional,
            // 字符串形式不携带 file_extension → None,由门面层回退全局默认(仍缺默认 yaml)。
            file_extension: None,
        }));
    }
    // 其它已知不支持的 scheme(如 http:/classpath:)→ warn+skip,不误当本地文件。
    if looks_like_scheme(rest) {
        tracing::warn!(entry = %raw, "yml: 无法识别的 import 前缀,跳过(仅支持 file:/nacos:)");
        return None;
    }
    // 无 scheme 的裸路径 → 当本地文件(相对 base_dir)。
    file_import(rest, base_dir, optional)
}

/// 业务作用：map 条目解析:`{file: ..., optional?}` 或 `{nacos: ..., group?, optional?}`。optional 省略默认 true。
///
/// # 参数
/// - `entry`: `yml.imports` 中的 map 声明。
/// - `base_dir`: 相对配置导入路径的基准目录。
fn parse_import_map(entry: &serde_json::Value, base_dir: &Path) -> Option<YmlImport> {
    let optional = read_optional_default_true(entry);

    if let Some(path) = entry.get("file").and_then(|v| v.as_str()) {
        return file_import(path.trim(), base_dir, optional);
    }
    if let Some(data_id) = entry.get("nacos").and_then(|v| v.as_str()) {
        let data_id = data_id.trim();
        if data_id.is_empty() {
            tracing::warn!(entry = %entry, "yml: yml.imports 的 nacos 条目缺 data_id,跳过");
            return None;
        }
        return Some(YmlImport::Nacos(NacosImport {
            data_id: data_id.to_string(),
            group: read_group(entry),
            optional,
            file_extension: read_file_extension(entry),
        }));
    }
    tracing::warn!(entry = %entry, "yml: yml.imports 的 map 条目既无 file 也无 nacos,跳过");
    None
}

/// 业务作用：造 `File` 导入:相对路径 join base_dir;以 `/` 开头按绝对路径(不 join)。
/// 防穿越:拒绝含 `..` 组件的路径——尤其 Nacos 远端下发的 import 列表(不受信),避免 `file:../../secret`
/// 把配置根外的任意本地文件读进有效配置。绝对路径允许(bootstrap 常用绝对配置路径,是本地可信输入)。
///
/// # 参数
/// - `path`: `file:` import 声明中的本地配置文件路径。
/// - `base_dir`: 相对配置导入路径的基准目录。
/// - `optional`: 资源缺失时是否允许跳过。
fn file_import(path: &str, base_dir: &Path, optional: bool) -> Option<YmlImport> {
    let p = Path::new(path);
    if p.components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        tracing::warn!(path = %path, "yml: file: import 含 `..` 路径穿越,跳过");
        return None;
    }
    let full = if p.is_absolute() {
        p.to_path_buf()
    } else {
        base_dir.join(p)
    };
    Some(YmlImport::File {
        path: full,
        optional,
    })
}

/// 业务作用：读 map 的 `group` 字段(缺省 `None`)。
///
/// # 参数
/// - `entry`: `yml.imports` 中的 nacos map 声明。
fn read_group(entry: &serde_json::Value) -> Option<String> {
    entry
        .get("group")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// 业务作用：读 map 的 `optional` 字段,**缺省 `true`**(显式配置 optional 默认 true)。
///
/// # 参数
/// - `entry`: `yml.imports` 中的 map 声明。
fn read_optional_default_true(entry: &serde_json::Value) -> bool {
    entry
        .get("optional")
        .and_then(|v| v.as_bool())
        .unwrap_or(true)
}

/// 业务作用：读 map 的 `file_extension` 字段(snake_case;缺省 `None` → 门面层回退全局 `nacos.file_extension`,仍缺默认 yaml)。
///
/// # 参数
/// - `entry`: `yml.imports` 中的 nacos map 声明。
fn read_file_extension(entry: &serde_json::Value) -> Option<String> {
    entry
        .get("file_extension")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// 业务作用：是否形似 `scheme:...`(scheme 为 `[A-Za-z][A-Za-z0-9+.-]*`)。用于把 `file:`/`nacos:` 之外的
/// 已知 scheme(http:/classpath: 等)识别出来 warn+skip,而不把裸文件名误判成 scheme。
///
/// # 参数
/// - `s`: 要解析的输入字符串。
fn looks_like_scheme(s: &str) -> bool {
    match s.split_once(':') {
        Some((scheme, _)) if !scheme.is_empty() => {
            let mut chars = scheme.chars();
            let first_ok = chars.next().is_some_and(|c| c.is_ascii_alphabetic());
            first_ok && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '.' | '-'))
        }
        _ => false,
    }
}
