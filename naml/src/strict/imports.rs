use std::{
    cmp::Ordering,
    path::{Component, Path, PathBuf},
};

use serde_json::Value;

use super::{ConfigError, ConfigPath, ErrorKind, LoadLimits, Result};
use crate::{ConfigFormat, NacosImport, YmlImport};

/// 单目录文件模式，匹配和观察共用相同的语法。
#[derive(Clone, Eq, PartialEq)]
pub struct FilePattern {
    directory: PathBuf,
    filename: String,
    glob: bool,
}

impl FilePattern {
    /// 业务作用：限定文件名通配范围，禁止扩大为递归目录遍历。
    /// 参数说明：`path` 为已经确定目录的精确路径或模式。
    /// 返回：合法模式；父级跳转、目录通配和未支持的模式语法失败。
    pub fn new(path: &Path) -> Result<Self> {
        if path.as_os_str().len() > 4096 {
            return Err(ConfigError::new(ErrorKind::Limit));
        }
        if path.as_os_str().is_empty()
            || path
                .components()
                .any(|part| matches!(part, Component::ParentDir))
        {
            return Err(ConfigError::new(ErrorKind::Declaration));
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| ConfigError::new(ErrorKind::Encoding))?;
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let parent_text = parent
            .to_str()
            .ok_or_else(|| ConfigError::new(ErrorKind::Encoding))?;
        if name.contains("**")
            || name.contains(['[', ']', '{', '}', '\0'])
            || parent_text.contains(['*', '?', '[', ']', '{', '}', '\0'])
        {
            return Err(ConfigError::new(ErrorKind::Unsupported));
        }
        Ok(Self {
            directory: parent.into(),
            filename: name.into(),
            glob: name.contains(['*', '?']),
        })
    }

    /// 业务作用：返回模式的固定观察目录。
    /// 参数说明：无。
    /// 返回：不包含文件名通配符的目录。
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// 业务作用：区分精确路径和需要枚举的模式。
    /// 参数说明：无。
    /// 返回：存在文件名通配符时为真。
    pub fn is_glob(&self) -> bool {
        self.glob
    }

    /// 业务作用：取得逻辑模式路径，真实链接目标不参与文件名排序。
    /// 参数说明：无。
    /// 返回：原目录与文件名组合。
    pub fn path(&self) -> PathBuf {
        self.directory.join(&self.filename)
    }

    /// 业务作用：按完整文件名匹配，问号匹配一个 Unicode 标量。
    /// 参数说明：`name` 是目录项原始 UTF-8 文件名。
    /// 返回：满足模式且符合隐藏文件约定时为真。
    pub fn matches(&self, name: &str) -> bool {
        if name.len() > 4096
            || name.contains(['/', '\0'])
            || (name.starts_with('.') && !self.filename.starts_with('.'))
        {
            return false;
        }
        let pattern: Vec<char> = self.filename.chars().collect();
        let text: Vec<char> = name.chars().collect();
        let (mut p, mut t, mut star, mut retry) = (0, 0, None, 0);
        while t < text.len() {
            if p < pattern.len() && (pattern[p] == '?' || pattern[p] == text[t]) {
                p += 1;
                t += 1;
            } else if p < pattern.len() && pattern[p] == '*' {
                star = Some(p);
                p += 1;
                retry = t;
            } else if let Some(index) = star {
                retry += 1;
                t = retry;
                p = index + 1;
            } else {
                return false;
            }
        }
        while p < pattern.len() && pattern[p] == '*' {
            p += 1;
        }
        p == pattern.len()
    }

    /// 业务作用：让隔离读取者提供的目录清单复用同一匹配与自然排序规则。
    /// 参数说明：`names` 为单目录完整名称列表；`limits` 约束枚举与命中数量。
    /// 返回：自然升序的完整逻辑路径；条目或命中超限时失败。
    pub fn expand_names(
        &self,
        names: impl IntoIterator<Item = String>,
        limits: &LoadLimits,
    ) -> Result<Vec<PathBuf>> {
        let mut matches = Vec::new();
        for (count, name) in names.into_iter().enumerate() {
            if count >= limits.directory_entries || name.len() > 4096 {
                return Err(ConfigError::new(ErrorKind::Limit));
            }
            if name.is_empty() || name.contains(['/', '\0']) || matches!(name.as_str(), "." | "..")
            {
                return Err(ConfigError::new(ErrorKind::Declaration));
            }
            if self.matches(&name) {
                if matches.len() >= limits.matches_per_pattern {
                    return Err(ConfigError::new(ErrorKind::Limit));
                }
                matches.push(name);
            }
        }
        matches.sort_by(|left, right| natural_filename_cmp(left, right));
        if matches.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ConfigError::new(ErrorKind::Duplicate));
        }
        Ok(matches
            .into_iter()
            .map(|name| self.directory.join(name))
            .collect())
    }
}

/// 业务作用：按文件名自然升序确定同一通配符组的覆盖优先级。
/// 参数说明：`left/right` 为完整 UTF-8 文件名，包含扩展名。
/// 返回：数字按值比较，同值短数字段在前，其余原始字节比较；不依赖 locale 或固定宽度整数。
pub fn natural_filename_cmp(left: &str, right: &str) -> Ordering {
    let (left, right) = (left.as_bytes(), right.as_bytes());
    let (mut l, mut r) = (0, 0);
    while l < left.len() && r < right.len() {
        if left[l].is_ascii_digit() && right[r].is_ascii_digit() {
            let (start_l, start_r) = (l, r);
            while l < left.len() && left[l].is_ascii_digit() {
                l += 1;
            }
            while r < right.len() && right[r].is_ascii_digit() {
                r += 1;
            }
            let mut significant_l = start_l;
            let mut significant_r = start_r;
            while significant_l < l && left[significant_l] == b'0' {
                significant_l += 1;
            }
            while significant_r < r && right[significant_r] == b'0' {
                significant_r += 1;
            }
            let order = (l - significant_l)
                .cmp(&(r - significant_r))
                .then_with(|| left[significant_l..l].cmp(&right[significant_r..r]))
                .then_with(|| (l - start_l).cmp(&(r - start_r)));
            if order != Ordering::Equal {
                return order;
            }
        } else {
            let order = left[l].cmp(&right[r]);
            if order != Ordering::Equal {
                return order;
            }
            l += 1;
            r += 1;
        }
    }
    (left.len() - l).cmp(&(right.len() - r))
}

/// 业务作用：严格解释导入清单，任何非法项都阻止形成不完整来源计划。
/// 参数说明：`tree` 已完成引导依赖求值；`base_dir` 为可信相对路径根。
/// 返回：保持声明顺序的 file/Nacos 描述；map 必须显式声明 optional。
pub fn parse_imports_checked(tree: &Value, base_dir: &Path) -> Result<Vec<YmlImport>> {
    parse_imports_limited(tree, base_dir, super::LoadLimits::default().declarations)
}

/// 业务作用：让加载器的声明预算覆盖解析分配，独立入口仍使用默认上限。
/// 参数说明：`tree` 是引导树；`base_dir` 固定相对目录；`limit` 为本轮声明上限。
/// 返回：有界且保持声明顺序的来源列表。
pub(crate) fn parse_imports_limited(
    tree: &Value,
    base_dir: &Path,
    limit: usize,
) -> Result<Vec<YmlImport>> {
    let Some(yml) = tree.get("yml") else {
        return Ok(Vec::new());
    };
    if !yml.is_object() {
        return Err(ConfigError::new(ErrorKind::Declaration));
    }
    let Some(imports) = yml.get("imports") else {
        return Ok(Vec::new());
    };
    let imports = imports
        .as_array()
        .ok_or_else(|| ConfigError::new(ErrorKind::Declaration))?;
    if imports.len() > limit {
        return Err(ConfigError::new(ErrorKind::Limit));
    }
    let mut result = Vec::new();
    for (index, entry) in imports.iter().enumerate() {
        result.push(parse_entry(entry, base_dir).map_err(|error| {
            error.at(&ConfigPath::default().key("yml").key("imports").index(index))
        })?);
    }
    Ok(result)
}

/// 业务作用：区分来源缺失策略与已经取得内容的校验要求。
/// 参数说明：`entry` 是单项声明；`base_dir` 为相对文件根。
/// 返回：唯一来源描述，未知字段和互斥来源立即拒绝。
fn parse_entry(entry: &Value, base_dir: &Path) -> Result<YmlImport> {
    let invalid = || ConfigError::new(ErrorKind::Declaration);
    if let Some(text) = entry.as_str() {
        let text = text.trim();
        let (text, optional) = text
            .strip_prefix("optional:")
            .map_or((text, false), |text| (text.trim(), true));
        if let Some(name) = text.strip_prefix("nacos:") {
            if name.trim().is_empty() {
                return Err(invalid());
            }
            return Ok(YmlImport::Nacos(NacosImport {
                data_id: name.trim().into(),
                optional,
                group: None,
                file_extension: None,
            }));
        }
        let path = text.strip_prefix("file:").unwrap_or(text);
        return file_entry(path, base_dir, optional);
    }
    let map = entry.as_object().ok_or_else(invalid)?;
    let optional = map
        .get("optional")
        .and_then(Value::as_bool)
        .ok_or_else(invalid)?;
    if map.contains_key("file") == map.contains_key("nacos") {
        return Err(invalid());
    }
    if let Some(file) = map.get("file") {
        if map
            .keys()
            .any(|key| !matches!(key.as_str(), "file" | "optional"))
        {
            return Err(invalid());
        }
        return file_entry(file.as_str().ok_or_else(invalid)?, base_dir, optional);
    }
    if map.keys().any(|key| {
        !matches!(
            key.as_str(),
            "nacos" | "optional" | "group" | "file_extension"
        )
    }) {
        return Err(invalid());
    }
    let data_id = map
        .get("nacos")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(invalid)?
        .trim()
        .to_string();
    let read_text = |name: &str| -> Result<Option<String>> {
        map.get(name)
            .map(|value| {
                value
                    .as_str()
                    .filter(|text| !text.trim().is_empty())
                    .map(|text| text.trim().to_string())
                    .ok_or_else(invalid)
            })
            .transpose()
    };
    let group = read_text("group")?;
    let file_extension = read_text("file_extension")?;
    if let Some(extension) = &file_extension {
        ConfigFormat::from_extension(extension)
            .map_err(|_| ConfigError::new(ErrorKind::Unsupported))?;
    }
    Ok(YmlImport::Nacos(NacosImport {
        data_id,
        group,
        optional,
        file_extension,
    }))
}

impl std::fmt::Debug for FilePattern {
    /// 业务作用：模式默认诊断不泄露环境展开后的目录和文件名。
    /// 参数说明：`f` 是格式化目标。
    /// 返回：仅是否需要目录匹配的摘要。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilePattern")
            .field("glob", &self.glob)
            .finish_non_exhaustive()
    }
}

/// 业务作用：在访问来源前统一校验本地协议、格式和模式，再以主文档目录确定路径。
/// 参数说明：`text` 为声明路径；`base_dir` 为固定根；`optional` 只允许缺失。
/// 返回：合法逻辑来源；不支持的协议或扩展名与来源是否存在无关，均拒绝。
fn file_entry(text: &str, base_dir: &Path, optional: bool) -> Result<YmlImport> {
    if text.trim().is_empty() {
        return Err(ConfigError::new(ErrorKind::Declaration));
    }
    // 可选只允许缺失，不能让缺失状态掩盖非法声明；两种声明写法共用同一门禁。
    if text.contains(':') {
        return Err(ConfigError::new(ErrorKind::Unsupported));
    }
    let path = Path::new(text.trim());
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("yaml");
    ConfigFormat::from_extension(extension)
        .map_err(|_| ConfigError::new(ErrorKind::Unsupported))?;
    let path = if path.is_absolute() {
        path.into()
    } else {
        base_dir.join(path)
    };
    FilePattern::new(&path)?;
    Ok(YmlImport::File { path, optional })
}
