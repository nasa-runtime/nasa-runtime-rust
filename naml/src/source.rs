//! 本地配置来源解析与有界读取。

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

const PROFILE_EXTENSIONS: &[&str] = &["toml", "json", "yaml", "yml"];

/// 一次加载使用的本地文件集合，以及文件监听需要覆盖的候选路径。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct YmlLocalSources {
    base_file: PathBuf,
    active_profile_file: Option<PathBuf>,
    profile_watch_files: Vec<PathBuf>,
}

impl YmlLocalSources {
    /// 业务作用：返回必需主配置路径，供审计、指纹和错误定位复用同一来源事实。
    ///
    /// 参数说明：无。
    ///
    /// 返回：调用方配置的主文件路径。
    pub fn base_file(&self) -> &Path {
        &self.base_file
    }

    /// 业务作用：返回本轮实际参与合并的 profile 文件，避免应用自行猜测扩展名。
    ///
    /// 参数说明：无。
    ///
    /// 返回：profile 已启用且文件存在时返回路径，否则返回 `None`。
    pub fn active_profile_file(&self) -> Option<&Path> {
        self.active_profile_file.as_deref()
    }

    /// 业务作用：枚举本轮实际读取的本地配置文件，供内容指纹和归档准确覆盖 profile。
    ///
    /// 参数说明：无。
    ///
    /// 返回：始终先返回主文件，随后至多返回一个活动 profile 文件。
    pub fn loaded_files(&self) -> impl Iterator<Item = &Path> {
        std::iter::once(self.base_file.as_path()).chain(self.active_profile_file.as_deref())
    }

    /// 业务作用：返回可能改变有效配置的精确路径集合，使目录级 watcher 兼容原子 rename，
    /// 同时拒绝同目录无关文件触发热更新。
    ///
    /// 参数说明：无。
    ///
    /// 返回：主文件以及当前 profile 名对应的无扩展名、TOML、JSON、YAML 候选路径。
    pub fn watch_files(&self) -> impl Iterator<Item = &Path> {
        std::iter::once(self.base_file.as_path())
            .chain(self.profile_watch_files.iter().map(PathBuf::as_path))
    }

    /// 业务作用：借用 profile 的全部监听候选，供应用区分主文件变更和 profile 变更。
    ///
    /// 参数说明：无。
    ///
    /// 返回：未启用 profile 时为空，否则包含与 loader 探测顺序一致的候选路径。
    pub fn profile_watch_files(&self) -> &[PathBuf] {
        &self.profile_watch_files
    }
}

/// 业务作用：按 loader 的主文件、profile 环境变量和文件名模式解析本轮本地来源。
///
/// 参数说明：`base_file` 是必需主文件；`profile_env` 指定 profile 环境变量；
/// `profile_pattern` 包含可选的 `{profile}` 占位符；`env_override` 用于隔离进程环境。
///
/// 返回：实际加载文件和精确监听候选；已存在但格式不受支持的 profile 会失败闭合。
pub(crate) fn resolve_local_sources(
    base_file: &Path,
    profile_env: &str,
    profile_pattern: &str,
    env_override: Option<&config::Map<String, String>>,
) -> Result<YmlLocalSources> {
    let profile = match env_override {
        Some(values) => values.get(profile_env).cloned(),
        None => std::env::var(profile_env).ok(),
    }
    .map(|value| value.trim().to_string())
    .filter(|value| !value.is_empty());

    let Some(profile) = profile else {
        return Ok(YmlLocalSources {
            base_file: base_file.to_path_buf(),
            active_profile_file: None,
            profile_watch_files: Vec::new(),
        });
    };

    let profile_base = PathBuf::from(profile_pattern.replace("{profile}", &profile));
    let mut candidates = Vec::with_capacity(1 + PROFILE_EXTENSIONS.len());
    candidates.push(profile_base.clone());
    let mut extension_base = profile_base.clone();
    if extension_base.extension().is_some() {
        extension_base.as_mut_os_string().push(".placeholder");
    }
    for extension in PROFILE_EXTENSIONS {
        let mut candidate = extension_base.clone();
        candidate.set_extension(extension);
        if !candidates.contains(&candidate) {
            candidates.push(candidate);
        }
    }

    let active_profile_file = if profile_base.is_file() {
        let Some(extension) = profile_base.extension() else {
            anyhow::bail!(
                "yml: profile 文件 {} 缺少格式扩展名，仅支持 yaml/yml/json/toml；本轮探测候选: {}",
                profile_base.display(),
                display_candidates(&candidates)
            );
        };
        let extension = extension.to_str().with_context(|| {
            format!(
                "yml: profile 文件 {} 的扩展名不是 UTF-8；本轮探测候选: {}",
                profile_base.display(),
                display_candidates(&candidates)
            )
        })?;
        super::loader::ConfigFormat::from_extension(extension).with_context(|| {
            format!(
                "yml: profile 文件 {} 格式不受支持；本轮探测候选: {}",
                profile_base.display(),
                display_candidates(&candidates)
            )
        })?;
        Some(profile_base)
    } else {
        candidates
            .iter()
            .skip(1)
            .find(|candidate| candidate.is_file())
            .cloned()
    };

    Ok(YmlLocalSources {
        base_file: base_file.to_path_buf(),
        active_profile_file,
        profile_watch_files: candidates,
    })
}

/// 业务作用：从单一文件句柄有界读取本地配置，避免 metadata 预检后文件增长造成无界分配。
///
/// 参数说明：`path` 是配置源；`max_bytes` 为可选硬上限，`None` 表示保持兼容的不设上限行为。
///
/// 返回：去除 UTF-8 BOM、将非法 UTF-8 字节替换为 U+FFFD 后的文本；文件缺失、读取失败或超过上限时返回错误。
pub(crate) fn read_local_source(path: &Path, max_bytes: Option<usize>) -> Result<String> {
    let bytes = match max_bytes {
        Some(max_bytes) => {
            let mut file = File::open(path)
                .with_context(|| format!("yml: 打开本地配置 {} 失败", path.display()))?;
            let mut bytes = Vec::with_capacity(max_bytes.min(64 * 1024));
            file.by_ref()
                .take(max_bytes as u64)
                .read_to_end(&mut bytes)
                .with_context(|| format!("yml: 读取本地配置 {} 失败", path.display()))?;
            let mut extra = [0_u8; 1];
            let exceeds_limit = file
                .read(&mut extra)
                .with_context(|| format!("yml: 检查本地配置 {} 读取上限失败", path.display()))?
                != 0;
            anyhow::ensure!(
                !exceeds_limit,
                "yml: 本地配置 {} 超过单文件上限 {} bytes",
                path.display(),
                max_bytes
            );
            bytes
        }
        _ => std::fs::read(path)
            .with_context(|| format!("yml: 读取本地配置 {} 失败", path.display()))?,
    };
    let bytes = bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(&bytes);
    Ok(String::from_utf8_lossy(bytes).into_owned())
}

/// 业务作用：把 profile 探测候选格式化进失败信息，帮助运维区分缺扩展名、编码和格式错误。
///
/// 参数说明：`candidates` 是与实际探测顺序一致的路径列表。
///
/// 返回：使用逗号分隔的可读路径文本。
fn display_candidates(candidates: &[PathBuf]) -> String {
    candidates
        .iter()
        .map(|candidate| candidate.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}
