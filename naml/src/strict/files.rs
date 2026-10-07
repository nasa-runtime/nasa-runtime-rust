use sha2::{Digest, Sha256};
use std::{
    fmt,
    fs::{self, Metadata, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    time::SystemTime,
};

use super::{ConfigError, ErrorKind, FilePattern, LoadPolicy, Result, SourceDocument};
use crate::ConfigFormat;

/// 实际打开文件取得的身份，供同轮来源去重和发布前复验。
#[derive(Clone, Eq, PartialEq)]
pub struct FileEvidence {
    path: PathBuf,
    canonical: PathBuf,
    identity: Identity,
    digest: [u8; 32],
}

#[derive(Clone, Eq, PartialEq)]
struct Identity {
    len: u64,
    modified: Option<SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

/// 同一次读取的文档和身份；内容摘要不通过默认诊断输出。
pub struct ReadSource {
    /// 本次读取取得的完整文本及格式。
    pub document: SourceDocument,
    /// 与文档同次取得的文件身份和内容摘要，用于候选发布前复验。
    pub evidence: FileEvidence,
}

impl FileEvidence {
    /// 业务作用：显式取得声明路径以建立对应观察。
    /// 参数说明：无。
    /// 返回：实际读取时使用的逻辑路径。
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 业务作用：提供本次实际读取的内容身份给内部去重逻辑。
    /// 参数说明：无。
    /// 返回：摘要材料，调用方不得用于公开指标或口令指纹日志。
    pub fn content_digest(&self) -> &[u8; 32] {
        &self.digest
    }

    /// 业务作用：识别符号链接和支持平台上的硬链接重复来源。
    /// 参数说明：`other` 为另一份已读取文件的身份。
    /// 返回：两次读取指向同一文件时为真。
    pub fn same_file(&self, other: &Self) -> bool {
        #[cfg(unix)]
        {
            self.identity.device == other.identity.device
                && self.identity.inode == other.identity.inode
        }
        #[cfg(not(unix))]
        {
            self.canonical == other.canonical
        }
    }

    /// 业务作用：区分相同值来自不同文件身份的观察变化。
    /// 参数说明：无。
    /// 返回：供内部对账使用的摘要，不适合公开日志或指标。
    pub fn observation_fingerprint(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        for path in [&self.path, &self.canonical] {
            let bytes = path.as_os_str().as_encoded_bytes();
            hash.update((bytes.len() as u64).to_be_bytes());
            hash.update(bytes);
        }
        #[cfg(unix)]
        {
            hash.update(self.identity.device.to_be_bytes());
            hash.update(self.identity.inode.to_be_bytes());
        }
        hash.update(self.digest);
        hash.finalize().into()
    }

    /// 业务作用：复验已读文档内容，拒绝已知改变的候选。
    /// 参数说明：`policy` 是原来源读取规则。
    /// 返回：身份和内容均未改变时成功。
    pub fn revalidate(&self, policy: &LoadPolicy) -> Result<()> {
        let next = read_source(&self.path, false, policy)?
            .ok_or_else(|| ConfigError::new(ErrorKind::SourceChanged))?;
        if self != &next.evidence {
            return Err(ConfigError::new(ErrorKind::SourceChanged));
        }
        Ok(())
    }
}

impl fmt::Debug for FileEvidence {
    /// 业务作用：默认诊断不显示文件路径和原文摘要。
    /// 参数说明：`f` 是格式化目标。
    /// 返回：仅文件规模。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileEvidence")
            .field("bytes", &self.identity.len)
            .finish_non_exhaustive()
    }
}

/// 业务作用：以同一文件句柄读取有限的普通文件并取得来源证据。
/// 参数说明：`path` 为逻辑路径；`optional` 只允许不存在；`policy` 控制根目录和读取预算。
/// 返回：文档及证据，可选不存在返回 None；异常来源不被吞掉。
pub fn read_source(path: &Path, optional: bool, policy: &LoadPolicy) -> Result<Option<ReadSource>> {
    policy.check_cancelled()?;
    let canonical = match checked_path(path, policy) {
        Ok(path) => path,
        Err(error) if optional && error.kind == ErrorKind::Missing => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // 非阻塞打开避免普通文件预检后被替换成 FIFO 时无限等待；仍不承诺终止普通文件的内核 I/O。
        options.custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC);
    }
    let mut file = options.open(path).map_err(io_error)?;
    let before = file.metadata().map_err(io_error)?;
    if !before.is_file() {
        return Err(ConfigError::new(ErrorKind::FileType));
    }
    if before.len() > policy.limits.source_bytes as u64 {
        return Err(ConfigError::new(ErrorKind::Limit));
    }
    let mut bytes = Vec::new();
    let mut block = [0u8; 8192];
    loop {
        policy.check_cancelled()?;
        let size = file.read(&mut block).map_err(io_error)?;
        if size == 0 {
            break;
        }
        let next = bytes
            .len()
            .checked_add(size)
            .ok_or_else(|| ConfigError::new(ErrorKind::Limit))?;
        if next > policy.limits.source_bytes {
            return Err(ConfigError::new(ErrorKind::Limit));
        }
        bytes.extend_from_slice(&block[..size]);
    }
    let identity = identity(&before);
    let after = file.metadata().map_err(io_error)?;
    if identity != self::identity(&after)
        || canonical != checked_path(path, policy)?
        || self::identity(&fs::metadata(path).map_err(io_error)?) != identity
    {
        return Err(ConfigError::new(ErrorKind::SourceChanged));
    }
    let extension = path
        .extension()
        .map(|value| {
            value
                .to_str()
                .ok_or_else(|| ConfigError::new(ErrorKind::Encoding))
        })
        .transpose()?
        .unwrap_or("yaml");
    let format = ConfigFormat::from_extension(extension)
        .map_err(|_| ConfigError::new(ErrorKind::Unsupported))?;
    let evidence = FileEvidence {
        path: path.into(),
        canonical,
        identity,
        digest: Sha256::digest(&bytes).into(),
    };
    Ok(Some(ReadSource {
        document: SourceDocument::new(
            path.to_str()
                .ok_or_else(|| ConfigError::new(ErrorKind::Encoding))?,
            format,
            bytes,
        ),
        evidence,
    }))
}

/// 业务作用：展开单目录模式并复用自然排序，零匹配严格遵循 optional。
/// 参数说明：`pattern` 为合法模式；`optional` 允许目录或匹配集合缺失；`policy` 为读取限制。
/// 返回：稳定有序的逻辑路径，目录异常和预算超限均失败。
pub fn expand_pattern(
    pattern: &FilePattern,
    optional: bool,
    policy: &LoadPolicy,
) -> Result<Vec<PathBuf>> {
    policy.check_cancelled()?;
    if !pattern.is_glob() {
        return Ok(vec![pattern.path()]);
    }
    match checked_path(pattern.directory(), policy) {
        Ok(_) => {}
        Err(error) if optional && error.kind == ErrorKind::Missing => return Ok(Vec::new()),
        Err(error) => return Err(error),
    }
    let entries = fs::read_dir(pattern.directory()).map_err(io_error)?;
    let mut names = Vec::new();
    for entry in entries {
        policy.check_cancelled()?;
        if names.len() >= policy.limits.directory_entries {
            return Err(ConfigError::new(ErrorKind::Limit));
        }
        let entry = entry.map_err(io_error)?;
        names.push(
            entry
                .file_name()
                .into_string()
                .map_err(|_| ConfigError::new(ErrorKind::Encoding))?,
        );
    }
    let result = pattern.expand_names(names, &policy.limits)?;
    if result.is_empty() && !optional {
        return Err(ConfigError::new(ErrorKind::Missing));
    }
    Ok(result)
}

/// 业务作用：检查受信目录与链接状态，不把损坏链接伪装成可选缺失。
/// 参数说明：`path` 是逻辑路径；`policy` 包含允许根。
/// 返回：当前真实路径；此检查不声称抵御恶意本机并发替换者。
fn checked_path(path: &Path, policy: &LoadPolicy) -> Result<PathBuf> {
    let mut prefix = PathBuf::new();
    for part in path.components() {
        if matches!(part, std::path::Component::ParentDir) {
            return Err(ConfigError::new(ErrorKind::Declaration));
        }
        prefix.push(part);
        if let Ok(metadata) = fs::symlink_metadata(&prefix) {
            if metadata.file_type().is_symlink() && fs::canonicalize(&prefix).is_err() {
                return Err(ConfigError::new(ErrorKind::Unreadable));
            }
        }
    }
    let canonical = fs::canonicalize(path).map_err(io_error)?;
    if !policy.allowed_roots.is_empty() {
        let mut permitted = false;
        for root in &policy.allowed_roots {
            let root = fs::canonicalize(root).map_err(io_error)?;
            permitted |= canonical.starts_with(root);
        }
        if !permitted {
            return Err(ConfigError::new(ErrorKind::PolicyChanged));
        }
    }
    Ok(canonical)
}

/// 业务作用：从已打开文件的 metadata 取得可复验身份。
/// 参数说明：`metadata` 为句柄或复验路径的元数据。
/// 返回：文件尺寸、修改时间和平台身份。
fn identity(metadata: &Metadata) -> Identity {
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    Identity {
        len: metadata.len(),
        modified: metadata.modified().ok(),
        #[cfg(unix)]
        device: metadata.dev(),
        #[cfg(unix)]
        inode: metadata.ino(),
    }
}

/// 业务作用：保留不存在与其它 I/O 失败的区别，同时隐藏路径。
/// 参数说明：`error` 为操作系统错误。
/// 返回：安全稳定类别。
fn io_error(error: std::io::Error) -> ConfigError {
    ConfigError::new(if error.kind() == std::io::ErrorKind::NotFound {
        ErrorKind::Missing
    } else {
        ErrorKind::Unreadable
    })
}
