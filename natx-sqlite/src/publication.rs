use std::fs::{File, OpenOptions};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// 备份与恢复只向新目标发布；路径锁协议与正常启动一致。
pub(crate) struct Publication {
    pub(crate) temporary: PathBuf,
    destination: PathBuf,
    identity: (u64, u64),
    _lock: File,
    _process: crate::ownership::ProcessLock,
}

impl Publication {
    /// 业务作用：在目标路径权威下创建私有临时文件，拒绝覆盖主文件和孤立 sidecar。
    ///
    /// 参数说明：
    /// - `destination`：必须尚不存在的新目标路径，父目录必须已经存在。
    ///
    /// 返回：与目标名称分离的新临时文件及持续持有的目标锁；路径不安全、目标冲突或候选耗尽时失败。
    pub(crate) fn reserve(destination: &Path) -> anyhow::Result<Self> {
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .canonicalize()?;
        let metadata = std::fs::metadata(&parent)?;
        anyhow::ensure!(
            metadata.is_dir() && metadata.mode() & 0o022 == 0,
            "SQLite backup parent is not private"
        );
        let name = destination
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("SQLite backup filename is missing"))?;
        let destination = parent.join(name);
        let process = crate::ownership::ProcessLock::acquire(&destination)?;
        let mut lock_name = name.to_os_string();
        lock_name.push(".owner.lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(parent.join(lock_name))?;
        crate::ownership::validate_file(&lock.metadata()?)?;
        lock.try_lock()
            .map_err(|_| anyhow::anyhow!("SQLite backup destination is already owned"))?;
        require_absent(&destination)?;
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        for _ in 0..16 {
            let temporary = parent.join(format!(
                ".nasa-sqlite-{}-{}.db",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            // 目标 owner 锁已存在且持续持有；按同一命名协议查找候选锁，
            // 可在创建半成品前排除文件系统等价名称，也避免临时 sidecar 占用最终目标。
            if !temporary_available(&temporary)? {
                continue;
            }
            let file = match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&temporary)
            {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            };
            let metadata = file.metadata()?;
            return Ok(Self {
                temporary,
                destination,
                identity: (metadata.dev(), metadata.ino()),
                _lock: lock,
                _process: process,
            });
        }
        anyhow::bail!("SQLite backup temporary path is unavailable")
    }

    /// 业务作用：在内容校验和物理关闭之后发布备份，目标存在时绝不覆盖。
    ///
    /// 参数说明：
    /// - `check`：在发布前复验维护预算及来源资格，不能撤销已经完成的发布事实。
    ///
    /// 返回：文件和目录均已同步的目标路径；发布后同步失败保留目标供核验。
    pub(crate) fn publish(self, check: impl Fn() -> anyhow::Result<()>) -> anyhow::Result<PathBuf> {
        check()?;
        self.verify()?;
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut sidecar = self.temporary.as_os_str().to_os_string();
            sidecar.push(suffix);
            anyhow::ensure!(
                std::fs::symlink_metadata(PathBuf::from(sidecar))
                    .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound),
                "SQLite backup still depends on sidecar files"
            );
        }
        let durable = File::open(&self.temporary)?;
        durable.sync_all()?;
        #[cfg(target_os = "macos")]
        {
            use std::os::fd::AsRawFd;
            // 主文件与 SQLite 提交使用同一硬件刷新策略，目录发布另由后续 fsync 确认。
            if unsafe { libc::fcntl(durable.as_raw_fd(), libc::F_FULLFSYNC) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
        }
        require_absent(&self.destination)?;
        // 文件同步可能阻塞；返回后必须重新检查排空期限，尚未发布的材料不能继续产生新目标。
        check()?;
        // 同文件系统硬链接提供 no-replace 发布；成功后只撤销本次临时名字，不删除最终目标。
        std::fs::hard_link(&self.temporary, &self.destination)?;
        std::fs::remove_file(&self.temporary)?;
        File::open(self.destination.parent().expect("canonical parent"))?
            .sync_all()
            .map_err(|_| anyhow::anyhow!("SQLite backup publication durability is unconfirmed"))?;
        Ok(self.destination.clone())
    }

    /// 业务作用：删除或发布前复验临时文件身份，避免处理被替换的材料。
    ///
    /// 参数说明：无。
    ///
    /// 返回：仍为本次创建的普通文件时成功。
    fn verify(&self) -> anyhow::Result<()> {
        let metadata = std::fs::symlink_metadata(&self.temporary)?;
        anyhow::ensure!(
            metadata.is_file() && (metadata.dev(), metadata.ino()) == self.identity,
            "SQLite backup temporary identity changed"
        );
        Ok(())
    }
}

impl Drop for Publication {
    /// 业务作用：撤销本次未发布的临时材料，保留已有目标和稳定锁文件。
    ///
    /// 参数说明：无。
    ///
    /// 返回：只清理仍具有创建身份的精确临时路径。
    fn drop(&mut self) {
        if self.verify().is_ok() {
            for suffix in ["-wal", "-shm", "-journal", ""] {
                let mut path = self.temporary.as_os_str().to_os_string();
                path.push(suffix);
                let _ = std::fs::remove_file(PathBuf::from(path));
            }
        }
    }
}

/// 业务作用：排除已占用的临时材料，以及与最终目标等价的主文件或 sidecar 名称。
///
/// 参数说明：
/// - `path`：尚未创建的临时主文件候选路径。
///
/// 返回：全部材料及其 owner 锁名称均未占用时允许创建；查找失败时停止预留。
fn temporary_available(path: &Path) -> anyhow::Result<bool> {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let mut material = path.as_os_str().to_os_string();
        material.push(suffix);
        let mut authority = material.clone();
        authority.push(".owner.lock");
        for candidate in [PathBuf::from(material), PathBuf::from(authority)] {
            match std::fs::symlink_metadata(candidate) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
                Ok(_) => return Ok(false),
            }
        }
    }
    Ok(true)
}

/// 业务作用：防止新目标携带旧日志材料或覆盖用户已有数据库。
///
/// 参数说明：
/// - `path`：目标数据库的规范路径。
///
/// 返回：主文件与全部 sidecar 都不存在时成功。
fn require_absent(path: &Path) -> anyhow::Result<()> {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let mut candidate = path.as_os_str().to_os_string();
        candidate.push(suffix);
        match std::fs::symlink_metadata(PathBuf::from(candidate)) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
            Ok(_) => anyhow::bail!("SQLite backup destination or sidecar already exists"),
        }
    }
    Ok(())
}
