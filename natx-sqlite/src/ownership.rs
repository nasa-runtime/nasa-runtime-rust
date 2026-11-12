use std::fs::{File, OpenOptions};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, OwnedSemaphorePermit};

/// 文件权威持续覆盖 SQLite 原生连接，不以异步调用方是否仍等待作为释放依据。
pub(crate) struct FileOwner {
    pub(crate) path: PathBuf,
    identity: (u64, u64),
    lock: File,
    lock_path: PathBuf,
    process: Mutex<Option<ProcessLock>>,
    releasing: Mutex<bool>,
    companion: Mutex<Option<Arc<dyn Send + Sync>>>,
    pub(crate) live: AtomicUsize,
    pub(crate) changed: Notify,
}

impl FileOwner {
    /// 业务作用：取得稳定路径锁并固定主文件身份，阻止两个受管来源同时接管一个文件。
    ///
    /// 参数说明：
    /// - `path`：相对于已固定工作目录的数据库路径。
    /// - `create`：仅授权本次启动创建缺失主文件。
    /// - `parents`：允许建立缺失的专用父目录。
    /// - `read_only`：禁止新建主文件和写入数据库。
    ///
    /// 返回：文件类型、权限和独占锁均成立时返回 owner 与新建标记；失败不会删除已有材料。
    pub(crate) fn acquire(
        path: &Path,
        create: bool,
        parents: bool,
        read_only: bool,
    ) -> anyhow::Result<(Arc<Self>, bool)> {
        let absolute = if path.is_absolute() {
            path.to_owned()
        } else {
            std::env::current_dir()?.join(path)
        };
        let parent = absolute
            .parent()
            .ok_or_else(|| anyhow::anyhow!("SQLite parent directory is missing"))?;
        if parents && !parent.exists() {
            create_private_directories(parent)?;
        }
        let parent = parent.canonicalize()?;
        let metadata = std::fs::metadata(&parent)?;
        anyhow::ensure!(
            metadata.is_dir() && metadata.mode() & 0o022 == 0,
            "SQLite parent directory must not be writable by other accounts"
        );
        let filename = absolute
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("SQLite filename is missing"))?;
        let path = parent.join(filename);
        let process = ProcessLock::acquire(&path)?;
        let mut lock_name = filename.to_os_string();
        lock_name.push(".owner.lock");
        let lock_path = parent.join(lock_name);
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&lock_path)?;
        validate_file(&lock.metadata()?)?;
        // 锁文件保持原 inode；正常退出不 unlink，避免后来的实例锁住另一个同名文件。
        lock.try_lock()
            .map_err(|_| anyhow::anyhow!("SQLite file is already owned"))?;
        let mut created = false;
        let file = match OpenOptions::new()
            .read(true)
            .write(!read_only)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && create && !read_only => {
                // 缺失主文件不能复用遗留日志，否则新库可能混入旧库未完成的持久状态。
                for suffix in ["-wal", "-shm", "-journal"] {
                    let mut sidecar = path.as_os_str().to_os_string();
                    sidecar.push(suffix);
                    match std::fs::symlink_metadata(PathBuf::from(sidecar)) {
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error.into()),
                        Ok(_) => anyhow::bail!("SQLite creation refuses orphan sidecar files"),
                    }
                }
                // 创建仅发生在持有路径权威的启动阶段；后续连接一律不带创建选项。
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                    .open(&path)?;
                created = true;
                file
            }
            Err(error) => return Err(error.into()),
        };
        let metadata = file.metadata()?;
        validate_file(&metadata)?;
        let owner = Arc::new(Self {
            path,
            identity: (metadata.dev(), metadata.ino()),
            lock,
            lock_path,
            process: Mutex::new(Some(process)),
            releasing: Mutex::new(false),
            companion: Mutex::new(None),
            live: AtomicUsize::new(0),
            changed: Notify::new(),
        });
        owner.verify()?;
        Ok((owner, created))
    }

    /// 业务作用：撤销新物理连接的权威，在最后一个原生访问者退出后释放路径锁。
    ///
    /// 参数说明：无。
    ///
    /// 返回：闲置时立即解锁，否则由物理析构完成解锁。
    pub(crate) fn release_when_idle(&self) {
        let mut releasing = self
            .releasing
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *releasing = true;
        let companion = if self.live.load(Ordering::Acquire) == 0 {
            let _ = self.lock.unlock();
            self.process
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take();
            self.companion
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
        } else {
            None
        };
        drop(releasing);
        // 宿主守卫的析构可能取得业务锁，必须在离开文件权威内部互斥后执行。
        drop(companion);
    }

    /// 业务作用：让宿主数据卷权威覆盖真实原生访问，防止启动回滚或运行时退出提前交接外部对象目录。
    ///
    /// 参数说明：
    /// - `guard`：不引用此 datasource 的宿主独占权守卫。
    ///
    /// 返回：每个文件 owner 只接纳一次，已开始释放或已绑定时拒绝。
    pub(crate) fn retain_companion(&self, guard: Arc<dyn Send + Sync>) -> anyhow::Result<()> {
        let releasing = self
            .releasing
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        anyhow::ensure!(!*releasing, "SQLite file owner is closing");
        let mut companion = self
            .companion
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        anyhow::ensure!(
            companion.is_none(),
            "SQLite file owner already has a companion guard"
        );
        *companion = Some(guard);
        Ok(())
    }

    /// 业务作用：在每次物理建连前复验文件与 sidecar，避免重连误开替换文件或空库。
    ///
    /// 参数说明：无。
    ///
    /// 返回：身份和权限保持成立时成功；任何漂移都拒绝继续打开。
    pub(crate) fn verify(&self) -> anyhow::Result<()> {
        let lock_metadata = std::fs::symlink_metadata(&self.lock_path)?;
        let held_lock = self.lock.metadata()?;
        validate_file(&lock_metadata)?;
        anyhow::ensure!(
            (lock_metadata.dev(), lock_metadata.ino()) == (held_lock.dev(), held_lock.ino()),
            "SQLite owner lock identity changed"
        );
        let metadata = std::fs::symlink_metadata(&self.path)?;
        validate_file(&metadata)?;
        anyhow::ensure!(
            (metadata.dev(), metadata.ino()) == self.identity,
            "SQLite file identity changed"
        );
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut sidecar = self.path.as_os_str().to_os_string();
            sidecar.push(suffix);
            let sidecar = PathBuf::from(sidecar);
            match std::fs::symlink_metadata(&sidecar) {
                Ok(metadata) => validate_sidecar(&sidecar, &metadata)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

/// 业务作用：校验可由原生关闭删除的辅助文件，区分已解除目录链接的旧快照与当前仍可访问的材料。
///
/// 参数说明：
/// - `path`：同一受管目录内的 WAL、SHM 或 journal 路径。
/// - `metadata`：不跟随符号链接取得的初次快照。
///
/// 返回：现存材料满足普通、单链接及私有权限时成功；已删除的旧快照只允许复验一次当前路径。
fn validate_sidecar(path: &Path, metadata: &std::fs::Metadata) -> anyhow::Result<()> {
    if metadata.is_file() && metadata.nlink() == 0 {
        // 最后一个连接关闭可与新建连重叠；零链接快照已不代表目录中的对象，不能据此隔离整个来源。
        return match std::fs::symlink_metadata(path) {
            Ok(current) => validate_file(&current),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        };
    }
    validate_file(metadata)
}

/// 物理容量与文件权威由原生连接析构回调保活，覆盖 worker 尚未结束的异步取消。
pub(crate) struct PhysicalLifetime {
    pub(crate) owner: Arc<FileOwner>,
    pub(crate) capacity: Arc<LeaseCapacity>,
    _permit: OwnedSemaphorePermit,
}

#[derive(Default)]
pub(crate) struct LeaseCapacity(Mutex<(bool, Option<OwnedSemaphorePermit>)>);

impl LeaseCapacity {
    /// 业务作用：将逻辑借用绑定到尚未物理关闭的连接，避免关闭与借出竞争留下永久占位。
    ///
    /// 参数说明：
    /// - `permit`：本次借用获得的来源容量。
    ///
    /// 返回：连接仍存活且没有其它借用时接纳，否则交还原凭证供调用方继续选择连接。
    pub(crate) fn acquire(&self, permit: OwnedSemaphorePermit) -> Result<(), OwnedSemaphorePermit> {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if state.0 || state.1.is_some() {
            return Err(permit);
        }
        state.1 = Some(permit);
        Ok(())
    }

    /// 业务作用：正常归还可复用连接后释放逻辑借用，允许下一位调用者进入池。
    ///
    /// 参数说明：无。
    ///
    /// 返回：只归还借用容量，不撤销物理连接的文件权威。
    pub(crate) fn release(&self) {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .1
            .take();
    }

    /// 业务作用：物理访问最终结束后收回失效句柄的逻辑占位，旧句柄不能再次取得该连接。
    ///
    /// 参数说明：无。
    ///
    /// 返回：永久关闭本连接的借用入口并归还剩余容量。
    fn close(&self) {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.0 = true;
        state.1.take();
    }
}

impl PhysicalLifetime {
    /// 业务作用：在开始打开物理连接之前占据容量并登记尚未退出的文件访问责任。
    ///
    /// 参数说明：
    /// - `owner`：本次数据库文件的独占 owner。
    /// - `permit`：受管物理连接容量。
    ///
    /// 返回：直到原生析构完成才解除的共享责任。
    pub(crate) fn new(
        owner: Arc<FileOwner>,
        permit: OwnedSemaphorePermit,
    ) -> anyhow::Result<Arc<Self>> {
        {
            let releasing = owner
                .releasing
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            anyhow::ensure!(!*releasing, "SQLite file owner is stopped");
            owner.live.fetch_add(1, Ordering::AcqRel);
        }
        Ok(Arc::new(Self {
            owner,
            capacity: Arc::new(LeaseCapacity::default()),
            _permit: permit,
        }))
    }
}

impl Drop for PhysicalLifetime {
    /// 业务作用：最后一个原生连接引用结束后发布物理退出证据。
    ///
    /// 参数说明：无。
    ///
    /// 返回：归还连接容量并唤醒等待关闭的拥有者。
    fn drop(&mut self) {
        // 必须等 actor 与原生析构保活责任均结束，不能因 Rust 句柄到期就提前放开实际连接容量。
        self.capacity.close();
        let releasing = self
            .owner
            .releasing
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let companion = if self.owner.live.fetch_sub(1, Ordering::AcqRel) == 1 && *releasing {
            let _ = self.owner.lock.unlock();
            self.owner
                .process
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take();
            self.owner
                .companion
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
        } else {
            None
        };
        drop(releasing);
        drop(companion);
        self.owner.changed.notify_waiters();
    }
}

pub(crate) struct ProcessLock(PathBuf);

static PROCESS_OWNERS: std::sync::LazyLock<Mutex<std::collections::BTreeSet<PathBuf>>> =
    std::sync::LazyLock::new(|| Mutex::new(std::collections::BTreeSet::new()));

impl ProcessLock {
    /// 业务作用：以规范路径登记进程内唯一创建权，补充操作系统锁在不同文件系统下的进程语义。
    ///
    /// 参数说明：
    /// - `path`：规范父目录与末级文件名组成的精确路径。
    ///
    /// 返回：首次登记成功；同进程另一个 owner 在任何文件 I/O 前拒绝。
    pub(crate) fn acquire(path: &Path) -> anyhow::Result<Self> {
        anyhow::ensure!(
            PROCESS_OWNERS
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .insert(path.to_owned()),
            "SQLite path is already owned in this process"
        );
        Ok(Self(path.to_owned()))
    }
}

impl Drop for ProcessLock {
    /// 业务作用：仅在文件权威实际释放后撤销当前进程登记。
    ///
    /// 参数说明：无。
    ///
    /// 返回：允许之后的新 owner 重新进入跨进程锁竞争。
    fn drop(&mut self) {
        PROCESS_OWNERS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.0);
    }
}

/// 业务作用：拒绝链接别名和向其它账号开放的数据库材料。
///
/// 参数说明：
/// - `metadata`：通过不跟随末级符号链接取得的文件元数据。
///
/// 返回：普通、单链接、私有文件时成功。
pub(crate) fn validate_file(metadata: &std::fs::Metadata) -> anyhow::Result<()> {
    anyhow::ensure!(
        metadata.is_file() && metadata.nlink() == 1,
        "SQLite requires a regular file without link aliases"
    );
    anyhow::ensure!(
        metadata.uid() == unsafe { libc::geteuid() },
        "SQLite file belongs to another account"
    );
    anyhow::ensure!(
        metadata.mode() & 0o077 == 0,
        "SQLite files must have private permissions"
    );
    Ok(())
}

/// 业务作用：只创建本次缺失的目录并从创建时限制访问，不改写已有目录权限。
///
/// 参数说明：
/// - `path`：需要建立的专用目录。
///
/// 返回：路径可用时成功；权限或并发创建异常按原始 I/O 失败返回。
fn create_private_directories(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true).mode(0o700);
    builder.create(path)?;
    anyhow::ensure!(
        std::fs::metadata(path)?.permissions().mode() & 0o077 == 0,
        "SQLite directory permissions are not private"
    );
    Ok(())
}
