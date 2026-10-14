//! 受控 POSIX 本地目录的单机恢复后端，不承诺网络文件系统或跨主机接管。

use super::{
    increment, wire, CheckpointError, CheckpointLease, CheckpointResult, CheckpointStore,
    ReplayCheckpoint, ReplayPosition,
};
use rustix::fs::{FlockOperation, Mode, OFlags};
use std::{
    collections::HashSet,
    fs::{self, File},
    io::{Read, Write},
    os::unix::fs::DirBuilderExt,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{SystemTime, UNIX_EPOCH},
};

static OWNED: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);
const MAX_ACTIVE_TASKS: usize = 1024;

/// POSIX 本地文件的恢复存储；目录须由组件独占管理，阻塞磁盘调用由回放自己的有界执行资源承担。
#[derive(Clone)]
pub struct FileCheckpointStore {
    root: Arc<PathBuf>,
}

impl FileCheckpointStore {
    /// 业务作用：固定受控本地根目录，缺失时以当前用户独占权限创建。
    /// 参数说明：root 为本地任务存储根，不允许借此承诺共享网络文件系统语义。
    /// 返回：规范化目录的存储；目录不可用时拒绝，不开放任务推进。
    pub fn new(root: impl AsRef<Path>) -> CheckpointResult<Self> {
        let root = root.as_ref();
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(root)
            .map_err(|_| CheckpointError::Io)?;
        let root = root.canonicalize().map_err(|_| CheckpointError::Io)?;
        if !root.is_dir() {
            return Err(CheckpointError::InvalidConfiguration);
        }
        Ok(Self {
            root: Arc::new(root),
        })
    }

    /// 业务作用：按完整正文、原子替换、目录同步的顺序持久化进度。
    /// 参数说明：checkpoint 为下一不可变快照。
    /// 返回：同步完成后成功；失败均为未知写入，不允许用旧内存状态继续推进。
    fn persist(&self, checkpoint: &ReplayCheckpoint) -> CheckpointResult<()> {
        let key = wire::key(&checkpoint.task_id)?;
        let bytes = wire::encode(checkpoint)?;
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| CheckpointError::Io)?
            .as_nanos();
        let temporary = self.root.join(format!(
            "{key}.pending-{}-{stamp}-{sequence}",
            std::process::id()
        ));
        let target = self.root.join(format!("{key}.checkpoint"));
        let mut created = false;
        let result = (|| -> std::io::Result<()> {
            let mut output = File::from(rustix::fs::open(
                &temporary,
                OFlags::CREATE | OFlags::EXCL | OFlags::WRONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::RUSR | Mode::WUSR,
            )?);
            created = true;
            output.write_all(&bytes)?;
            output.sync_all()?;
            drop(output);
            // 先同步完整正文再原子替换并同步目录；任何一步未知都不能发布新的内存进度。
            fs::rename(&temporary, &target)?;
            File::open(self.root.as_ref())?.sync_all()?;
            Ok(())
        })();
        // 暂存文件没有成为 checkpoint 的权限，清理只针对本次精确路径。
        if created {
            let _ = fs::remove_file(&temporary);
        }
        result.map_err(|_| CheckpointError::OutcomeUnknown)
    }
}

impl CheckpointStore for FileCheckpointStore {
    /// 业务作用：只读加载完整进度，不触碰持有所有权的锁文件 descriptor。
    /// 参数说明：task_id 为稳定任务名。
    /// 返回：不存在为空；损坏、错误任务身份或未激活代次拒绝。
    fn load(&self, task_id: &str) -> CheckpointResult<Option<ReplayCheckpoint>> {
        let file = self
            .root
            .join(format!("{}.checkpoint", wire::key(task_id)?));
        let file = match rustix::fs::open(
            &file,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        ) {
            Ok(file) => File::from(file),
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(_) => return Err(CheckpointError::Io),
        };
        let mut bytes = Vec::new();
        file.take(wire::MAX_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| CheckpointError::Io)?;
        let checkpoint = wire::decode(&bytes)?;
        if checkpoint.task_id != task_id || checkpoint.generation == 0 {
            return Err(CheckpointError::Corrupt);
        }
        Ok(Some(checkpoint))
    }

    /// 业务作用：在独占任务且新代次已经落盘后交付 lease。
    /// 参数说明：initial 为尚未推进的冻结声明，恢复时完整核对原合同。
    /// 返回：单一活动所有者；进程崩溃释放锁后从既有进度接续，不重新计算移动边界。
    fn claim(&self, initial: &ReplayCheckpoint) -> CheckpointResult<Box<dyn CheckpointLease>> {
        initial.validate()?;
        if initial.generation != 0
            || initial.revision != 0
            || initial
                .positions
                .iter()
                .any(|position| position.next_offset != position.start_offset)
        {
            return Err(CheckpointError::InvalidConfiguration);
        }
        let lock_path = self
            .root
            .join(format!("{}.lock", wire::key(&initial.task_id)?));
        let task_lock = TaskLock::acquire(lock_path)?;
        let next = if let Some(previous) = self.load(&initial.task_id)? {
            if !initial.same_contract(&previous) {
                return Err(CheckpointError::ContractMismatch);
            }
            ReplayCheckpoint::new(
                previous.task_id,
                previous.contract_fingerprint,
                increment(previous.generation)?,
                increment(previous.revision)?,
                previous.positions,
            )?
        } else {
            ReplayCheckpoint::new(
                initial.task_id.clone(),
                initial.contract_fingerprint.clone(),
                1,
                0,
                initial.positions.clone(),
            )?
        };
        // 只有代次持久化成立才向调用方开放权限；任何失败都由 TaskLock 析构归还完整锁责任。
        self.persist(&next)?;
        Ok(Box::new(FileLease {
            store: self.clone(),
            task_lock: Some(task_lock),
            current: next,
            fenced: false,
        }))
    }
}

/// 同进程登记与 POSIX 锁构成一份不可分离的所有权责任。
struct TaskLock {
    path: PathBuf,
    file: Option<File>,
    acquired: bool,
    registered: bool,
}

impl TaskLock {
    /// 业务作用：先阻止同进程重复打开，再非阻塞取得整个锁文件的跨进程权限。
    /// 参数说明：path 为规范化根下的精确任务锁路径。
    /// 返回：唯一锁责任；现有所有者或活动任务容量耗尽时拒绝。
    fn acquire(path: PathBuf) -> CheckpointResult<Self> {
        {
            let mut owned = owned();
            // fcntl 锁属于整个进程，重复 descriptor 的关闭可能解除原锁，因此登记必须先于 open。
            if owned.contains(&path) {
                return Err(CheckpointError::Busy);
            }
            if owned.len() >= MAX_ACTIVE_TASKS {
                return Err(CheckpointError::Capacity);
            }
            owned.insert(path.clone());
        }
        let mut lock = Self {
            path,
            file: None,
            acquired: false,
            registered: true,
        };
        let file = rustix::fs::open(
            &lock.path,
            OFlags::CREATE | OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|_| CheckpointError::Io)?;
        lock.file = Some(File::from(file));
        match rustix::fs::fcntl_lock(
            lock.file.as_ref().expect("opened task lock"),
            FlockOperation::NonBlockingLockExclusive,
        ) {
            Ok(()) => {
                lock.acquired = true;
                Ok(lock)
            }
            Err(rustix::io::Errno::AGAIN | rustix::io::Errno::ACCESS) => Err(CheckpointError::Busy),
            Err(_) => Err(CheckpointError::Io),
        }
    }

    /// 业务作用：先释放原生锁并关闭 descriptor，再允许同进程新所有者登记。
    /// 参数说明：无。
    /// 返回：清理完成或固定 IO 分类；重复调用不影响后续已经取得的所有权。
    fn release(&mut self) -> CheckpointResult<()> {
        if !self.registered {
            return Ok(());
        }
        let Some(file) = self.file.take() else {
            owned().remove(&self.path);
            self.registered = false;
            return Ok(());
        };
        let result = if self.acquired {
            rustix::fs::fcntl_lock(&file, FlockOperation::Unlock).map_err(|_| CheckpointError::Io)
        } else {
            Ok(())
        };
        self.acquired = false;
        // 旧 descriptor 必须先退出，不能在新 claim 打开同 inode 后才触发进程级锁释放。
        drop(file);
        owned().remove(&self.path);
        self.registered = false;
        result
    }
}

impl Drop for TaskLock {
    /// 业务作用：在 claim 拒绝、任务退出或展开时归还锁责任。
    /// 参数说明：无。
    /// 返回：关闭 descriptor 后移除本进程登记，不产生新的持久化或业务副作用。
    fn drop(&mut self) {
        let _ = self.release();
    }
}

/// 实际 lease 独占可变进度；持久化未知后保持保护状态直至归还责任。
struct FileLease {
    store: FileCheckpointStore,
    task_lock: Option<TaskLock>,
    current: ReplayCheckpoint,
    fenced: bool,
}

impl CheckpointLease for FileLease {
    /// 业务作用：读取此 lease 最后确认的完整持久化事实。
    /// 参数说明：无。
    /// 返回：独立快照，不能通过修改结果改变内部推进权限。
    fn checkpoint(&self) -> ReplayCheckpoint {
        self.current.clone()
    }

    /// 业务作用：在新业务调用前复验本地独占责任与完整磁盘快照，失权后禁止旧代次继续写入。
    /// 参数说明：无。返回：独占责任和持久证据仍成立时成功；失败永久保护当前 lease。
    fn verify(&mut self) -> CheckpointResult<()> {
        if self.task_lock.is_none() {
            return Err(CheckpointError::Closed);
        }
        if self.fenced {
            return Err(CheckpointError::Fenced);
        }
        // 受控目录中的外部证据变化必须在下一次调用前拒绝，不能仅依赖 claim 时的历史快照。
        let result = self.store.load(&self.current.task_id).and_then(|stored| {
            if stored.as_ref() == Some(&self.current) {
                Ok(())
            } else {
                Err(CheckpointError::Fenced)
            }
        });
        if result.is_err() {
            self.fenced = true;
        }
        result
    }

    /// 业务作用：复验条件序号和磁盘证据后推进单调进度，禁止旧代次覆盖新责任。
    /// 参数说明：expected_revision 为已观察序号，positions 为原范围内的完整新进度。
    /// 返回：持久化成功后才更新内部快照；读写未知或证据改变后永久保护当前 lease。
    fn advance(
        &mut self,
        expected_revision: u64,
        positions: Vec<ReplayPosition>,
    ) -> CheckpointResult<ReplayCheckpoint> {
        if self.task_lock.is_none() {
            return Err(CheckpointError::Closed);
        }
        if self.fenced {
            return Err(CheckpointError::Fenced);
        }
        if expected_revision != self.current.revision {
            return Err(CheckpointError::StaleRevision);
        }
        let next = ReplayCheckpoint::new(
            self.current.task_id.clone(),
            self.current.contract_fingerprint.clone(),
            self.current.generation,
            increment(self.current.revision)?,
            positions,
        )?;
        if !self.current.same_contract(&next) {
            return Err(CheckpointError::ContractMismatch);
        }
        if self
            .current
            .positions
            .iter()
            .zip(&next.positions)
            .any(|(previous, next)| next.next_offset < previous.next_offset)
        {
            return Err(CheckpointError::InvalidConfiguration);
        }
        let result = (|| {
            // 业务处理期间仍可能发生证据变化，推进时必须再次复验当前权威。
            self.verify()?;
            self.store.persist(&next)?;
            Ok(())
        })();
        if let Err(error) = result {
            self.fenced = true;
            return Err(error);
        }
        self.current = next;
        Ok(self.current.clone())
    }

    /// 业务作用：永久关闭本 lease 后归还独占任务锁。
    /// 参数说明：无。
    /// 返回：资源实际退出或固定 IO 分类，不撤销已经完成的目标写入。
    fn close(&mut self) -> CheckpointResult<()> {
        match self.task_lock.take() {
            Some(mut lock) => lock.release(),
            None => Ok(()),
        }
    }
}

/// 业务作用：集中维护同一进程内所有任务的打开门禁，恢复锁中毒时仍保留已有登记。
/// 参数说明：无。
/// 返回：短时持有的登记守卫，不能跨磁盘写入或业务调用持有。
fn owned() -> std::sync::MutexGuard<'static, HashSet<PathBuf>> {
    OWNED
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
