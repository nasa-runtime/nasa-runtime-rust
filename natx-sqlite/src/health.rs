use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use crate::SqliteDataSource;

/// 关键不变量丢失后保持隔离，恢复需要重新建立资源，不以探针成功解除。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum IsolationReason {
    /// 数据库文件身份或持有权不再满足打开时的约束。
    FileIdentity = 1,
    /// 原生引擎确认数据库损坏或文件格式不符。
    Corrupt = 2,
    /// 连续 I/O 失败达到隔离条件。
    Io = 3,
    /// 物理连接 worker 异常退出，无法继续保证执行与关闭责任。
    Worker = 4,
    /// 连接策略或事务状态不再符合受管约束。
    Policy = 5,
}

/// 实际物理连接在发布前确认的引擎能力与策略。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionCapabilities {
    /// 实际链接的 SQLite 引擎版本文本。
    pub engine_version: String,
    /// 引擎允许的 SQL 绑定参数编号上限。
    pub variable_limit: i32,
    /// 连接实际生效的 synchronous 数值。
    pub synchronous: i32,
    /// 实际外键约束开关；发布来源前必须确认为开启。
    pub foreign_keys: bool,
    /// 实际脏读开关；受管连接必须关闭。
    pub read_uncommitted: bool,
    /// 实际 schema 信任开关；受管连接必须关闭。
    pub trusted_schema: bool,
    /// 实际 fullfsync 开关，与运行平台的连接策略核对。
    pub fullfsync: bool,
    /// 已安装的原生锁等待上限，单位为毫秒。
    pub busy_timeout_ms: u64,
    /// 启动时是否已通过 JSON 函数探测。
    pub json: bool,
    /// 启动时是否已通过 RETURNING 语法探测。
    pub returning: bool,
}

#[derive(Default)]
pub(crate) struct Health {
    isolated: AtomicU8,
    io_failures: AtomicU8,
    pub(crate) close_deadline: Mutex<Option<Instant>>,
    pub(crate) reads: Mutex<BTreeMap<u64, Instant>>,
    sequence: AtomicU64,
    pub(crate) wal_bytes: AtomicU64,
    pub(crate) available_bytes: AtomicU64,
    pub(crate) pressure: AtomicU8,
    pub(crate) connections: Mutex<Vec<std::sync::Weak<crate::native::NativeState>>>,
}

impl SqliteDataSource {
    /// 业务作用：向宿主报告不可自动解除的来源隔离原因。
    ///
    /// 参数说明：无。
    ///
    /// 返回：无隔离时为空，关键故障后保留首次原因直至资源重新创建。
    pub fn isolation_reason(&self) -> Option<IsolationReason> {
        match self.0.health.isolated.load(Ordering::Acquire) {
            1 => Some(IsolationReason::FileIdentity),
            2 => Some(IsolationReason::Corrupt),
            3 => Some(IsolationReason::Io),
            4 => Some(IsolationReason::Worker),
            5 => Some(IsolationReason::Policy),
            _ => None,
        }
    }

    /// 业务作用：提供启动时读取确认的原生能力，避免仅以配置推断生效状态。
    ///
    /// 参数说明：无。
    ///
    /// 返回：首条连接已确认、所有后续连接必须匹配的固定能力。
    pub fn capabilities(&self) -> &ConnectionCapabilities {
        self.0
            .capabilities
            .get()
            .expect("published SQLite capabilities")
    }

    /// 业务作用：关键故障立即关闭新业务与缓存入口，同时保留物理关闭责任。
    ///
    /// 参数说明：
    /// - `reason`：不可自动恢复的不变量失败原因。
    ///
    /// 返回：首次原因保持不变，已有原生操作通过回调感知隔离。
    pub(crate) fn isolate(&self, reason: IsolationReason) {
        // 首次原因固定隔离事实，后续探针成功不能恢复准入；恢复需要重新建立来源。
        let _ = self.0.health.isolated.compare_exchange(
            0,
            reason as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        // 先封口新业务并隔离缓存，避免故障来源继续接纳操作或提供旧缓存数据。
        self.0.accepting.store(false, Ordering::Release);
        self.quarantine_cache();
        // 撤下空闲 actor 触发关闭；在途及关闭中的真实连接仍承担物理容量与文件 owner 责任。
        self.0
            .idle
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
    }

    /// 业务作用：为每次实际文件访问复验权威，发现漂移即持续隔离整个来源。
    ///
    /// 参数说明：无。
    ///
    /// 返回：身份保持成立才成功，后续恢复原路径也不会自动恢复业务权。
    pub(crate) fn verify_file(&self) -> anyhow::Result<()> {
        if let Err(error) = self.0.file.verify() {
            self.isolate(IsolationReason::FileIdentity);
            return Err(error);
        }
        anyhow::ensure!(
            self.isolation_reason().is_none(),
            "SQLite datasource is isolated"
        );
        Ok(())
    }

    /// 业务作用：把物理故障转为统一来源隔离，普通锁竞争不撤销业务权威。
    ///
    /// 参数说明：
    /// - `result`：真实 worker 的执行结果。
    ///
    /// 返回：损坏立即隔离，连续三次 I/O 失败隔离，成功清除瞬时 I/O 计数。
    pub(crate) fn observe_result<T>(&self, result: &Result<T, sqlx::Error>) {
        self.observe_category(result.as_ref().err().map(crate::classify_sqlite_error));
    }

    /// 业务作用：仅把维护中确认属于源库的故障接入隔离和缓存门禁，目标与发布失败不撤销源库权威。
    ///
    /// 参数说明：
    /// - `result`：保留原生扩展码和归属的维护最终结果。
    ///
    /// 返回：源故障记录同一错误计数；成功重置瞬时 I/O 计数，目标失败不作为源恢复证据。
    pub(crate) fn observe_maintenance<T>(&self, result: &anyhow::Result<T>) {
        match result {
            Ok(_) => self.observe_category(None),
            Err(error) => {
                if let Some(error) = error.downcast_ref::<crate::MaintenanceError>() {
                    if error.origin == crate::MaintenanceErrorOrigin::Source {
                        self.observe_category(Some(crate::diagnostics::classify_code(error.code)));
                    }
                } else if let Some(error) = error.downcast_ref::<sqlx::Error>() {
                    self.observe_category(Some(crate::classify_sqlite_error(error)));
                }
            }
        }
    }

    /// 业务作用：将源库执行证据统一转换为稳定隔离状态和诊断计数。
    ///
    /// 参数说明：
    /// - `category`：失败类别；None 表示源操作确认成功。
    ///
    /// 返回：损坏立即隔离，连续三次 I/O 失败隔离，成功只清除瞬时计数。
    fn observe_category(&self, category: Option<crate::SqliteErrorCategory>) {
        use crate::SqliteErrorCategory;
        match category {
            None => {
                self.0.health.io_failures.store(0, Ordering::Release);
            }
            Some(category) => {
                self.0.diagnostics.record_category(category);
                match category {
                    SqliteErrorCategory::Corrupt => self.isolate(IsolationReason::Corrupt),
                    SqliteErrorCategory::Io
                        if self.0.health.io_failures.fetch_add(1, Ordering::AcqRel) >= 2 =>
                    {
                        self.isolate(IsolationReason::Io);
                    }
                    _ => {}
                }
            }
        }
    }

    /// 业务作用：执行前检查剩余排空时间与来源隔离，阻止超时事务继续业务 SQL。
    ///
    /// 参数说明：无。
    ///
    /// 返回：隔离或排空期限到达时为真，回滚与物理关闭另走清理权限。
    pub(crate) fn business_expired(&self) -> bool {
        self.isolation_reason().is_some()
            || self
                .0
                .health
                .close_deadline
                .lock()
                .map_or(true, |deadline| {
                    deadline.is_some_and(|deadline| Instant::now() >= deadline)
                })
    }

    /// 业务作用：在阻塞执行域采集磁盘压力并在写入前保留恢复空间。
    ///
    /// 参数说明：
    /// - `write`：本次是否可能修改数据库。
    ///
    /// 返回：读取始终保留诊断能力；写入在空间不足或 WAL 超限时拒绝，不删除日志。
    pub(crate) fn check_pressure(&self, write: bool) -> anyhow::Result<()> {
        use std::os::unix::ffi::OsStrExt;
        let mut wal = self.0.file.path.as_os_str().to_os_string();
        wal.push("-wal");
        let bytes = match std::fs::metadata(std::path::PathBuf::from(wal)) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        let path = std::ffi::CString::new(self.0.file.path.as_os_str().as_bytes())?;
        let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let stats = unsafe { stats.assume_init() };
        // 平台原生字段宽度不同，先在宽整数中计算，再按统一指标容量饱和收敛。
        let available = u64::try_from(u128::from(stats.f_bavail) * u128::from(stats.f_frsize))
            .unwrap_or(u64::MAX);
        self.0.health.wal_bytes.store(bytes, Ordering::Relaxed);
        self.0
            .health
            .available_bytes
            .store(available, Ordering::Relaxed);
        let pressure = if available < self.0.config.min_free_bytes {
            1
        } else if bytes >= self.0.config.max_wal_bytes {
            2
        } else {
            0
        };
        self.0.health.pressure.store(pressure, Ordering::Release);
        anyhow::ensure!(
            !write || pressure == 0,
            "SQLite write rejected by storage pressure"
        );
        Ok(())
    }

    /// 业务作用：登记持有连接的读取起点，用于长读诊断而不增加标签基数。
    ///
    /// 参数说明：无。
    ///
    /// 返回：仅供当前 lease 在归还时移除的唯一序号。
    pub(crate) fn track_read(&self) -> u64 {
        let id = self.0.health.sequence.fetch_add(1, Ordering::Relaxed);
        self.0
            .health
            .reads
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(id, Instant::now());
        id
    }
}
