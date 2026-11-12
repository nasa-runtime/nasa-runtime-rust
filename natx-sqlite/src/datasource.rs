use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::Semaphore;

use crate::executor::{Actor, Lease};
use crate::ownership::FileOwner;

/// SQLite 文件级与连接级策略；未知参数在任何文件操作前拒绝。
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SqliteOptions {
    /// 持久数据库的文件路径；不接受 URI 或内存数据库。
    pub path: PathBuf,
    /// 文件缺失时是否允许创建；只读来源必须关闭。
    pub create_if_missing: bool,
    /// 是否允许创建缺失的父目录；只读来源必须关闭。
    pub create_parent_directories: bool,
    /// 以只读权限打开，禁止写事务、迁移应用及写入型维护。
    pub read_only: bool,
    /// 声明的日志模式；已有文件的实际模式必须与之相符。
    pub journal_mode: JournalMode,
    /// 同步策略；未指定时根据日志模式选取受管默认值。
    pub synchronous: Option<Synchronous>,
    /// 是否启用外键约束；受管来源要求为 true。
    pub foreign_keys: bool,
    /// 原生锁竞争等待上限，单位为毫秒；操作总预算仍可提前终止等待。
    pub busy_timeout_ms: u64,
    /// WAL 自动 checkpoint 的页数阈值；仅可写 WAL 来源可显式配置。
    pub wal_autocheckpoint_pages: Option<u32>,
}

/// 数据库持久日志模式；已有文件必须与部署声明一致。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JournalMode {
    /// 使用 WAL；读者可持有提交快照，日志回收受读者生命周期约束。
    #[default]
    Wal,
    /// 使用 DELETE 回滚日志；提交结束后由引擎删除回滚日志。
    Delete,
}

/// 同步策略表达持久化边界，关闭同步不属于受管文件库合同。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Synchronous {
    /// 使用 SQLite NORMAL 同步策略。
    Normal,
    /// 使用 SQLite FULL 同步策略。
    Full,
    /// 使用 SQLite EXTRA 同步策略。
    Extra,
}

impl Default for SqliteOptions {
    /// 业务作用：给已有文件提供私有访问、外键与持久提交的默认策略。
    ///
    /// 参数说明：无。
    ///
    /// 返回：路径仍须显式填写，不默认创建数据库。
    fn default() -> Self {
        Self {
            path: PathBuf::new(),
            create_if_missing: false,
            create_parent_directories: false,
            read_only: false,
            journal_mode: JournalMode::Wal,
            synchronous: None,
            foreign_keys: true,
            busy_timeout_ms: 1000,
            wal_autocheckpoint_pages: None,
        }
    }
}

/// 每个来源的容量、单次执行和事务预算；未传播上游预算时仍保持有界。
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DataSourceConfig {
    /// 文件访问、日志与连接初始化策略。
    pub sqlite: SqliteOptions,
    /// 同时存活的物理连接上限，关闭中的连接仍占容量。
    pub max_connections: u32,
    /// 启动预热连接数；为零时仍建立首条连接以确认引擎与文件策略。
    pub min_connections: u32,
    /// 借用连接的默认等待上限，单位为毫秒。
    pub acquire_timeout_ms: u64,
    /// 启动连接建立与初始化的等待上限，单位为毫秒。
    pub connect_timeout_ms: u64,
    /// 单次数据库操作的默认总预算，单位为毫秒。
    pub operation_timeout_ms: u64,
    /// 最外层事务的默认存活预算，单位为毫秒。
    pub transaction_timeout_ms: u64,
    /// 流消费者未继续取行的最大间隔，单位为毫秒；防止长期占用连接与读快照。
    pub stream_idle_timeout_ms: u64,
    /// 单次结果允许的累计行数上限，超过即停止交付。
    pub max_result_rows: usize,
    /// 单次结果允许的累计数据字节上限，防止无界结果占用内存。
    pub max_result_bytes: usize,
    /// 写入准入要求的最小可用磁盘字节数。
    pub min_free_bytes: u64,
    /// WAL 字节压力阈值，超过时限制新写入。
    pub max_wal_bytes: u64,
}

impl Default for DataSourceConfig {
    /// 业务作用：限制文件库的物理线程、结果内存和无上游预算调用的存活时间。
    ///
    /// 参数说明：无。
    ///
    /// 返回：四连接上限和有限执行预算，路径仍由业务指定。
    fn default() -> Self {
        Self {
            sqlite: SqliteOptions::default(),
            max_connections: 4,
            min_connections: 1,
            acquire_timeout_ms: 3000,
            connect_timeout_ms: 3000,
            operation_timeout_ms: 30_000,
            transaction_timeout_ms: 30_000,
            stream_idle_timeout_ms: 10_000,
            max_result_rows: 10_000,
            max_result_bytes: 16 * 1024 * 1024,
            min_free_bytes: 64 * 1024 * 1024,
            max_wal_bytes: 256 * 1024 * 1024,
        }
    }
}

impl DataSourceConfig {
    /// 业务作用：在创建目录和取得文件锁前拒绝不支持的部署与无界参数。
    ///
    /// 参数说明：无。
    ///
    /// 返回：全部配置一致且有界时成功；不产生 I/O。
    pub fn validate(&self) -> anyhow::Result<()> {
        let path = self.sqlite.path.to_string_lossy();
        anyhow::ensure!(
            !path.is_empty()
                && !path.contains('\0')
                && path != ":memory:"
                && !path.starts_with("file:")
                && !path.starts_with("sqlite:"),
            "SQLite requires a file path, not a URI or memory database"
        );
        anyhow::ensure!(
            (1..=64).contains(&self.max_connections)
                && self.min_connections <= self.max_connections,
            "invalid SQLite connection limits"
        );
        for value in [
            self.acquire_timeout_ms,
            self.connect_timeout_ms,
            self.operation_timeout_ms,
            self.transaction_timeout_ms,
            self.stream_idle_timeout_ms,
        ] {
            anyhow::ensure!(
                (1..=3_600_000).contains(&value),
                "SQLite timeouts must be finite and at most one hour"
            );
        }
        anyhow::ensure!(
            (1..=i32::MAX as u64).contains(&self.sqlite.busy_timeout_ms),
            "invalid SQLite busy timeout"
        );
        anyhow::ensure!(self.sqlite.foreign_keys, "SQLite foreign_keys must be true");
        anyhow::ensure!(
            (1..=1_000_000).contains(&self.max_result_rows)
                && (1024..=256 * 1024 * 1024).contains(&self.max_result_bytes),
            "invalid SQLite result limits"
        );
        if self.sqlite.read_only {
            anyhow::ensure!(
                !self.sqlite.create_if_missing && !self.sqlite.create_parent_directories,
                "read-only SQLite cannot create files or directories"
            );
        }
        anyhow::ensure!(
            self.max_wal_bytes >= 4096,
            "SQLite WAL pressure limit must be at least one page"
        );
        if let Some(pages) = self.sqlite.wal_autocheckpoint_pages {
            anyhow::ensure!(
                pages > 0
                    && pages <= i32::MAX as u32
                    && !self.sqlite.read_only
                    && self.sqlite.journal_mode == JournalMode::Wal,
                "wal_autocheckpoint_pages requires a writable WAL database"
            );
        }
        Ok(())
    }
}

pub(crate) struct Source {
    pub(crate) health: crate::health::Health,
    pub(crate) capabilities: std::sync::OnceLock<crate::ConnectionCapabilities>,
    pub(crate) diagnostics: crate::diagnostics::Diagnostics,
    pub(crate) name: natx_core::DatasourceRef,
    pub(crate) registry: std::sync::OnceLock<std::sync::Weak<crate::SqliteDataSourceRegistry>>,
    pub(crate) file: Arc<FileOwner>,
    pub(crate) config: DataSourceConfig,
    pub(crate) accepting: AtomicBool,
    pub(crate) draining: AtomicBool,
    close: tokio::sync::Mutex<()>,
    pub(crate) physical: Arc<Semaphore>,
    pub(crate) borrowers: Arc<Semaphore>,
    pub(crate) idle: Mutex<Vec<Actor>>,
    pub(crate) maintenance: Arc<tokio::sync::Mutex<()>>,
    pub(crate) cache_safe: AtomicBool,
    pub(crate) cache_namespace: String,
    pub(crate) cache_epoch: std::sync::atomic::AtomicU64,
}

/// 可共享的数据源使用句柄；不向业务授予整源关闭或文件替换权限。
#[derive(Clone)]
pub struct SqliteDataSource(pub(crate) Arc<Source>);

/// 数据源唯一关闭责任；Application 保留该对象，getter 只返回使用句柄。
pub struct SqliteDataSourceOwner {
    source: SqliteDataSource,
}

impl SqliteDataSourceOwner {
    /// 业务作用：取得共享使用句柄而不转移关闭责任。
    ///
    /// 参数说明：无。
    ///
    /// 返回：指向同一个文件与连接池的轻量句柄。
    pub fn datasource(&self) -> SqliteDataSource {
        self.source.clone()
    }

    /// 业务作用：关闭新根调用并等待全部物理 worker 退出，保持超时后的文件权威。
    ///
    /// 参数说明：
    /// - `deadline`：宿主给予关闭过程的绝对截止点。
    ///
    /// 返回：实际连接全部关闭才成功；超时保持关闭态和未结束连接的 owner。
    pub async fn close(&self, deadline: tokio::time::Instant) -> anyhow::Result<()> {
        let _serial = tokio::time::timeout_at(deadline, self.source.0.close.lock()).await?;
        if self.source.0.file.live.load(Ordering::Acquire) == 0 {
            self.source.stop_accepting();
            return Ok(());
        }
        {
            let mut current = self
                .source
                .0
                .health
                .close_deadline
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            *current = Some(current.map_or(deadline.into_std(), |previous| {
                previous.min(deadline.into_std())
            }));
        }
        // 先撤销新根操作，已接纳事务按原连接排空；清理 guard 保证取消关闭等待也不会重新开放路由。
        self.source.0.accepting.store(false, Ordering::Release);
        self.source.0.draining.store(true, Ordering::Release);
        let cleanup = CloseGuard(&self.source);
        let drained = tokio::time::timeout_at(
            deadline,
            self.source
                .0
                .borrowers
                .clone()
                .acquire_many_owned(self.source.0.config.max_connections),
        )
        .await;
        let mut maintenance_result = Ok(());
        if let Ok(Ok(mut permit)) = drained {
            while tokio::time::Instant::now() < deadline {
                let actor = self
                    .source
                    .0
                    .idle
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .pop();
                let Some(actor) = actor else {
                    break;
                };
                let lease = match Lease::new(self.source.clone(), actor, permit, deadline) {
                    Ok(lease) => Arc::new(lease),
                    Err(returned) => {
                        // 排空后的候选仍可能已经决定关闭；继续选择健康连接，不把关闭竞争当成 checkpoint 失败。
                        permit = returned;
                        continue;
                    }
                };
                if !self.source.is_read_only()
                    && self.source.0.config.sqlite.journal_mode == JournalMode::Wal
                    && self.source.isolation_reason().is_none()
                {
                    maintenance_result = async {
                        let gate = tokio::time::timeout_at(
                            deadline,
                            self.source.0.maintenance.clone().lock_owned(),
                        )
                        .await?;
                        lease
                            .maintain(
                                crate::maintenance::Operation::Checkpoint(
                                    crate::CheckpointMode::Passive,
                                ),
                                gate,
                                deadline,
                            )
                            .await?;
                        Ok(())
                    }
                    .await;
                }
                break;
            }
        }
        drop(cleanup);
        loop {
            let changed = self.source.0.file.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.source.0.file.live.load(Ordering::Acquire) == 0 {
                return maintenance_result;
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|_| anyhow::anyhow!("SQLite physical close is incomplete"))?;
        }
    }
}

struct CloseGuard<'a>(&'a SqliteDataSource);

impl Drop for CloseGuard<'_> {
    /// 业务作用：关闭流程取消或失败时继续移交物理清理，不能恢复新业务接纳。
    ///
    /// 参数说明：无。
    ///
    /// 返回：清理空闲连接，活动连接继续保活原 owner。
    fn drop(&mut self) {
        self.0.stop_accepting();
    }
}

impl Drop for SqliteDataSourceOwner {
    /// 业务作用：拥有者离开时撤销新业务准入，真实连接继续保活文件锁直到物理退出。
    ///
    /// 参数说明：无。
    ///
    /// 返回：空闲连接收到关闭，活动责任不会被提前计为已结束。
    fn drop(&mut self) {
        self.source.stop_accepting();
    }
}

impl SqliteDataSource {
    /// 业务作用：让同时管理外部对象文件的宿主核对元数据来源与业务数据根的一致性。
    ///
    /// 参数说明：无。
    ///
    /// 返回：创建 owner 时固定、父目录已解析的绝对数据库路径；调用方不得据此绕过来源直接读写活动数据库。
    pub fn database_path(&self) -> &std::path::Path {
        &self.0.file.path
    }

    /// 业务作用：将外部对象目录的独占权绑定到同一文件 owner，取消异步调用也不能提前释放。
    ///
    /// 参数说明：
    /// - `guard`：宿主持有的独占权；不得直接或间接引用本 datasource，以免形成所有权环。
    ///
    /// 返回：仅首次绑定成功；关闭开始后拒绝，原生连接全部退出并释放文件权威后解除保活。
    pub fn retain_file_guard<T: Send + Sync + 'static>(&self, guard: Arc<T>) -> anyhow::Result<()> {
        self.0.file.retain_companion(guard)
    }

    /// 业务作用：向宿主暴露冻结的借用等待预算，用于观测阈值校验。
    ///
    /// 参数说明：无。
    ///
    /// 返回：创建数据源时确定的有限毫秒数。
    pub fn acquire_timeout_ms(&self) -> u64 {
        self.0.config.acquire_timeout_ms
    }

    /// 业务作用：读取同源已提交写入的本地序号，防止写后继续命中旧查询结果。
    ///
    /// 参数说明：无。
    ///
    /// 返回：当前进程内单调递增的提交序号。
    pub fn cache_epoch(&self) -> u64 {
        self.0.cache_epoch.load(Ordering::Acquire)
    }

    /// 业务作用：读取当前 owner 专用缓存前缀，防止同名来源跨生命周期复用缓存。
    ///
    /// 参数说明：无。
    ///
    /// 返回：不含路径、仅在该来源生命周期内稳定的身份。
    pub fn cache_namespace(&self) -> &str {
        &self.0.cache_namespace
    }

    /// 业务作用：判断共享缓存是否仍具有可信数据库写入边界。
    ///
    /// 参数说明：无。
    ///
    /// 返回：没有不确定写入或缓存清理失败时为真。
    pub fn cache_safe(&self) -> bool {
        self.0.cache_safe.load(Ordering::Acquire)
    }

    /// 业务作用：缓存清理或提交结果不确定时隔离当前 owner 的共享缓存。
    ///
    /// 参数说明：无。
    ///
    /// 返回：来源余下生命周期不再读写共享缓存。
    pub fn quarantine_cache(&self) {
        self.0.cache_safe.store(false, Ordering::Release);
    }

    /// 业务作用：读取稳定 datasource 名称，用于宏、事务和观测对齐。
    ///
    /// 参数说明：无。
    ///
    /// 返回：不含文件路径的已校验名称。
    pub fn name(&self) -> &str {
        self.0.name.as_str()
    }

    /// 业务作用：确认两个句柄是否具有完全相同的文件与生命周期权威。
    ///
    /// 参数说明：
    /// - `other`：待比较的来源句柄。
    ///
    /// 返回：同一 source 时为真，不以相同名字代替身份。
    pub fn same_source(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// 业务作用：读取数据源是否只允许数据库读取。
    ///
    /// 参数说明：无。
    ///
    /// 返回：冻结的只读策略。
    pub fn is_read_only(&self) -> bool {
        self.0.config.sqlite.read_only
    }

    /// 业务作用：撤销业务准入并转入关闭清理，保留回滚与原生收口责任。
    ///
    /// 参数说明：无。
    ///
    /// 返回：来源停止业务借用与建连，不再提供业务排空窗口；文件权威仅在真实连接全部退出后释放。
    pub(crate) fn stop_accepting(&self) {
        // 先撤销新根调用和排空标志，避免 owner 离开或关闭清理开始后仍把连接回池用于业务。
        self.0.accepting.store(false, Ordering::Release);
        self.0.draining.store(false, Ordering::Release);
        // 关闭容量入口并唤醒等待方，使已有连接的业务门禁失效；回滚和原生关闭仍保留清理许可。
        self.0.borrowers.close();
        self.0.physical.close();
        // 先登记退出后解锁，再撤下空闲 actor；仍在执行或关闭的真实连接继续保活文件 owner。
        self.0.file.release_when_idle();
        self.0
            .idle
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
    }

    /// 业务作用：从有界池借用同源物理连接，不在文件丢失后创建替代库。
    ///
    /// 参数说明：
    /// - `deadline`：调用链固定的操作、事务或维护截止点，局部 acquire 等待不能覆盖它。
    ///
    /// 返回：完成原生期限交接的拥有型借用责任；局部等待到期或来源关闭时失败。
    pub(crate) async fn acquire(
        &self,
        deadline: tokio::time::Instant,
    ) -> anyhow::Result<Arc<Lease>> {
        use natx_core::observability::{connection_slot, AcquireOutcome, ConnectionPurpose};
        let mut waiting = connection_slot(crate::DatabaseDriver::Sqlite, self.name())
            .acquire(ConnectionPurpose::Direct);
        let result = async {
            anyhow::ensure!(
                self.0.accepting.load(Ordering::Acquire),
                "SQLite datasource is stopped"
            );
            let acquire_deadline = deadline.min(
                tokio::time::Instant::now()
                    + Duration::from_millis(self.0.config.acquire_timeout_ms),
            );
            let mut permit =
                tokio::time::timeout_at(acquire_deadline, self.0.borrowers.clone().acquire_owned())
                    .await??;
            loop {
                // 只限制选池及建连等待，不把局部 acquire 期限交给已接纳调用，也不在换候选时续期。
                if tokio::time::Instant::now() >= acquire_deadline {
                    anyhow::bail!(sqlx::Error::PoolTimedOut);
                }
                anyhow::ensure!(
                    self.0.accepting.load(Ordering::Acquire) && !self.business_expired(),
                    "SQLite datasource is stopped or isolated"
                );
                let actor = {
                    let mut idle = self
                        .0
                        .idle
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    loop {
                        // 失效候选仍由原生关闭责任保活；先查完已有空闲连接，不能让慢关闭遮挡健康候选。
                        let Some(actor) = idle.pop() else {
                            break None;
                        };
                        if actor.usable() {
                            break Some(actor);
                        }
                    }
                };
                let actor = match actor {
                    Some(actor) => actor,
                    None => crate::executor::connect_actor(self, false, acquire_deadline).await?,
                };
                // 连接打开期间可能进入停机；尚未接纳的根调用不能借完成建连绕过关闭门。
                anyhow::ensure!(
                    self.0.accepting.load(Ordering::Acquire),
                    "SQLite datasource stopped during acquire"
                );
                if tokio::time::Instant::now() >= acquire_deadline {
                    anyhow::bail!(sqlx::Error::PoolTimedOut);
                }
                match Lease::new(self.clone(), actor, permit, deadline) {
                    Ok(lease) => return Ok(Arc::new(lease)),
                    Err(returned) => {
                        // worker 已先决定关闭时不再接纳该候选；保留同一逻辑容量及原等待期限选择其它连接。
                        permit = returned;
                    }
                }
            }
        }
        .await;
        let outcome = match &result {
            Ok(_) => AcquireOutcome::Ok,
            Err(_) if !self.0.accepting.load(Ordering::Acquire) => AcquireOutcome::Closed,
            Err(error)
                if error
                    .downcast_ref::<tokio::time::error::Elapsed>()
                    .is_some()
                    || matches!(
                        error.downcast_ref::<sqlx::Error>(),
                        Some(sqlx::Error::PoolTimedOut)
                    ) =>
            {
                AcquireOutcome::Timeout
            }
            Err(_) => AcquireOutcome::Other,
        };
        waiting.finish(outcome);
        result
    }
}

/// 业务作用：建立 standalone 文件数据源及唯一关闭 owner，不自动安装全局名称。
///
/// 参数说明：
/// - `name`：稳定 datasource 名称。
/// - `config`：已经冻结的文件、连接与预算配置。
///
/// 返回：完成文件身份、原生能力和连接策略校验后返回 owner；失败保留真实磁盘事实。
pub async fn open_owned(
    name: &str,
    config: DataSourceConfig,
) -> anyhow::Result<SqliteDataSourceOwner> {
    config.validate()?;
    let name = natx_core::DatasourceRef::new(name)?;
    let options = config.sqlite.clone();
    let (file, created) = tokio::task::spawn_blocking(move || {
        FileOwner::acquire(
            &options.path,
            options.create_if_missing,
            options.create_parent_directories,
            options.read_only,
        )
    })
    .await??;
    static GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let cache_namespace = format!(
        "{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos(),
        GENERATION.fetch_add(1, Ordering::Relaxed)
    );
    let source = SqliteDataSource(Arc::new(Source {
        health: crate::health::Health::default(),
        capabilities: std::sync::OnceLock::new(),
        diagnostics: crate::diagnostics::Diagnostics::default(),
        name,
        registry: std::sync::OnceLock::new(),
        file,
        physical: Arc::new(Semaphore::new(config.max_connections as usize)),
        borrowers: Arc::new(Semaphore::new(config.max_connections as usize)),
        config,
        accepting: AtomicBool::new(true),
        draining: AtomicBool::new(false),
        close: tokio::sync::Mutex::new(()),
        idle: Mutex::new(Vec::new()),
        maintenance: Arc::new(tokio::sync::Mutex::new(())),
        cache_safe: AtomicBool::new(true),
        cache_namespace,
        cache_epoch: std::sync::atomic::AtomicU64::new(0),
    }));
    let owner = SqliteDataSourceOwner {
        source,
    };
    let deadline = tokio::time::Instant::now()
        + Duration::from_millis(owner.source.0.config.connect_timeout_ms);
    let first = crate::executor::connect_actor(&owner.source, created, deadline).await?;
    owner
        .source
        .0
        .idle
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .push(first);
    for _ in 1..owner.source.0.config.min_connections {
        let connection = crate::executor::connect_actor(&owner.source, false, deadline).await?;
        owner
            .source
            .0
            .idle
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(connection);
    }
    natx_core::observability::register_datasource(
        natx_core::DatabaseDriver::Sqlite,
        owner.source.name(),
    )?;
    Ok(owner)
}
