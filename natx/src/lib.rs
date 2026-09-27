//! 基于 task-local 的 ambient 事务运行时。
//!
//! 业务方法可在事务上下文中自动复用同一条数据库连接，支持默认数据源和命名数据源。
//! Pool acquire、事务连接槽和执行前拒绝分别计量；Mapper、迁移、探测与直接访问使用固定用途。
//! 受管 YAML 冻结等待日志、超时通知和 SQLx 语句选项；通知只做非阻塞入队，不影响连接与事务结果。
// ============================================================================
// src/tx.rs —— 基于 task_local 的【ambient(环境态)事务】运行时
//
// 目标:业务方法贴 #[transactional],方法体里嵌套调用的 repo【自动用同一个事务连接】,
//   不用手动把 &mut Transaction 一层层传下去(ambient / 环境态事务)。
//
// ── 范围声明(务必读):这是一个极简 ambient 事务便利层,不是通用事务框架 ──
//   支持:无参 #[transactional] / #[transactional(datasource = "...")] / async fn /
//        返回 anyhow::Result<T> / 默认 MySqlPool + 命名 datasource pool /
//        嵌套时复用相同 datasource 的外层事务(无外层则新建)/
//        嵌套 Err 触发 rollback-only 整体回滚。
//   【不支持】(宏对任何参数直接 compile_error,不会静默忽略):独立子事务、savepoint 部分回滚、
//        隔离级别、只读、超时、按错误类型区分是否回滚、同一事务跨 datasource 等。
//        需要时按真实业务场景单独提案,勿当作"已实现"使用。
//   安全性依赖调用方纪律:① 要进事务的 SQL 必须走 natx::conn()/natx::mandatory_conn(),用 &self.pool 会绕过;
//        ② 事务内 tokio::spawn 出的 task 不继承事务(写入会 autocommit、不随回滚);
//        ③ 不可在持有一个 Conn 时再取 Conn；重复取得会立即返回连接占用错误。
//
// ── 推荐用法 ──
//   · 启动 main:natx::try_init(pool)?(fail-fast;旧 natx::init 仅兼容、重复初始化只打日志)。
//   · 必须参与事务的关键写 repo:natx::mandatory_conn()(取不到事务即 Err,不静默 autocommit)。
//   · 可独立(无事务)运行的 repo:natx::conn()(有事务用事务、无则用池)。
//   · 复杂事务 / 流式边读边写:用显式 &mut Transaction,让借用检查器在编译期挡住冲突。
//
// 思路(就是"task_local 里放当前事务;repo 取得到就用它、取不到就用 pool"):
//   · run()  : begin 一个事务,放进 task_local 作用域里跑业务,正常 commit / 出错 rollback。
//   · conn() : repo 调它取"当前连接"——task_local 里有事务就返事务连接,没有就从 pool 取一个。
//
// ── 为什么不能简单地把 Transaction 直接塞 task_local ──
//   task_local 的 .with() 只给【共享引用 &T】,而 sqlx 执行 query 要【&mut Transaction】(独占)。
//   所以要内部可变性。又因为 query 的 future【借着 &mut 跨 .await】:
//     · RefCell 不行:它的 RefMut 跨 await 是 !Send → axum handler(要求 Send future)编译不过,且会重入 panic。
//     · 必须用 tokio::sync::Mutex:它的 guard 是 Send,能安全跨 await。
//   还要:
//     · Transaction<'static>:task_local 值要 'static —— pool.begin() 恰好返回 Transaction<'static>(持有从池取走的连接)。
//     · Option<..>:commit(self) 要【按值消费】事务,而我们只有锁里的 &mut → 用 Option::take() 把它取出来提交。
//     · Arc<..>:run() 提交时还要再拿到这个事务(scope 不返还值)→ 用 Arc 共享一个句柄。
//   综上,task_local 存的是:Arc<tokio::sync::Mutex<Option<Transaction<'static, MySql>>>>。
// ============================================================================

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, RwLock, Weak};

use sqlx::{MySql, MySqlConnection, Transaction};

pub mod datasource;
pub use natx_core::observability;

pub use natx_core::{
    redact_url, redacted_endpoint, BootstrapKind, CommitOutcome, DataSourceCatalog,
    DataSourceLookupError, DataSourcePoolConfig, DatabaseCapabilities, DatabaseDriver,
    DatasourceNameError, DatasourceRef, ManagedInstallationToken, ManagedRegistryOwner,
    PoolConfigError, RegistryCoordinationError, RegistryModeSnapshot, TxDecision, TxEntryError,
    TxRollbackCause, TxRunError, DEFAULT_DATASOURCE, MAX_DATASOURCE_NAME_BYTES,
    MAX_MANAGED_DATASOURCES,
};

/// 重导出底层连接池类型：Application 与业务的公开 getter 需要能命名它，
/// 且必须与本 crate 使用的 sqlx 依赖严格同源，否则不同依赖实例会得到两个互不相同的类型。
pub use sqlx::MySqlPool;
use tokio::sync::{Mutex, OwnedMutexGuard};

/// 当前 crate 实际编入的数据库后端能力。
pub const DATABASE_CAPABILITIES: DatabaseCapabilities =
    DatabaseCapabilities::empty().with(DatabaseDriver::MySql);

/// 当前事务的"槽"类型。
///
/// `Arc` 用于在 task_local 上下文和提交阶段共享同一个事务句柄，`tokio::Mutex`
/// 用于让 sqlx 的 `&mut Transaction` 可以安全跨 `.await`，`Option` 用于最外层
/// `run_for` 在 commit/rollback 前把事务按值取出。
type TxSlot = Arc<Mutex<Option<Transaction<'static, MySql>>>>;

/// 最外层事务提交成功后执行的异步副作用。
///
/// 典型用途是 Mapper 缓存失效、提交后消息通知等“不能早于 commit 执行”的动作。
type AfterCommitHook = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// 当前 ambient 事务上下文。
///
/// 它把事务槽、datasource、rollback-only 标记和提交后 hook 绑定在一起。嵌套
/// `#[transactional]` 的 body 返回 `Err` 时会置位 rollback-only；最外层提交前检查该标记，
/// 即使外层吞掉错误并返回 `Ok`，也会整体 rollback，避免脏提交。
struct TxContext {
    /// 当前事务连接槽。
    tx: TxSlot,
    /// 当前事务所属 datasource。
    datasource: DatasourceRef,
    /// 嵌套层失败后置位，要求最外层整体回滚。
    rollback_only: AtomicBool,
    // 记录首个置位原因(内层 Err 的 Display),最外层报错时给线索。std Mutex:临界区不跨 await。
    rollback_reason: std::sync::Mutex<Option<String>>,
    // 最外层 commit 成功后执行的异步 hook。用于缓存失效、消息通知等“提交后副作用”。
    after_commit: std::sync::Mutex<Vec<AfterCommitHook>>,
}

/// task-local 中实际保存的事务上下文引用。
type TxCtx = Arc<TxContext>;

// ── ambient 事务句柄:沿【调用栈】向下传播(scope 包住的那段 future 都能 try_with 取到)──
//   注意:它是【任务内沿调用栈】传的,不是线程本地——async 跨线程也跟着走(tokio task_local 的语义)。
tokio::task_local! {
    static CUR_TX: TxCtx;
}

// ── 全局连接池(不在事务里时,conn() 从这里取连接;run() 从这里 begin)──
//   独立程序可在启动时注入一次；Application 受管模式发布冻结 registry 并在停机时撤销。
static POOL: OnceLock<StdMutex<Option<MySqlPool>>> = OnceLock::new();
static DATASOURCE_POOLS: OnceLock<StdMutex<HashMap<String, MySqlPool>>> = OnceLock::new();
static MANAGED_DATASOURCES: OnceLock<RwLock<Weak<DataSourceRegistry>>> = OnceLock::new();

/// 业务作用：作为单个 Application 的冻结 datasource 表，统一事务、Mapper 与受管持久适配器的 pool 身份。
pub struct DataSourceRegistry {
    pools: BTreeMap<DatasourceRef, MySqlPool>,
    accepting: AtomicBool,
    owner: OnceLock<ManagedRegistryOwner>,
    catalog: OnceLock<Arc<DataSourceCatalog>>,
}

impl DataSourceRegistry {
    /// 业务作用：从启动期已建立的 pool 一次性构造冻结命名表。
    ///
    /// 参数说明：`pools` 为 qualifier 与已受管连接池的集合。
    ///
    /// 返回：名称全部合法、不重复且数量有界时返回 registry；失败时不发布部分表。
    pub fn try_new(pools: impl IntoIterator<Item = (String, MySqlPool)>) -> anyhow::Result<Self> {
        let mut registry = BTreeMap::new();
        for (name, pool) in pools {
            if registry.len() >= MAX_MANAGED_DATASOURCES {
                anyhow::bail!(
                    "managed datasource count exceeds the supported limit of {MAX_MANAGED_DATASOURCES}"
                );
            }
            let reference = DatasourceRef::new(name)?;
            if registry.insert(reference.clone(), pool).is_some() {
                anyhow::bail!("datasource `{reference}` is configured more than once");
            }
        }
        anyhow::ensure!(
            !registry.is_empty(),
            "managed datasource registry cannot be empty"
        );
        for name in registry.keys() {
            observability::register_datasource(DatabaseDriver::MySql, name.as_str())?;
        }
        Ok(Self {
            pools: registry,
            accepting: AtomicBool::new(true),
            owner: OnceLock::new(),
            catalog: OnceLock::new(),
        })
    }

    /// 业务作用：为 Application 构造绑定 owner、尚未对 getter 开放的 MySQL registry。
    ///
    /// 参数说明：
    /// - `pools`：启动期已经建立完成的 MySQL pool 集合。
    /// - `token`：本次 Application 从 `natx-core` 领取的安装 token。
    ///
    /// 返回：名称合法且 owner 唯一时返回关闭态 registry；失败时不发布任何全局状态。
    pub fn try_new_managed(
        pools: impl IntoIterator<Item = (String, MySqlPool)>,
        token: &ManagedInstallationToken,
    ) -> anyhow::Result<Self> {
        let registry = Self::try_new(pools)?;
        registry.accepting.store(false, Ordering::Release);
        registry
            .owner
            .set(token.owner().clone())
            .map_err(|_| anyhow::anyhow!("managed registry owner is already bound"))?;
        Ok(registry)
    }

    /// 业务作用：在 registry 尚未停机时选择指定连接池。
    ///
    /// 参数说明：`datasource` 是已校验或配置边界传入的 qualifier。
    ///
    /// 返回：运行期内命中时返回同一 pool 的 clone；停机或名称缺失时失败且不回退默认库。
    pub fn pool(&self, datasource: &str) -> anyhow::Result<MySqlPool> {
        if !self.accepting.load(Ordering::Acquire) {
            anyhow::bail!("managed datasource registry is closing");
        }
        let reference = DatasourceRef::new(datasource)?;
        if self.owner.get().is_some() {
            let expected = self
                .catalog
                .get()
                .ok_or_else(|| anyhow::anyhow!("managed datasource catalog is not bound"))?;
            let active = natx_core::managed_catalog().map_err(anyhow::Error::new)?;
            anyhow::ensure!(
                Arc::ptr_eq(expected, &active)
                    && active
                        .owner()
                        .ptr_eq(self.owner.get().expect("owner checked above")),
                "managed datasource catalog identity does not match this registry"
            );
        }
        self.pools.get(&reference).cloned().ok_or_else(|| {
            anyhow::anyhow!("datasource `{reference}` is not managed by this application")
        })
    }

    /// 业务作用：确认命名 datasource 是否属于该冻结表。
    ///
    /// 参数说明：`datasource` 是待复验的 qualifier。
    ///
    /// 返回：registry 尚开放且名称存在时返回真。
    pub fn contains(&self, datasource: &str) -> bool {
        if !self.accepting.load(Ordering::Acquire) {
            return false;
        }
        if self.owner.get().is_some()
            && !self.catalog.get().is_some_and(|expected| {
                natx_core::managed_catalog()
                    .ok()
                    .is_some_and(|active| Arc::ptr_eq(expected, &active))
            })
        {
            return false;
        }
        DatasourceRef::new(datasource)
            .ok()
            .is_some_and(|reference| self.pools.contains_key(&reference))
    }

    /// 业务作用：在连接池关闭前封口新的事务与连接选择。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；已借出的 pool 仍由 Application 停机预算排空。
    pub fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    /// 业务作用：在 catalog 完成同 owner 发布后开放本 typed registry。
    ///
    /// 参数说明：`token` 必须属于构造该 registry 的 Application。
    ///
    /// 返回：owner 匹配时开放连接选择；错配时保持关闭。
    pub fn start_accepting(&self, token: &ManagedInstallationToken) -> anyhow::Result<()> {
        let owner = self
            .owner
            .get()
            .ok_or_else(|| anyhow::anyhow!("managed registry owner is not bound"))?;
        anyhow::ensure!(
            owner.ptr_eq(token.owner()),
            "managed registry owner does not match installation token"
        );
        let catalog = self
            .catalog
            .get()
            .ok_or_else(|| anyhow::anyhow!("managed datasource catalog is not bound"))?;
        anyhow::ensure!(
            catalog.owner().ptr_eq(token.owner()),
            "managed datasource catalog owner does not match installation token"
        );
        self.accepting.store(true, Ordering::Release);
        Ok(())
    }

    /// 业务作用：读取 registry 绑定的 Application owner，供按身份撤销全局槽。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已安装或以 managed 构造时返回 owner；普通未安装 registry 返回 `None`。
    pub fn managed_owner(&self) -> Option<&ManagedRegistryOwner> {
        self.owner.get()
    }

    /// 业务作用：生成该 MySQL registry 对应的 catalog 条目。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按 datasource 名排序且 driver 固定为 MySQL 的拥有型条目。
    pub fn catalog_entries(&self) -> Vec<(String, DatabaseDriver)> {
        self.pools
            .keys()
            .map(|reference| (reference.as_str().to_owned(), DatabaseDriver::MySql))
            .collect()
    }

    /// 业务作用：让持有同 owner token 的 Application staging 读取关闭态 pool 以登记资源和执行门禁。
    ///
    /// 参数说明：
    /// - `datasource`：待读取的 MySQL datasource。
    /// - `token`：当前 Application 安装 token。
    ///
    /// 返回：owner 与名称都匹配时返回 pool clone；该入口不开放普通业务 getter。
    #[doc(hidden)]
    pub fn managed_pool(
        &self,
        datasource: &str,
        token: &ManagedInstallationToken,
    ) -> anyhow::Result<MySqlPool> {
        let owner = self
            .owner
            .get()
            .ok_or_else(|| anyhow::anyhow!("managed registry owner is not bound"))?;
        anyhow::ensure!(
            owner.ptr_eq(token.owner()),
            "managed registry owner does not match installation token"
        );
        let reference = DatasourceRef::new(datasource)?;
        self.pools
            .get(&reference)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("datasource `{reference}` is not in this registry"))
    }

    /// 业务作用：把 MySQL typed registry 绑定到唯一 Application owner，阻止跨实例复用资源表。
    ///
    /// 参数说明：`owner` 是构造 catalog 与其它 typed registry 共同使用的身份。
    ///
    /// 返回：首次绑定或同身份重复绑定成功；另一 owner 已占用时保持原身份并失败。
    fn bind_owner(&self, owner: &ManagedRegistryOwner) -> anyhow::Result<()> {
        if let Some(existing) = self.owner.get() {
            anyhow::ensure!(
                existing.ptr_eq(owner),
                "managed registry is already bound to another owner"
            );
            return Ok(());
        }
        self.owner
            .set(owner.clone())
            .map_err(|_| anyhow::anyhow!("managed registry owner binding raced"))
    }

    /// 业务作用：把 MySQL typed registry 绑定到同 owner 的共享 catalog，供开放前后复验全局权威。
    ///
    /// 参数说明：
    /// - `catalog`：跨 driver 完整 datasource catalog。
    /// - `token`：当前 Application 安装 token。
    ///
    /// 返回：owner 和 Arc 身份一致时成功；错配时不替换已有 catalog。
    fn bind_catalog(
        &self,
        catalog: &Arc<DataSourceCatalog>,
        token: &ManagedInstallationToken,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            catalog.owner().ptr_eq(token.owner()),
            "managed catalog owner does not match installation token"
        );
        if let Some(existing) = self.catalog.get() {
            anyhow::ensure!(
                Arc::ptr_eq(existing, catalog),
                "managed registry is already bound to another catalog"
            );
            return Ok(());
        }
        self.catalog
            .set(catalog.clone())
            .map_err(|_| anyhow::anyhow!("managed catalog binding raced"))
    }
}

/// 事务被标记 rollback-only 时,最外层 `run()` 返回的具名错误(经 `anyhow::Error` 携带)。
/// 上层可 `err.downcast_ref::<natx::RollbackOnly>()` 区分它与普通业务错误。
/// 触发场景:某嵌套 `#[transactional]` 的 body 返回了 `Err`,但被外层吞掉/转成 `Ok` —— 整体仍回滚。
#[derive(Debug)]
pub struct RollbackOnly {
    /// 首个置位 rollback-only 的内层错误的 Display 文本(线索)。
    pub reason: String,
}

impl std::fmt::Display for RollbackOnly {
    /// 业务作用：输出 rollback-only 的稳定诊断文本，供错误链、日志和调试展示。
    ///
    /// 参数说明：
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    ///
    /// 返回：文本成功写入 formatter 返回 `Ok`；写入失败返回格式化错误。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "事务被标记 rollback-only(某嵌套 #[transactional] 返回了 Err,但被外层吞掉):{}",
            self.reason
        )
    }
}

impl std::error::Error for RollbackOnly {}

impl RollbackOnly {
    /// 稳定错误码:供跨服务日志检索 / 上层分类,避免解析 Display 文本(文案可能变,code 不变)。
    pub const CODE: &'static str = "TX_ROLLBACK_ONLY";
}

/// 业务作用：注入默认连接池，建立 ambient transaction 与普通连接的唯一数据源入口。
///
/// main 启动时调一次。重复调用【不会替换】已有 pool —— 此时记 error 日志使其可见,
/// 而非静默忽略(避免"以为换了 pool 实际没生效"的隐蔽问题)。需要启动期 fail-fast 用 [`try_init`]。
///
/// 参数说明：
/// - `pool`: 应用启动时创建好的 MySQL 连接池,作为后续事务和普通连接获取的全局来源。
///
/// 返回：无；重复初始化保留原 pool 并记录错误。
pub fn init(pool: MySqlPool) {
    if let Err(e) = try_init(pool) {
        tracing::error!(error = %e, "natx::init 重复调用,本次注入被忽略(pool 不会被替换)");
    }
}

/// 业务作用：以 fail-fast 方式注入默认连接池，防止启动期误以为连接源已被替换。
///
/// **重复初始化返回 Err**，启动期推荐用它而不是只打日志。
///
/// 参数说明：
/// - `pool`: 应用启动时创建好的 MySQL 连接池，作为独立模式的默认连接源。
///
/// 返回：当前无独立池且无受管 registry 时成功；重复或模式冲突时不替换已有 pool。
pub fn try_init(pool: MySqlPool) -> anyhow::Result<()> {
    natx_core::coordinate(|coordinator| {
        coordinator
            .register_standalone(DatabaseDriver::MySql, DEFAULT_DATASOURCE)
            .map_err(anyhow::Error::new)?;
        let mut current = POOL
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        anyhow::ensure!(
            current.is_none(),
            "natx::init/try_init 重复调用:连接池已初始化,不能替换"
        );
        observability::register_datasource(DatabaseDriver::MySql, DEFAULT_DATASOURCE)?;
        *current = Some(pool);
        Ok(())
    })
}

/// 业务作用：注入命名 datasource，使显式多库业务能绑定到稳定且不可替换的连接源。
///
/// `default` 等价于 [`try_init`]。
///
/// 命名 datasource 主要给 Mapper 和明确多库业务使用；它不会改变现有无参
/// `#[transactional]` / [`conn`] 的默认库语义。
///
/// 参数说明：
/// - `name`: datasource 名称；`"default"` 表示默认库，其它名称必须非空且无首尾空白。
/// - `pool`: 该 datasource 对应的 MySQL 连接池。
///
/// 返回：首次注册返回 `Ok`；名称非法或重复注册返回错误，已有 pool 保持不变。
pub fn try_init_datasource(name: impl Into<String>, pool: MySqlPool) -> anyhow::Result<()> {
    let name = name.into();
    validate_datasource_name(&name)?;
    if name == DEFAULT_DATASOURCE {
        return try_init(pool);
    }
    natx_core::coordinate(|coordinator| {
        coordinator
            .register_standalone(DatabaseDriver::MySql, &name)
            .map_err(anyhow::Error::new)?;
        let pools = DATASOURCE_POOLS.get_or_init(|| StdMutex::new(HashMap::new()));
        let mut pools = pools.lock().unwrap_or_else(|error| error.into_inner());
        if pools.contains_key(&name) {
            return Err(anyhow::anyhow!(
                "natx::try_init_datasource 重复调用:datasource `{name}` 已初始化,不能替换"
            ));
        }
        observability::register_datasource(DatabaseDriver::MySql, &name)?;
        pools.insert(name, pool);
        Ok(())
    })
}

/// 业务作用：以日志可见但不中断调用方的方式注入命名 datasource。
///
/// 参数说明：
/// - `name`: datasource 名称；`"default"` 表示默认库。
/// - `pool`: 该 datasource 对应的 MySQL 连接池。
///
/// 返回：无；名称非法或重复注册时保留原 pool 并记录错误。
pub fn init_datasource(name: impl Into<String>, pool: MySqlPool) {
    let name = name.into();
    if let Err(e) = try_init_datasource(name.clone(), pool) {
        tracing::error!(datasource = %name, error = %e, "natx::init_datasource 重复调用,本次注入被忽略(pool 不会被替换)");
    }
}

/// 业务作用：校验 datasource 名称可安全作为连接池注册键和诊断字段。
///
/// 参数说明：
/// - `name`: 待校验的 datasource 名称。
///
/// 返回：非空且无首尾空白返回 `Ok`；否则返回错误。
fn validate_datasource_name(name: &str) -> anyhow::Result<()> {
    natx_core::validate_datasource_name(name).map_err(|error| match error {
        natx_core::DatasourceNameError::Empty => anyhow::anyhow!("datasource 名称不能为空"),
        natx_core::DatasourceNameError::SurroundingWhitespace => {
            anyhow::anyhow!("datasource 名称首尾不能包含空白")
        }
        natx_core::DatasourceNameError::InvalidCharacters => anyhow::anyhow!(
            "datasource 名称只能包含 ASCII 字母、数字、'.'、'_'和'-'，且不超过 {MAX_DATASOURCE_NAME_BYTES} 字节"
        ),
    })
}

/// 业务作用：发布当前 Application 拥有的唯一 datasource registry。
///
/// 参数说明：`registry` 是 DB 组件在全部数据源建立成功后冻结的命名表。
///
/// 返回：当前进程没有独立注册表且没有其它 live Application 时成功；冲突时保留原权威并失败。
pub fn install_managed_registry(registry: &Arc<DataSourceRegistry>) -> anyhow::Result<()> {
    let owner = registry
        .managed_owner()
        .cloned()
        .unwrap_or_else(ManagedRegistryOwner::new);
    registry.bind_owner(&owner)?;
    let catalog = match registry.catalog.get() {
        Some(catalog) => catalog.clone(),
        None => {
            let catalog = Arc::new(DataSourceCatalog::try_new(
                owner.clone(),
                registry.catalog_entries(),
            )?);
            registry
                .catalog
                .set(catalog.clone())
                .map_err(|_| anyhow::anyhow!("managed catalog binding raced"))?;
            catalog
        }
    };

    natx_core::coordinate(|coordinator| {
        ensure_local_standalone_empty()?;
        let token = coordinator
            .begin_bootstrap(&owner, BootstrapKind::Configured)
            .map_err(anyhow::Error::new)?;
        if let Err(error) = install_managed_slot_locked(registry) {
            let _ = coordinator.abort_bootstrap(&token);
            return Err(error);
        }
        registry.bind_catalog(&catalog, &token)?;
        if let Err(error) = coordinator.install_catalog(&token, &catalog) {
            clear_managed_slot_locked(registry);
            let _ = coordinator.abort_bootstrap(&token);
            return Err(anyhow::Error::new(error));
        }
        if let Err(error) = registry.start_accepting(&token) {
            clear_managed_slot_locked(registry);
            let _ = coordinator.clear_managed(&owner);
            return Err(error);
        }
        if let Err(error) = coordinator.open_catalog(&token, &catalog) {
            registry.stop_accepting();
            clear_managed_slot_locked(registry);
            let _ = coordinator.clear_managed(&owner);
            return Err(anyhow::Error::new(error));
        }
        Ok(())
    })
}

/// 业务作用：为 napp 领取受管 datasource owner 与安装 token，但不创建或开放连接池。
///
/// 参数说明：`kind` 区分配置建池与 UserHook 延后 default。
///
/// 返回：进程入口为空时进入 Bootstrapping；已有 standalone 或 managed 权威时失败。
#[doc(hidden)]
pub fn begin_managed_bootstrap(
    kind: BootstrapKind,
) -> anyhow::Result<(ManagedRegistryOwner, ManagedInstallationToken)> {
    natx_core::begin_managed_bootstrap(kind).map_err(anyhow::Error::new)
}

/// 业务作用：把关闭态 MySQL registry 安装到当前 owner 的 typed 槽，但不开放全局 getter。
///
/// 参数说明：
/// - `registry`：由 [`DataSourceRegistry::try_new_managed`] 构造的关闭态表。
/// - `token`：当前 Application 安装 token。
///
/// 返回：owner 匹配、无 standalone 且 typed 槽空闲时成功；失败不改变原权威。
#[doc(hidden)]
pub fn install_closed_managed_registry(
    registry: &Arc<DataSourceRegistry>,
    token: &ManagedInstallationToken,
) -> anyhow::Result<()> {
    registry.bind_owner(token.owner())?;
    registry.stop_accepting();
    natx_core::coordinate(|coordinator| {
        coordinator
            .verify_installation_token(token)
            .map_err(anyhow::Error::new)?;
        ensure_local_standalone_empty()?;
        install_managed_slot_locked(registry)
    })
}

/// 业务作用：把共享 catalog 绑定到已安装的 MySQL registry，保证资源持有同一 owner。
///
/// 参数说明：
/// - `registry`：当前 Application 的 MySQL typed registry。
/// - `catalog`：跨 driver 完整 catalog。
/// - `token`：当前 Application 安装 token。
///
/// 返回：三者 owner 一致时成功；错配时 registry 保持关闭。
#[doc(hidden)]
pub fn attach_managed_catalog(
    registry: &Arc<DataSourceRegistry>,
    catalog: &Arc<DataSourceCatalog>,
    token: &ManagedInstallationToken,
) -> anyhow::Result<()> {
    registry.bind_catalog(catalog, token)
}

/// 业务作用：在共享 catalog 即将开放前开放 MySQL typed registry。
///
/// 参数说明：
/// - `registry`：已经绑定共享 catalog 的 registry。
/// - `token`：当前 Application 安装 token。
///
/// 返回：owner 匹配时开放；错配时保持关闭。
#[doc(hidden)]
pub fn open_managed_registry(
    registry: &Arc<DataSourceRegistry>,
    token: &ManagedInstallationToken,
) -> anyhow::Result<()> {
    registry.start_accepting(token)
}

/// 业务作用：在 catalog 封口后按 registry Arc 身份撤销 MySQL typed 槽。
///
/// 参数说明：`registry` 是正在停机或启动失败清理的资源表。
///
/// 返回：无；旧实例不命中当前槽时不修改新实例。
#[doc(hidden)]
pub fn clear_managed_registry_slot(registry: &Arc<DataSourceRegistry>) {
    natx_core::coordinate(|_| {
        registry.stop_accepting();
        clear_managed_slot_locked(registry);
    });
}

/// 业务作用：把 UserHook 以独立入口注入的默认连接池原子转交给 Application registry。
///
/// 参数说明: 无。
///
/// 返回：仅存在一个独立默认池、无命名独立池且无其它受管 registry 时完成所有权转移；
/// 条件不成立时保留原有入口并失败。
pub fn adopt_standalone_default_registry() -> anyhow::Result<Arc<DataSourceRegistry>> {
    natx_core::coordinate(|coordinator| {
        anyhow::ensure!(
            !local_named_standalone_present(),
            "named standalone datasources cannot be adopted by the default-only managed registry"
        );
        let owner = ManagedRegistryOwner::new();
        let token = coordinator
            .begin_legacy_adopt(&owner, DatabaseDriver::MySql)
            .map_err(anyhow::Error::new)?;
        let pool = {
            let mut slot = POOL
                .get_or_init(|| StdMutex::new(None))
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            match slot.take() {
                Some(pool) => pool,
                None => {
                    // 本地 pool 与 core 模式不一致时立即恢复 standalone，避免一次失败永久占住引导态。
                    let _ = coordinator.restore_legacy_standalone(&token, DatabaseDriver::MySql);
                    return Err(anyhow::anyhow!("datasource `default` 未初始化"));
                }
            }
        };
        let registry = Arc::new(DataSourceRegistry::try_new([(
            DEFAULT_DATASOURCE.to_owned(),
            pool.clone(),
        )])?);
        registry.bind_owner(&owner)?;
        let catalog = Arc::new(DataSourceCatalog::try_new(
            owner.clone(),
            registry.catalog_entries(),
        )?);
        registry.bind_catalog(&catalog, &token)?;

        let publish = (|| -> anyhow::Result<()> {
            install_managed_slot_locked(&registry)?;
            coordinator
                .install_catalog(&token, &catalog)
                .map_err(anyhow::Error::new)?;
            registry.start_accepting(&token)?;
            coordinator
                .open_catalog(&token, &catalog)
                .map_err(anyhow::Error::new)?;
            Ok(())
        })();
        if let Err(error) = publish {
            registry.stop_accepting();
            clear_managed_slot_locked(&registry);
            if coordinator.clear_managed(&owner).is_ok() {
                let _ = coordinator.register_standalone(DatabaseDriver::MySql, DEFAULT_DATASOURCE);
            } else {
                let _ = coordinator.restore_legacy_standalone(&token, DatabaseDriver::MySql);
            }
            *POOL
                .get_or_init(|| StdMutex::new(None))
                .lock()
                .unwrap_or_else(|lock_error| lock_error.into_inner()) = Some(pool);
            return Err(error);
        }
        Ok(registry)
    })
}

/// 业务作用：把 UserHook 安装的 MySQL default 转入同 owner 的关闭态 registry。
///
/// 参数说明：`token` 是 Start 阶段领取的 DeferredDefault 安装 token。
///
/// 返回：只存在 MySQL default 且 owner/driver 匹配时返回关闭态 registry；失败保留 standalone。
#[doc(hidden)]
pub fn adopt_standalone_default_registry_closed(
    token: &ManagedInstallationToken,
) -> anyhow::Result<Arc<DataSourceRegistry>> {
    natx_core::coordinate(|coordinator| {
        coordinator
            .verify_deferred_adopt(token, DatabaseDriver::MySql)
            .map_err(anyhow::Error::new)?;
        anyhow::ensure!(
            !local_named_standalone_present(),
            "named standalone datasources cannot be adopted by the default-only managed registry"
        );
        let pool = {
            let mut slot = POOL
                .get_or_init(|| StdMutex::new(None))
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            slot.take()
                .ok_or_else(|| anyhow::anyhow!("datasource `default` 未初始化"))?
        };
        let registry = match DataSourceRegistry::try_new_managed(
            [(DEFAULT_DATASOURCE.to_owned(), pool.clone())],
            token,
        ) {
            Ok(registry) => Arc::new(registry),
            Err(error) => {
                *POOL
                    .get_or_init(|| StdMutex::new(None))
                    .lock()
                    .unwrap_or_else(|lock_error| lock_error.into_inner()) = Some(pool);
                return Err(error);
            }
        };
        if let Err(error) = install_managed_slot_locked(&registry) {
            *POOL
                .get_or_init(|| StdMutex::new(None))
                .lock()
                .unwrap_or_else(|lock_error| lock_error.into_inner()) = Some(pool);
            return Err(error);
        }
        Ok(registry)
    })
}

/// 业务作用：在动态数据库 UserHook 开放前确认独立注册表不含历史资源。
///
/// 参数说明: 无。
///
/// 返回：无 standalone pool 且无 live 受管 registry 时成功；否则拒绝开放会混淆所有权的引导窗口。
#[doc(hidden)]
pub fn ensure_standalone_datasources_empty_for_managed_bootstrap() -> anyhow::Result<()> {
    natx_core::coordinate(|coordinator| {
        anyhow::ensure!(
            matches!(coordinator.snapshot(), RegistryModeSnapshot::Empty),
            "standalone datasource registry must be empty before managed user-hook bootstrap"
        );
        ensure_local_standalone_empty()
    })
}

/// 业务作用：在动态 UserHook 未完成接管就失败时，取回本轮引导窗口注入的全部池。
///
/// 参数说明: 无。
///
/// 返回：无 live 受管 registry 时返回已从全局入口撤销的有序 pool 列表；已存在受管权威时拒绝取回。
#[doc(hidden)]
pub fn take_standalone_datasources_for_managed_shutdown() -> anyhow::Result<Vec<(String, MySqlPool)>>
{
    natx_core::coordinate(|coordinator| {
        anyhow::ensure!(
            matches!(
                coordinator.snapshot(),
                RegistryModeSnapshot::Empty
                    | RegistryModeSnapshot::Standalone(DatabaseDriver::MySql)
            ),
            "managed datasource bootstrap requires owner-aware cleanup"
        );
        let pools = take_local_standalone_pools();
        coordinator
            .release_standalone(DatabaseDriver::MySql)
            .map_err(anyhow::Error::new)?;
        Ok(pools)
    })
}

/// 业务作用：在 DeferredDefault 启动失败时按 owner token 取回 MySQL standalone pool。
///
/// 参数说明：`token` 是本轮 Application 安装 token。
///
/// 返回：owner 与 deferred driver 匹配时撤销并返回有序 pool；错配时不触碰全局入口。
#[doc(hidden)]
pub fn take_deferred_standalone_for_managed_shutdown(
    token: &ManagedInstallationToken,
) -> anyhow::Result<Vec<(String, MySqlPool)>> {
    natx_core::coordinate(|coordinator| {
        coordinator
            .verify_deferred_adopt(token, DatabaseDriver::MySql)
            .map_err(anyhow::Error::new)?;
        let pools = take_local_standalone_pools();
        coordinator
            .clear_deferred_driver(token, DatabaseDriver::MySql)
            .map_err(anyhow::Error::new)?;
        Ok(pools)
    })
}

/// 业务作用：关闭态 adopt 后若后续 staging 失败，清除 core 中已接管的 MySQL deferred driver 记录。
///
/// 参数说明：`token` 是本轮 DeferredDefault owner token；调用方必须先撤销 typed 槽并关闭 pool。
///
/// 返回：owner 与 driver 匹配时清除记录；错配时保留模式，防止越权释放。
#[doc(hidden)]
pub fn clear_adopted_deferred_driver(token: &ManagedInstallationToken) -> anyhow::Result<()> {
    natx_core::coordinate(|coordinator| {
        coordinator
            .clear_deferred_driver(token, DatabaseDriver::MySql)
            .map_err(anyhow::Error::new)
    })
}

/// 业务作用：在全局数据源协调锁内发布唯一受管 registry。
///
/// 参数说明：`registry` 是已冻结且由 Application 持有的资源表。
///
/// 返回：独立入口为空且无其它 live registry 时成功；冲突时不改变全局槽。
fn install_managed_slot_locked(registry: &Arc<DataSourceRegistry>) -> anyhow::Result<()> {
    let slot = MANAGED_DATASOURCES.get_or_init(|| RwLock::new(Weak::new()));
    let mut current = slot.write().unwrap_or_else(|error| error.into_inner());
    if current.upgrade().is_some() {
        anyhow::bail!("another managed datasource registry is already active");
    }
    *current = Arc::downgrade(registry);
    Ok(())
}

/// 业务作用：按 Arc 身份清除 MySQL typed registry 弱槽，防止旧实例撤销新实例。
///
/// 参数说明：`registry` 是正在失败清理或停机的 registry。
///
/// 返回：无；槽为空或身份不匹配时保持当前状态。
fn clear_managed_slot_locked(registry: &Arc<DataSourceRegistry>) {
    let Some(slot) = MANAGED_DATASOURCES.get() else {
        return;
    };
    let mut current = slot.write().unwrap_or_else(|error| error.into_inner());
    if current
        .upgrade()
        .is_some_and(|active| Arc::ptr_eq(&active, registry))
    {
        *current = Weak::new();
    }
}

/// 业务作用：仅在全局槽仍指向目标 Application registry 时封口并撤销发布。
///
/// 参数说明：`registry` 是正在进入逆序停机的资源表。
///
/// 返回：无；指针复验防止旧 Application 误清理后续实例的槽位。
pub fn clear_managed_registry(registry: &Arc<DataSourceRegistry>) {
    natx_core::coordinate(|coordinator| {
        registry.stop_accepting();
        clear_managed_slot_locked(registry);
        if let Some(owner) = registry.managed_owner() {
            let _ = coordinator.clear_managed(owner);
        }
    });
}

/// 业务作用：确认 MySQL 本地 standalone 表为空，避免与受管 registry 形成两张资源表。
///
/// 参数说明: 无。
///
/// 返回：没有 live 受管 registry 时成功；已进入受管模式时失败。
fn ensure_local_standalone_empty() -> anyhow::Result<()> {
    anyhow::ensure!(
        !POOL.get().is_some_and(|pool| {
            pool.lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_some()
        }) && !local_named_standalone_present(),
        "standalone datasource registry is already initialized and cannot be mixed with managed mode"
    );
    Ok(())
}

/// 业务作用：检查 MySQL 命名 standalone 表是否仍持有任何 pool。
///
/// 参数说明: 无。
///
/// 返回：至少存在一个命名 pool 时返回真；未初始化或空表返回假。
fn local_named_standalone_present() -> bool {
    DATASOURCE_POOLS.get().is_some_and(|pools| {
        !pools
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_empty()
    })
}

/// 业务作用：原子取走 MySQL 本地 default 与命名 standalone pool，供失败路径按序关闭。
///
/// 参数说明: 无。
///
/// 返回：按 datasource 名排序的拥有型 pool 列表；调用后本地 standalone 表为空。
fn take_local_standalone_pools() -> Vec<(String, MySqlPool)> {
    let mut pools = Vec::new();
    if let Some(pool) = POOL.get().and_then(|pool| {
        pool.lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
    }) {
        pools.push((DEFAULT_DATASOURCE.to_owned(), pool));
    }
    if let Some(named) = DATASOURCE_POOLS.get() {
        pools.extend(std::mem::take(
            &mut *named.lock().unwrap_or_else(|error| error.into_inner()),
        ));
    }
    pools.sort_by(|left, right| left.0.cmp(&right.0));
    pools
}

/// 业务作用：解析指定 datasource 的连接池，统一默认库与命名库的查找失败语义。
///
/// 参数说明：
/// - `datasource`: 业务声明的 datasource 名称。
///
/// 返回：已初始化的连接池 clone；名称非法或未初始化返回错误。
fn pool_for(datasource: &str) -> anyhow::Result<MySqlPool> {
    natx_core::ensure_driver(DatabaseDriver::MySql).map_err(anyhow::Error::new)?;
    match pool_for_checked(datasource) {
        Ok(pool) => Ok(pool),
        // 兼容入口继续保留 1.x 的未初始化文本；新增 checked 入口仍提供结构化 NotFound。
        Err(DataSourceLookupError::NotFound { datasource }) => {
            Err(anyhow::anyhow!("datasource `{datasource}` 未初始化"))
        }
        Err(error) => Err(anyhow::Error::new(error)),
    }
}

/// 业务作用：解析 MySQL pool 并保留 datasource 查找的结构化失败，供加法 checked 事务入口使用。
///
/// 参数说明：`datasource` 是业务声明的 datasource 名称。
///
/// 返回：命中 typed pool 时返回 clone；名称、driver、生命周期或缺失状态返回封闭 lookup 分类。
fn pool_for_checked(datasource: &str) -> Result<MySqlPool, DataSourceLookupError> {
    let reference = natx_core::resolve_datasource(datasource, DatabaseDriver::MySql)?;
    if let Some(registry) = MANAGED_DATASOURCES.get().and_then(|slot| {
        slot.read()
            .unwrap_or_else(|error| error.into_inner())
            .upgrade()
    }) {
        return registry
            .pool(reference.as_str())
            .map_err(|_| DataSourceLookupError::RegistryUnavailable);
    }
    if reference.as_str() == DEFAULT_DATASOURCE {
        return POOL
            .get()
            .and_then(|pool| {
                pool.lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone()
            })
            .ok_or(DataSourceLookupError::NotFound {
                datasource: reference,
            });
    }
    let pools = DATASOURCE_POOLS
        .get()
        .ok_or_else(|| DataSourceLookupError::NotFound {
            datasource: reference.clone(),
        })?;
    let pools = pools.lock().unwrap_or_else(|error| error.into_inner());
    pools
        .get(reference.as_str())
        .cloned()
        .ok_or(DataSourceLookupError::NotFound {
            datasource: reference,
        })
}

/// 业务作用：向无事务长生命周期执行器提供指定 datasource 的独立连接池入口。
///
/// 这是给无事务、长生命周期执行器使用的只读入口，例如声明式 Mapper 的 stream/cursor
/// 查询。它不会加入 ambient 事务；需要事务语义的普通 SQL 仍必须走 [`conn_for`]。
///
/// 参数说明：
/// - `datasource`: 业务声明的 datasource 名称。
///
/// 返回：已初始化的连接池 clone；名称非法或未初始化返回错误。
pub fn pool_for_datasource(datasource: &str) -> anyhow::Result<MySqlPool> {
    pool_for(datasource)
}

/// 业务作用：检测当前 task 是否持有 ambient transaction，防止 spawn 后关键写静默降级为 autocommit。
///
/// 用途:事务内 `tokio::spawn` 出的 task【不继承】当前事务(task_local 不跨 spawn 传播),
/// 其 `natx::conn()` 会 fallback 到 pool、写入绕过事务且不随回滚撤销。业务可在 spawn 前用本函数自检。
///
/// 参数说明: 无。
///
/// 返回：当前 task 位于事务 scope 内返回真，否则返回假。
pub fn in_transaction() -> bool {
    CUR_TX.try_with(|_| ()).is_ok()
}

/// 业务作用：读取当前事务绑定的 datasource，供关键写与诊断复验连接归属。
///
/// 参数说明: 无。
///
/// 返回：事务内返回 datasource 名称；无 ambient transaction 返回 `None`。
pub fn current_datasource() -> Option<DatasourceRef> {
    CUR_TX.try_with(|ctx| ctx.datasource.clone()).ok()
}

// ════════════════════════════════════════════════════════════════════════════
// run —— #[transactional] 生成的代码会调它(begin / 提交 / 回滚 / 传播)
// ════════════════════════════════════════════════════════════════════════════
/// 业务作用：以兼容语义执行默认 datasource 事务，`Ok` 提交、`Err` 整体回滚。
///
/// 传播规则:若【外层已在事务中】(CUR_TX 已设),则直接复用、不另开事务(由最外层统一提交)。
///
/// 参数说明：
/// - `body`: 需要在默认 datasource 事务内执行的业务 future。
///
/// 返回：提交确认返回业务值；业务错误、rollback-only 或数据库失败返回错误。
pub async fn run<T, F>(body: F) -> anyhow::Result<T>
where
    F: std::future::Future<Output = anyhow::Result<T>>,
{
    run_for(DEFAULT_DATASOURCE, body).await
}

/// 业务作用：拒绝 ambient 事务并以非事务方式执行业务体，`#[transactional(never)]` 的运行时入口。
///
/// 有些路径**绝不能被外层事务包住**:自治审计写(外层回滚不得连带撤销)、对外即时可见的副作用、
/// 以及依赖"语句各自提交"的维护操作。ambient 传播默认"复用外层事务",这类路径被事务内调用时
/// 会静默入伙并随外层回滚——本入口把该违规变成进入前的显式错误。
///
/// 参数说明：
/// - `body`: 以非事务连接执行的业务 future(内部 `conn()`/`conn_for` 走 pool 直连,语句各自提交)。
///
/// 返回：无 ambient 事务时返回业务结果；检测到 ambient 事务立即返回错误且不执行业务体。
pub async fn run_never<T, F>(body: F) -> anyhow::Result<T>
where
    F: std::future::Future<Output = anyhow::Result<T>>,
{
    if let Some(driver) = natx_core::current_driver() {
        // `never` 拒绝全部数据库 driver 的环境事务；只检查 MySQL task-local 会让 PostgreSQL
        // 外层事务中的自治副作用继续执行，破坏调用方声明的立即可见与不随外层回滚语义。
        anyhow::bail!(
            "transactional(never) 拒绝在 ambient 事务内执行(driver={driver});\
             该函数的副作用不允许随外层事务回滚,请在事务外调用"
        );
    }
    body.await
}

/// 业务作用：按业务代码给出的显式裁决，在默认 datasource 中提交或回滚本地事务。
///
/// 参数说明：
/// - `body`: 返回 [`TxDecision::Commit`] 或 [`TxDecision::Rollback`] 的业务 Future。
///
/// 返回：最外层 `Commit` 且数据库确认提交时返回领域结果；嵌套 `Commit` 只加入外层事务；
/// `Rollback`、rollback-only、COMMIT 不确定或物理回滚失败返回对应 [`TxRunError`]。
pub async fn run_decided<T, E, F>(body: F) -> Result<T, TxRunError<E>>
where
    F: Future<Output = TxDecision<T, E>>,
{
    run_decided_for(DEFAULT_DATASOURCE, body).await
}

/// 业务作用：在默认 MySQL datasource 执行显式裁决，并保留 datasource 查找的结构化错误。
///
/// 参数说明：`body` 返回提交或回滚裁决。
///
/// 返回：查找失败经 [`TxEntryError::Lookup`] 返回；事务开始后的失败经 [`TxEntryError::Run`] 返回。
pub async fn run_decided_checked<T, E, F>(body: F) -> Result<T, TxEntryError<E>>
where
    F: Future<Output = TxDecision<T, E>>,
{
    run_decided_for_checked(DEFAULT_DATASOURCE, body).await
}

/// 业务作用：在指定 MySQL datasource 执行显式裁决，并在连接前暴露名称、状态与 driver 错配。
///
/// 参数说明：
/// - `datasource`：本次本地事务绑定的数据源。
/// - `body`：返回提交或回滚裁决的业务 Future。
///
/// 返回：datasource 入口失败与事务执行失败保持为两个可穷举阶段。
pub async fn run_decided_for_checked<T, E, F, D>(
    datasource: D,
    body: F,
) -> Result<T, TxEntryError<E>>
where
    F: Future<Output = TxDecision<T, E>>,
    D: AsRef<str>,
{
    let datasource = DatasourceRef::new(datasource)
        .map_err(|_| TxEntryError::Lookup(DataSourceLookupError::InvalidName))?;
    natx_core::ensure_driver(DatabaseDriver::MySql).map_err(|error| {
        TxEntryError::Run(TxRunError::Infrastructure {
            reason: error.to_string(),
        })
    })?;
    let prepared_pool = if CUR_TX.try_with(|_| ()).is_ok() {
        None
    } else {
        Some(pool_for_checked(datasource.as_str()).map_err(TxEntryError::Lookup)?)
    };
    run_decision_kernel_with_pool(
        datasource,
        body,
        |_| "nested explicit rollback".to_string(),
        prepared_pool,
    )
    .await
    .map_err(TxEntryError::Run)
}

/// 业务作用：按业务代码给出的显式裁决，在指定 datasource 中提交或回滚本地事务。
///
/// 嵌套 `Rollback` 会先把整个 ambient transaction 标为 rollback-only；即使外层吞掉
/// 内层错误并返回 `Commit`，最外层仍执行物理回滚。所有错误 reason 均为稳定脱敏分类。
///
/// 参数说明：
/// - `datasource`: 本次事务使用的数据源名称。
/// - `body`: 返回提交或回滚裁决的业务 Future。
///
/// 返回：最外层提交确认后返回领域值；明确回滚、rollback-only、提交拒绝/不确定、
/// 回滚失败或事务基础设施失败时返回对应分类。
pub async fn run_decided_for<T, E, F, D>(datasource: D, body: F) -> Result<T, TxRunError<E>>
where
    F: Future<Output = TxDecision<T, E>>,
    D: AsRef<str>,
{
    let datasource = DatasourceRef::new(datasource).map_err(|_| TxRunError::Infrastructure {
        reason: "invalid datasource name".to_string(),
    })?;
    run_decision_kernel(datasource, body, |_| "nested explicit rollback".to_string()).await
}

/// 业务作用：以兼容 `Ok`/`Err` 语义在指定 datasource 执行本地事务。
///
/// 嵌套调用时只能加入相同 datasource 的外层事务；如果当前已经在其它 datasource
/// 的事务中，直接返回 Err，避免把多库写入静默塞进错误连接。
///
/// 参数说明：
/// - `datasource`: 本次事务所属 datasource 名称。
/// - `body`: 需要在该 datasource 事务内执行的业务 future。
///
/// 返回：提交确认返回业务值；业务错误、rollback-only 或数据库失败返回 `anyhow::Error`；
/// 需要精确区分 commit uncertain/rollback failed 的调用方应使用 [`run_decided_for`]。
pub async fn run_for<T, F, D>(datasource: D, body: F) -> anyhow::Result<T>
where
    F: std::future::Future<Output = anyhow::Result<T>>,
    D: AsRef<str>,
{
    let datasource = DatasourceRef::new(datasource)?;
    let decision = async {
        match body.await {
            Ok(value) => TxDecision::Commit(value),
            Err(error) => TxDecision::Rollback(error),
        }
    };
    match run_decision_kernel(datasource, decision, |error| format!("{error:#}")).await {
        Ok(value) => Ok(value),
        Err(TxRunError::Rollback(error)) => Err(error),
        Err(TxRunError::RollbackOnly { reason }) => {
            Err(anyhow::Error::new(RollbackOnly { reason }))
        }
        Err(TxRunError::RollbackFailed { cause, reason }) => {
            // 兼容 API 仍返回原业务错误/RollbackOnly，但物理回滚失败必须留下可观测证据；
            // 只有显式 API 暴露完整 RollbackFailed 分类。
            tracing::error!(component = "tx", event = "rollback_failed", reason = %reason, "事务回滚失败");
            match cause {
                TxRollbackCause::Decision(error) => Err(error),
                TxRollbackCause::RollbackOnly { reason } => {
                    Err(anyhow::Error::new(RollbackOnly { reason }))
                }
            }
        }
        Err(TxRunError::CommitRejected { reason })
        | Err(TxRunError::CommitUncertain { reason })
        | Err(TxRunError::Infrastructure { reason }) => Err(anyhow::anyhow!(reason)),
    }
}

/// 业务作用：执行显式事务裁决的唯一内核，统一嵌套传播、物理提交/回滚与 after-commit 顺序。
///
/// 参数说明：
/// - `datasource`: 本次事务的数据源。
/// - `body`: 返回显式裁决的业务 Future。
/// - `describe_rollback`: 内层回滚被外层吞掉时保存的脱敏原因生成器。
///
/// 返回：提交确认时返回领域结果；其它阶段返回保持原始裁决的封闭错误分类。
async fn run_decision_kernel<T, E, F, R>(
    datasource: DatasourceRef,
    body: F,
    describe_rollback: R,
) -> Result<T, TxRunError<E>>
where
    F: Future<Output = TxDecision<T, E>>,
    R: Fn(&E) -> String,
{
    run_decision_kernel_with_pool(datasource, body, describe_rollback, None).await
}

/// 业务作用：复用 checked 入口已经解析的 pool 执行事务，消除查找与 BEGIN 之间的重复全局查询。
///
/// 参数说明：
/// - `datasource`：本次事务的数据源。
/// - `body`：返回显式裁决的业务 Future。
/// - `describe_rollback`：生成首个 rollback-only 脱敏原因。
/// - `prepared_pool`：外层 checked 入口已解析的 pool；嵌套或兼容入口传 `None`。
///
/// 返回：数据库确认提交时返回业务值；其它执行阶段返回封闭事务分类。
async fn run_decision_kernel_with_pool<T, E, F, R>(
    datasource: DatasourceRef,
    body: F,
    describe_rollback: R,
    prepared_pool: Option<MySqlPool>,
) -> Result<T, TxRunError<E>>
where
    F: Future<Output = TxDecision<T, E>>,
    R: Fn(&E) -> String,
{
    natx_core::ensure_driver(DatabaseDriver::MySql).map_err(|error| {
        TxRunError::Infrastructure {
            reason: error.to_string(),
        }
    })?;
    // 嵌套调用只能加入相同 datasource；跨库写不能静默伪装成一个本地事务。
    if let Ok(ctx) = CUR_TX.try_with(Clone::clone) {
        if ctx.datasource != datasource {
            return Err(TxRunError::Infrastructure {
                reason: "ambient transaction datasource mismatch".to_string(),
            });
        }
        return match body.await {
            TxDecision::Commit(value) => Ok(value),
            TxDecision::Rollback(error) => {
                // 内层一旦要求回滚就置位全局门禁；外层吞掉错误也不能重新获得提交权。
                ctx.rollback_only.store(true, Ordering::Release);
                let mut reason = ctx.rollback_reason.lock().unwrap();
                if reason.is_none() {
                    *reason = Some(describe_rollback(&error));
                }
                Err(TxRunError::Rollback(error))
            }
        };
    }

    let pool = match prepared_pool {
        Some(pool) => pool,
        None => pool_for(datasource.as_str()).map_err(|error| TxRunError::Infrastructure {
            reason: error.to_string(),
        })?,
    };
    let connection = observed_pool_acquire(
        datasource.as_str(),
        observability::ConnectionPurpose::Direct,
        &pool,
    )
    .await
    .map_err(|_| TxRunError::Infrastructure {
        reason: "transaction connection acquisition failed".to_string(),
    })?;
    let transaction =
        Transaction::begin(connection, None)
            .await
            .map_err(|_| TxRunError::Infrastructure {
                reason: "transaction begin failed".to_string(),
            })?;
    let slot: TxSlot = Arc::new(Mutex::new(Some(transaction)));
    let ctx: TxCtx = Arc::new(TxContext {
        tx: slot.clone(),
        datasource,
        rollback_only: AtomicBool::new(false),
        rollback_reason: std::sync::Mutex::new(None),
        after_commit: std::sync::Mutex::new(Vec::new()),
    });
    let decision = natx_core::scope_driver(DatabaseDriver::MySql, CUR_TX.scope(ctx.clone(), body))
        .await
        .map_err(|error| TxRunError::Infrastructure {
            reason: error.to_string(),
        })?;
    // 提交/回滚前必须独占槽。业务体已返回,常规语句守卫必然已释放;此刻仍有人持锁只能是
    // 泄漏的连接句柄(未消费完的事务内 MapperStream、被移出事务体的 Conn)——用阻塞等待会把
    // 泄漏变成永久卡死,fail-fast 才能把缺陷在提交门禁处显形。
    let transaction = slot
        .try_lock()
        .map_err(|_| TxRunError::Infrastructure {
            reason: "transaction connection is still held at commit; drop or fully consume any \
                     transactional MapperStream/Conn before the transactional body returns"
                .to_string(),
        })?
        .take()
        .ok_or_else(|| TxRunError::Infrastructure {
            reason: "transaction ownership was lost".to_string(),
        })?;

    match decision {
        TxDecision::Commit(_value) if ctx.rollback_only.load(Ordering::Acquire) => {
            let reason = ctx
                .rollback_reason
                .lock()
                .unwrap()
                .take()
                .unwrap_or_else(|| "nested transaction requested rollback".to_string());
            // rollback-only 是提交前最后门禁；物理回滚未确认时不能返回普通 RollbackOnly。
            match transaction.rollback().await {
                Ok(()) => Err(TxRunError::RollbackOnly { reason }),
                Err(_) => Err(TxRunError::RollbackFailed {
                    cause: TxRollbackCause::RollbackOnly { reason },
                    reason: "database transaction rollback failed".to_string(),
                }),
            }
        }
        TxDecision::Commit(value) => match transaction.commit().await {
            Ok(()) => {
                run_after_commit_hooks(&ctx).await;
                Ok(value)
            }
            Err(sqlx::Error::Database(_)) => Err(TxRunError::CommitRejected {
                reason: "database rejected transaction commit".to_string(),
            }),
            Err(
                sqlx::Error::Io(_)
                | sqlx::Error::Tls(_)
                | sqlx::Error::Protocol(_)
                | sqlx::Error::WorkerCrashed,
            ) => Err(TxRunError::CommitUncertain {
                reason: "database commit acknowledgement is uncertain".to_string(),
            }),
            Err(_) => Err(TxRunError::Infrastructure {
                reason: "transaction commit infrastructure failed".to_string(),
            }),
        },
        TxDecision::Rollback(error) => match transaction.rollback().await {
            Ok(()) => Err(TxRunError::Rollback(error)),
            Err(_) => Err(TxRunError::RollbackFailed {
                cause: TxRollbackCause::Decision(error),
                reason: "database transaction rollback failed".to_string(),
            }),
        },
    }
}

/// 业务作用：只在数据库明确确认 COMMIT 后执行并清空 after-commit hooks。
///
/// 参数说明：
/// - `context`: 本次最外层事务上下文。
///
/// 返回：无；hook panic/join 失败只记录日志，不能把已经成功的数据库提交伪装成失败。
async fn run_after_commit_hooks(context: &TxContext) {
    let hooks = {
        let mut hooks = context.after_commit.lock().unwrap();
        std::mem::take(&mut *hooks)
    };
    for hook in hooks {
        match tokio::spawn(hook()).await {
            Ok(()) => {}
            Err(error) if error.is_panic() => {
                tracing::error!(
                    component = "tx",
                    event = "after_commit_hook_panic",
                    "after_commit hook panicked after transaction commit"
                );
            }
            Err(error) => {
                tracing::error!(
                    component = "tx",
                    event = "after_commit_hook_join_error",
                    error = %error,
                    "after_commit hook task failed after transaction commit"
                );
            }
        }
    }
}

/// 业务作用：登记只在最外层事务确认提交后执行的异步 hook，避免回滚事务泄漏外部副作用。
///
/// 该 hook 只在当前任务已经处于 ambient 事务中时允许注册；如果事务最终 rollback、
/// 被 rollback-only 拦截、或者 commit 自身失败，hook 都不会执行。
///
/// 参数说明：
/// - `f`: commit 成功后才执行的异步闭包。
///
/// 返回：事务内登记成功返回 `Ok`；没有 ambient transaction 时拒绝登记并返回错误。
pub fn after_commit<F, Fut>(f: F) -> anyhow::Result<()>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let ctx = CUR_TX
        .try_with(|ctx| ctx.clone())
        .map_err(|_| anyhow::anyhow!("natx::after_commit 只能在 #[transactional] 事务内注册"))?;
    ctx.after_commit.lock().unwrap().push(Box::new(
        move || -> Pin<Box<dyn Future<Output = ()> + Send>> { Box::pin(f()) },
    ));
    Ok(())
}

// ════════════════════════════════════════════════════════════════════════════
// conn —— repo 取"当前连接":在事务里就用事务连接,否则从 pool 取
// ════════════════════════════════════════════════════════════════════════════
/// 业务作用：获取默认 datasource 的当前连接；事务内复用事务连接，事务外从池中获取。
///
/// 参与 ambient 事务的查询必须从此入口取得连接。直接使用另一 Pool 会绕过当前事务，
/// 其写入不受当前事务的回滚约束；数据访问层可显式接收事务 executor，或复用本入口。
///
/// 参数说明: 无。
///
/// 返回：事务内返回持锁事务连接，事务外返回池连接；连接源不可用返回错误。
pub async fn conn() -> anyhow::Result<Conn> {
    conn_for(DEFAULT_DATASOURCE).await
}

/// 业务作用：从指定 datasource 获取当前连接，并拒绝事务内跨 datasource 的错误复用。
///
/// 如果当前处于 ambient 事务中，datasource 必须与事务 datasource 相同；否则返回
/// Err，避免事务内跨库时静默复用错误连接。
///
/// 参数说明：
/// - `datasource`: 本次 SQL 应使用的 datasource 名称。
///
/// 返回：事务内返回同 datasource 的事务连接，事务外返回池连接；名称、归属或获取失败返回错误。
pub async fn conn_for(datasource: impl AsRef<str>) -> anyhow::Result<Conn> {
    observed_conn_for(
        datasource.as_ref(),
        observability::ConnectionPurpose::Direct,
        ConnectionMode::Optional,
    )
    .await
}

/// 业务作用：获取默认 datasource 的强制事务连接，阻止关键写在上下文丢失后降级为 autocommit。
///
/// 同 [`conn`],但【必须】处于 ambient 事务中,否则返回 `Err`(**不** fallback 到 pool)。
///
/// 用于**关键写 repo**:确保该 SQL 一定参与当前事务,杜绝"上下文丢失(如 `tokio::spawn`)时
/// 静默换成 autocommit 连接、写入绕过回滚"的脏写。可独立(无事务)调用的 repo 仍用 [`conn`]。
///
/// 参数说明: 无。
///
/// 返回：事务内返回持锁事务连接；无事务或 datasource 不匹配返回错误。
pub async fn mandatory_conn() -> anyhow::Result<Conn> {
    mandatory_conn_for(DEFAULT_DATASOURCE).await
}

/// 业务作用：获取指定 datasource 的强制事务连接，并把连接归属作为关键写门禁。
///
/// 参数说明：
/// - `datasource`: 本次关键写 SQL 必须加入的 datasource 名称。
///
/// 返回：ambient transaction 存在且 datasource 一致时返回连接；否则返回错误且绝不 fallback。
pub async fn mandatory_conn_for(datasource: impl AsRef<str>) -> anyhow::Result<Conn> {
    observed_conn_for(
        datasource.as_ref(),
        observability::ConnectionPurpose::Direct,
        ConnectionMode::Mandatory,
    )
    .await
}

/// 业务作用：获取默认 datasource 的非事务连接，拒绝在 ambient 事务内调用(`tx = "never"` 的连接入口)。
///
/// 参数说明: 无。
///
/// 返回：无 ambient 事务时返回池连接(语句各自提交)；处于事务内立即返回错误且不取连接。
pub async fn never_conn() -> anyhow::Result<Conn> {
    never_conn_for(DEFAULT_DATASOURCE).await
}

/// 业务作用：获取指定 datasource 的非事务连接，把"该语句不允许随外层事务回滚"固化为连接门禁。
///
/// 与 [`run_never`] 同一裁决:副作用必须立即可见的语句(自治审计写、维护操作)被事务内调用时,
/// 在取连接前显式失败,而不是静默加入外层事务。
///
/// 参数说明：
/// - `datasource`: 本次 SQL 使用的 datasource 名称。
///
/// 返回：无 ambient 事务时返回该 datasource 的池连接；处于事务内返回携带事务 datasource 的错误。
pub async fn never_conn_for(datasource: impl AsRef<str>) -> anyhow::Result<Conn> {
    observed_conn_for(
        datasource.as_ref(),
        observability::ConnectionPurpose::Direct,
        ConnectionMode::Never,
    )
    .await
}

/// 业务作用：为 Mapper 获取连接并明确标记连接用途。
/// 参数说明：`datasource` 是静态 Mapper 绑定的数据源。
/// 返回：同源事务连接或池连接；拒绝和等待保持原有事务语义。
pub async fn mapper_conn_for(datasource: impl AsRef<str>) -> anyhow::Result<Conn> {
    observed_conn_for(
        datasource.as_ref(),
        observability::ConnectionPurpose::Mapper,
        ConnectionMode::Optional,
    )
    .await
}

/// 业务作用：为关键 Mapper 写强制取得同源事务连接。
/// 参数说明：`datasource` 是 Mapper 的数据源。
/// 返回：事务存在且同源时返回连接；不会回落到 autocommit。
pub async fn mapper_mandatory_conn_for(datasource: impl AsRef<str>) -> anyhow::Result<Conn> {
    observed_conn_for(
        datasource.as_ref(),
        observability::ConnectionPurpose::Mapper,
        ConnectionMode::Mandatory,
    )
    .await
}

/// 业务作用：为不允许进入事务的 Mapper 获取池连接。
/// 参数说明：`datasource` 是 Mapper 的数据源。
/// 返回：没有 ambient transaction 时返回池连接；存在事务时拒绝。
pub async fn mapper_never_conn_for(datasource: impl AsRef<str>) -> anyhow::Result<Conn> {
    observed_conn_for(
        datasource.as_ref(),
        observability::ConnectionPurpose::Mapper,
        ConnectionMode::Never,
    )
    .await
}

/// 业务作用：允许框架调用方显式区分迁移、探测和直接连接用途。
/// 参数说明：`datasource` 指定库；`purpose` 是固定用途。
/// 返回：沿用同源 ambient 事务选择规则的受观测连接。
pub async fn conn_for_with_purpose(
    datasource: impl AsRef<str>,
    purpose: observability::ConnectionPurpose,
) -> anyhow::Result<Conn> {
    observed_conn_for(datasource.as_ref(), purpose, ConnectionMode::Optional).await
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConnectionMode {
    Optional,
    Mandatory,
    Never,
}

/// 业务作用：在连接权威边界统一门禁、池等待与事务槽等待。
/// 参数说明：`datasource` 是目标库；`purpose` 是用途；`mode` 固定事务要求。
/// 返回：只在全部门禁通过后取得连接，失败不会伪装成数据库执行错误。
async fn observed_conn_for(
    datasource: &str,
    purpose: observability::ConnectionPurpose,
    mode: ConnectionMode,
) -> anyhow::Result<Conn> {
    use observability::{AcquireOutcome, ConnectionRejection as Rejection};
    let observation = observability::connection_slot(DatabaseDriver::MySql, datasource);
    // 跨后端请求在获取任何连接之前拒绝，避免事务内绕过既有控制权威。
    natx_core::ensure_driver(DatabaseDriver::MySql).map_err(|error| {
        observation.reject(Rejection::CrossDriver);
        anyhow::Error::new(error)
    })?;
    let reference = DatasourceRef::new(datasource).inspect_err(|_| {
        observation.reject(Rejection::UnknownDatasource);
    })?;
    match CUR_TX.try_with(|ctx| (ctx.datasource.clone(), ctx.tx.clone())) {
        Ok((tx_datasource, slot)) => {
            // 自治语句不能悄然加入外层事务，否则副作用可见性会随回滚改变。
            if mode == ConnectionMode::Never {
                observation.reject(Rejection::TxForbidden);
                anyhow::bail!("tx=never 的语句拒绝在 ambient 事务内执行(当前事务 datasource=`{tx_datasource}`)");
            }
            // 连接归属必须与事务一致，绝不能把跨库访问伪装成同一个本地事务。
            if tx_datasource != reference {
                observation.reject(Rejection::CrossDatasource);
                anyhow::bail!("当前事务 datasource=`{tx_datasource}` 不能获取 datasource=`{reference}` 的连接");
            }
            let mut wait = observation.transaction_slot();
            match slot.try_lock_owned() {
                Ok(connection) => {
                    wait.finish(AcquireOutcome::Ok);
                    Ok(Conn::Tx(connection))
                }
                Err(_) => {
                    // 同一物理连接不能同时交给两个可变访问者；立即拒绝可避免当前任务等待自己释放守卫。
                    wait.finish(AcquireOutcome::Other);
                    observation.reject(Rejection::TxConnectionBusy);
                    anyhow::bail!(
                        "当前事务连接已被持有；必须先让已有 Conn 离开作用域，再调用 conn()"
                    )
                }
            }
        }
        Err(_) => {
            // 强制事务操作失去上下文时必须拒绝，不能降级到立即提交的连接。
            if mode == ConnectionMode::Mandatory {
                observation.reject(Rejection::TxRequired);
                anyhow::bail!("mandatory_conn_for({reference}):当前不在 #[transactional] 事务中(关键写必须在事务内)");
            }
            let pool = pool_for_checked(datasource).map_err(|error| {
                observation.reject(match error {
                    DataSourceLookupError::DriverMismatch { .. } => Rejection::CrossDriver,
                    DataSourceLookupError::InvalidName | DataSourceLookupError::NotFound { .. } => {
                        Rejection::UnknownDatasource
                    }
                    _ => Rejection::RegistryUnavailable,
                });
                anyhow::Error::new(error)
            })?;
            Ok(Conn::Pool(
                observed_pool_acquire(datasource, purpose, &pool).await?,
            ))
        }
    }
}

/// 业务作用：只计量 Pool 获取阶段，避免把 SQL 执行时间混入等待。
/// 参数说明：`datasource` 与 `purpose` 是固定身份；`pool` 是已通过门禁的池。
/// 返回：成功时交出池连接；失败在类型擦除前记录稳定分类。此入口不加入 ambient 事务，调用方负责池的控制权威。
pub async fn observed_pool_acquire(
    datasource: &str,
    purpose: observability::ConnectionPurpose,
    pool: &MySqlPool,
) -> Result<sqlx::pool::PoolConnection<MySql>, sqlx::Error> {
    let mut wait =
        observability::connection_slot(DatabaseDriver::MySql, datasource).acquire(purpose);
    let result = pool.acquire().await;
    wait.finish(match &result {
        Ok(_) => observability::AcquireOutcome::Ok,
        Err(error) => classify_acquire_error(error),
    });
    result
}

/// 业务作用：把连接获取错误映射为不依赖错误原文的稳定分类。
/// 参数说明：`error` 是 SQLx 获取阶段的类型化错误。
/// 返回：超时、关闭、worker 或其它固定分类。
pub fn classify_acquire_error(error: &sqlx::Error) -> observability::AcquireOutcome {
    use observability::AcquireOutcome;
    match error {
        sqlx::Error::PoolTimedOut => AcquireOutcome::Timeout,
        sqlx::Error::PoolClosed => AcquireOutcome::Closed,
        sqlx::Error::WorkerCrashed => AcquireOutcome::Worker,
        _ => AcquireOutcome::Other,
    }
}

/// 业务作用：为已冻结的 MySQL Pool 目录建立抓取侧状态源。
/// 参数说明：`pools` 为数据源名称与对应池句柄。
/// 返回：身份全部合法时返回近似 Pool 快照源，不创建采样 worker。
pub fn pool_metrics_source(
    pools: impl IntoIterator<Item = (String, MySqlPool)>,
) -> anyhow::Result<observability::PoolMetricsSource> {
    struct PoolReader(MySqlPool);
    impl observability::PoolSnapshot for PoolReader {
        /// 业务作用：读取 MySQL Pool 的近似当前状态。
        /// 参数说明：无。
        /// 返回：连接总量、空闲量和配置上限。
        fn snapshot(&self) -> observability::PoolState {
            observability::PoolState {
                total: self.0.size(),
                idle: self.0.num_idle().min(u32::MAX as usize) as u32,
                max: self.0.options().get_max_connections(),
            }
        }
    }
    observability::PoolMetricsSource::new(
        DatabaseDriver::MySql,
        pools
            .into_iter()
            .map(|(name, pool)| {
                (
                    name,
                    Arc::new(PoolReader(pool)) as Arc<dyn observability::PoolSnapshot>,
                )
            })
            .collect(),
    )
}

/// "当前连接"句柄。两种来源统一暴露成 `&mut MySqlConnection`(它实现了 sqlx::Executor):
///   · 事务连接:Transaction 与 PoolConnection 都 DerefMut 到 MySqlConnection,故能统一。
/// ⚠️ 同一段代码里【不要同时持有两个 conn() 句柄】:事务分支的第二次取得会立即返回连接占用错误。
///    正确用法是 query 跑完即让 Conn 离开作用域(锁随之释放),下一条 query 再 conn()。
pub enum Conn {
    /// 事务连接:持有槽的 OwnedMutexGuard(锁),内含 Transaction。
    Tx(OwnedMutexGuard<Option<Transaction<'static, MySql>>>),
    /// 普通连接:从池取走的一条连接(用完归还)。
    Pool(sqlx::pool::PoolConnection<MySql>),
}

impl Conn {
    /// 业务作用：把非事务池连接标记为归还时关闭，用于会话锁解除结果不确定等不可复用状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：池连接成功进入 close-on-drop 保护态；事务连接没有独立会话所有权时返回错误。
    pub fn close_on_drop(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Pool(connection) => {
                connection.close_on_drop();
                Ok(())
            }
            Self::Tx(_) => anyhow::bail!(
                "ambient transaction connection cannot be independently closed on drop"
            ),
        }
    }

    /// 业务作用：把事务连接与池连接统一暴露为 SQLx 执行所需的可变 MySQL 连接。
    ///
    /// 取出 `&mut MySqlConnection` 交给 sqlx 执行 query。
    /// 该方法让事务连接和普通池连接在业务调用点具有同一执行入口。
    ///
    /// Transaction / PoolConnection 都 DerefMut 到 MySqlConnection,这里靠 deref coercion 统一返回类型。
    /// 命名与 AsMut::as_mut 同形是刻意的(语义一致);改名会破坏既有业务调用方,仅消 lint。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前句柄持有的可变 MySQL 连接；事务所有权提前丢失属于不变量破坏并 panic。
    #[allow(clippy::should_implement_trait)]
    pub fn as_mut(&mut self) -> &mut MySqlConnection {
        match self {
            Conn::Tx(guard) => guard.as_mut().expect("事务已被取走"), // &mut Transaction → &mut MySqlConnection
            Conn::Pool(c) => c, // &mut PoolConnection → &mut MySqlConnection
        }
    }
}

// ── re-export 过程宏：业务项目 `use natx::transactional;` 即可。──
pub use natx_macro::transactional;

/// 业务作用：让连接池等待或当前事务连接的互斥等待服从调用链预算。
/// 参数说明：`datasource` 为已注册来源；`budget` 为共享绝对预算。
/// 返回：成功取得连接守卫；已取消或到期时停止等待，不提交、回滚或重试业务事务。
pub async fn conn_for_budget(
    datasource: impl AsRef<str>,
    budget: &nabudget::RequestBudget,
) -> anyhow::Result<Conn> {
    budget.run(conn_for(datasource)).await?
}

/// 业务作用：显式约束调用方确认无业务写入副作用的读取等待。
/// 参数说明：`budget` 为共享预算；`read` 为已确认只读的查询 future。
/// 返回：保留查询结果或预算错误；取消会丢弃当前 future，不承诺数据库从未执行。
/// 本方法不解析 SQL、不根据 SELECT 前缀推断安全性；有副作用的函数、锁定读和事务写不得使用。
/// COMMIT、事务提交结果和业务重试继续由事务入口裁决。
pub async fn read_with_budget<T>(
    budget: &nabudget::RequestBudget,
    read: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    budget.run(read).await?
}
