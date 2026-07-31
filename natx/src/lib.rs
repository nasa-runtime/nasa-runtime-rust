//! 基于 task-local 的 ambient 事务运行时。
//!
//! 业务方法可在事务上下文中自动复用同一条数据库连接，支持默认数据源和命名数据源。
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
//        ③ 不可在持有一个 Conn 时再取 Conn(非重入 Mutex 会卡)。彻底免疫请用显式 &mut Transaction。
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

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use sqlx::{MySql, MySqlConnection, Transaction};

pub mod datasource;

/// 重导出底层连接池类型：Application 与业务的公开 getter 需要能命名它，
/// 且必须与本 crate 使用的 sqlx 版本严格同源，否则跨版本会得到两个不同一的类型。
pub use sqlx::MySqlPool;
use tokio::sync::{Mutex, OwnedMutexGuard};

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
    datasource: &'static str,
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
//   main 启动建好池后调 natx::init(pool) 注入一次(和 hystrix REGISTRY、cacheable L2 同套路)。
static POOL: OnceLock<MySqlPool> = OnceLock::new();
static DATASOURCE_POOLS: OnceLock<StdMutex<HashMap<String, MySqlPool>>> = OnceLock::new();
const DEFAULT_DATASOURCE: &str = "default";

/// 事务被标记 rollback-only 时,最外层 `run()` 返回的具名错误(经 `anyhow::Error` 携带)。
/// 上层可 `err.downcast_ref::<natx::RollbackOnly>()` 区分它与普通业务错误。
/// 触发场景:某嵌套 `#[transactional]` 的 body 返回了 `Err`,但被外层吞掉/转成 `Ok` —— 整体仍回滚。
#[derive(Debug)]
pub struct RollbackOnly {
    /// 首个置位 rollback-only 的内层错误的 Display 文本(线索)。
    pub reason: String,
}

impl std::fmt::Display for RollbackOnly {
    /// 实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
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

/// 注入连接池(main 启动时调一次)。重复调用【不会替换】已有 pool —— 此时记 error 日志使其可见,
/// 而非静默忽略(避免"以为换了 pool 实际没生效"的隐蔽问题)。需要启动期 fail-fast 用 [`try_init`]。
///
/// # 参数
/// - `pool`: 应用启动时创建好的 MySQL 连接池,作为后续事务和普通连接获取的全局来源。
pub fn init(pool: MySqlPool) {
    if let Err(e) = try_init(pool) {
        tracing::error!(error = %e, "natx::init 重复调用,本次注入被忽略(pool 不会被替换)");
    }
}

/// 注入连接池,**重复初始化返回 Err**(启动期推荐用它,以便 fail-fast 而非只打日志)。
///
/// # 参数
/// - `pool`: 应用启动时创建好的 MySQL 连接池,会写入全局 OnceLock 且不能被后续调用替换。
pub fn try_init(pool: MySqlPool) -> anyhow::Result<()> {
    POOL.set(pool).map_err(|_| {
        anyhow::anyhow!("natx::init/try_init 重复调用:连接池已初始化,不能替换(OnceLock 只能设一次)")
    })
}

/// 注入命名 datasource 连接池。`default` 等价于 [`try_init`]。
///
/// 命名 datasource 主要给 Mapper 和明确多库业务使用；它不会改变现有无参
/// `#[transactional]` / [`conn`] 的默认库语义。
///
/// # 参数
/// - `name`: datasource 名称；`"default"` 表示默认库，其它名称必须非空且无首尾空白。
/// - `pool`: 该 datasource 对应的 MySQL 连接池。
pub fn try_init_datasource(name: impl Into<String>, pool: MySqlPool) -> anyhow::Result<()> {
    let name = name.into();
    validate_datasource_name(&name)?;
    if name == DEFAULT_DATASOURCE {
        return try_init(pool);
    }
    let pools = DATASOURCE_POOLS.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut pools = pools.lock().unwrap();
    if pools.contains_key(&name) {
        return Err(anyhow::anyhow!(
            "natx::try_init_datasource 重复调用:datasource `{name}` 已初始化,不能替换"
        ));
    }
    pools.insert(name, pool);
    Ok(())
}

/// 注入命名 datasource 连接池；重复调用只打 error 日志。
///
/// # 参数
/// - `name`: datasource 名称；`"default"` 表示默认库。
/// - `pool`: 该 datasource 对应的 MySQL 连接池。
pub fn init_datasource(name: impl Into<String>, pool: MySqlPool) {
    let name = name.into();
    if let Err(e) = try_init_datasource(name.clone(), pool) {
        tracing::error!(datasource = %name, error = %e, "natx::init_datasource 重复调用,本次注入被忽略(pool 不会被替换)");
    }
}

/// 校验 datasource 名称是否适合进入全局连接池表和错误日志。
///
/// # 参数
/// - `name`: 待校验的 datasource 名称。
fn validate_datasource_name(name: &str) -> anyhow::Result<()> {
    if name.trim().is_empty() {
        return Err(anyhow::anyhow!("datasource 名称不能为空"));
    }
    if name.trim() != name {
        return Err(anyhow::anyhow!(
            "datasource `{name}` 不合法:首尾不能包含空白"
        ));
    }
    Ok(())
}

/// 获取指定 datasource 的连接池 clone。
///
/// # 参数
/// - `datasource`: 业务声明的 datasource 名称。
fn pool_for(datasource: &str) -> anyhow::Result<MySqlPool> {
    validate_datasource_name(datasource)?;
    if datasource == DEFAULT_DATASOURCE {
        return Ok(pool().clone());
    }
    let pools = DATASOURCE_POOLS
        .get()
        .ok_or_else(|| anyhow::anyhow!("datasource `{datasource}` 未初始化"))?;
    let pools = pools.lock().unwrap();
    pools
        .get(datasource)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("datasource `{datasource}` 未初始化"))
}

/// 返回指定 datasource 的连接池 clone。
///
/// 这是给无事务、长生命周期执行器使用的只读入口，例如声明式 Mapper 的 stream/cursor
/// 查询。它不会加入 ambient 事务；需要事务语义的普通 SQL 仍必须走 [`conn_for`]。
///
/// # 参数
/// - `datasource`: 业务声明的 datasource 名称。
pub fn pool_for_datasource(datasource: &str) -> anyhow::Result<MySqlPool> {
    pool_for(datasource)
}

/// 当前是否处于 ambient 事务中。
/// 用途:事务内 `tokio::spawn` 出的 task【不继承】当前事务(task_local 不跨 spawn 传播),
/// 其 `natx::conn()` 会 fallback 到 pool、写入绕过事务且不随回滚撤销。业务可在 spawn 前用本函数自检。
pub fn in_transaction() -> bool {
    CUR_TX.try_with(|_| ()).is_ok()
}

/// 当前 ambient 事务所属 datasource；无事务时返回 `None`。
pub fn current_datasource() -> Option<&'static str> {
    CUR_TX.try_with(|ctx| ctx.datasource).ok()
}

/// 返回全局数据库连接池；用于事务封装复用同一连接来源。
fn pool() -> &'static MySqlPool {
    POOL.get()
        .expect("tx 未初始化:请在 main 启动时调用 natx::init(db_pool)")
}

// ════════════════════════════════════════════════════════════════════════════
// run —— #[transactional] 生成的代码会调它(begin / 提交 / 回滚 / 传播)
// ════════════════════════════════════════════════════════════════════════════
/// 在事务中执行 body:正常返回 Ok → commit;返回 Err → rollback。
/// 传播规则:若【外层已在事务中】(CUR_TX 已设),则直接复用、不另开事务(由最外层统一提交)。
///
/// # 参数
/// - `body`: 需要在默认 datasource 事务内执行的业务 future。
pub async fn run<T, F>(body: F) -> anyhow::Result<T>
where
    F: std::future::Future<Output = anyhow::Result<T>>,
{
    run_for(DEFAULT_DATASOURCE, body).await
}

/// 在指定 datasource 的事务中执行 body。
///
/// 嵌套调用时只能加入相同 datasource 的外层事务；如果当前已经在其它 datasource
/// 的事务中，直接返回 Err，避免把多库写入静默塞进错误连接。
///
/// # 参数
/// - `datasource`: 本次事务所属 datasource 名称。
/// - `body`: 需要在该 datasource 事务内执行的业务 future。
pub async fn run_for<T, F>(datasource: &'static str, body: F) -> anyhow::Result<T>
where
    F: std::future::Future<Output = anyhow::Result<T>>,
{
    validate_datasource_name(datasource)?;
    // ── 传播:已经在事务里 → 跑 body 加入外层事务,提交/回滚交给最外层 run ──
    //   若 body 返回 Err,标记整个事务 rollback-only:即便外层吞掉该 Err 并返回 Ok,
    //   最外层 run() 提交前也会发现并回滚(rollback-only 整体回滚)。
    if let Ok(ctx) = CUR_TX.try_with(|c| c.clone()) {
        if ctx.datasource != datasource {
            return Err(anyhow::anyhow!(
                "当前事务 datasource=`{}` 不能加入 datasource=`{}` 的事务",
                ctx.datasource,
                datasource
            ));
        }
        let result = body.await;
        if let Err(e) = &result {
            ctx.rollback_only.store(true, Ordering::Release);
            // 只记首个原因(后续不覆盖)
            let mut reason = ctx.rollback_reason.lock().unwrap();
            if reason.is_none() {
                *reason = Some(format!("{e:#}"));
            }
        }
        return result;
    }

    // ── 最外层:begin 一个事务,建上下文放进 task_local ──
    // pool().begin() 返回 Transaction<'static>(它从池里取走一条连接,自带 'static)。
    let pool = pool_for(datasource)?;
    let slot: TxSlot = Arc::new(Mutex::new(Some(pool.begin().await?)));
    let ctx: TxCtx = Arc::new(TxContext {
        tx: slot.clone(),
        datasource,
        rollback_only: AtomicBool::new(false),
        rollback_reason: std::sync::Mutex::new(None),
        after_commit: std::sync::Mutex::new(Vec::new()),
    });

    // CUR_TX.scope(value, future):在 future 执行期间把 value 设进 task_local;
    //   future 内部(以及它 .await 的所有嵌套调用)都能用 CUR_TX.try_with 取到这个 ctx。
    let result = CUR_TX.scope(ctx.clone(), body).await;

    // ── 取回事务并按结果提交/回滚 ──
    // take():把 Transaction 从 Option 里【按值】取出来(commit/rollback 都消费 self)。
    let tx = slot
        .lock()
        .await
        .take()
        .expect("事务连接丢失(不应发生:run 独占该 slot 的提交权)");
    match result {
        Ok(v) => {
            // body 整体成功,但若任一嵌套层置位了 rollback-only,则整体回滚并报错(脏提交防线)。
            if ctx.rollback_only.load(Ordering::Acquire) {
                if let Err(re) = tx.rollback().await {
                    tracing::error!(component = "tx", event = "rollback_failed", phase = "rollback_only", rollback_error = %re, "rollback-only 事务回滚失败");
                }
                let reason = ctx
                    .rollback_reason
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap_or_default();
                // 用具名错误类型(而非裸字符串),上层可 `downcast_ref::<natx::RollbackOnly>()`
                // 区分"业务错误"与"事务被 rollback-only 拦截"。
                Err(anyhow::Error::new(RollbackOnly { reason }))
            } else {
                tx.commit().await?; // 业务成功 → 提交
                let hooks = {
                    let mut hooks = ctx.after_commit.lock().unwrap();
                    std::mem::take(&mut *hooks)
                };
                for hook in hooks {
                    match tokio::spawn(hook()).await {
                        Ok(()) => {}
                        Err(e) if e.is_panic() => {
                            tracing::error!(
                                component = "tx",
                                event = "after_commit_hook_panic",
                                "after_commit hook panicked after transaction commit"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                component = "tx",
                                event = "after_commit_hook_join_error",
                                error = %e,
                                "after_commit hook task failed after transaction commit"
                            );
                        }
                    }
                }
                Ok(v)
            }
        }
        Err(e) => {
            // 业务失败 → 回滚。回滚错误不掩盖原始业务错误,但【记日志】使其可见(原实现静默吞掉)。
            if let Err(re) = tx.rollback().await {
                tracing::error!(component = "tx", event = "rollback_failed", phase = "business_error", business_error = %e, rollback_error = %re, "事务回滚失败(业务错误后)");
            }
            Err(e)
        }
    }
}

/// 注册一个最外层事务 commit 成功后执行的异步 hook。
///
/// 该 hook 只在当前任务已经处于 ambient 事务中时允许注册；如果事务最终 rollback、
/// 被 rollback-only 拦截、或者 commit 自身失败，hook 都不会执行。
///
/// # 参数
/// - `f`: commit 成功后才执行的异步闭包。
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
/// repo 里每条 query 都先 `let mut c = natx::conn().await?;` 再 `query.execute(c.as_mut())`。
/// 这就是"task_local 取得到用事务、取不到用 pool"的落点。
///
/// # ★ 关键:想参与 `#[transactional]` 事务的 repo,连接【必须】走这里,不能用 self.pool
/// 同一个 repo 方法,取连接的方式决定它【能不能加入 ambient 事务】:
///
/// | repo 取连接方式 | 在 `#[transactional]` 里调用时 | 单独(无事务)调用时 | 备注 |
/// |---|---|---|---|
/// | `natx::conn().await`(本函数) | ✅ 用【事务连接】,写入随事务提交/回滚 | ✅ 用池连接,正常 | 想参与事务就用它 |
/// | `&self.pool`(struct 字段,如原版 KLineRepository) | ❌ **另取一条池连接,绕过事务** —— 写入【不在事务内、不会回滚】 | ✅ 用池连接,正常 | 需在 main 显式 `new` struct 并注入 |
///
/// 所以这是个**静默陷阱**:把一个 `self.pool` 风格的 repo 方法塞进 `#[transactional]` 流程里,
/// 它看起来"在事务里",实际却用了另一条连接 —— 事务回滚时它的写入**不会被撤销**(脏写)。
/// 对照:
///   · 原版 `repository/kline.rs`(struct + `self.pool`):只读查询,不需要事务,故用 self.pool;
///     代价是它【无法】加入 `#[transactional]`(真要它进事务,得改成收 executor 参数或走 natx::conn)。
///   · `repository/tx_demo.rs`(自由函数 + `natx::conn()`):为参与事务而生,无 struct、连接全局取。
/// 一句话:**要不要进事务,在"怎么取连接"时就决定了——进事务用 `natx::conn()`,不进事务才用 `self.pool`。**
pub async fn conn() -> anyhow::Result<Conn> {
    conn_for(DEFAULT_DATASOURCE).await
}

/// 同 [`conn`]，但从指定 datasource 取连接。
///
/// 如果当前处于 ambient 事务中，datasource 必须与事务 datasource 相同；否则返回
/// Err，避免事务内跨库时静默复用错误连接。
///
/// # 参数
/// - `datasource`: 本次 SQL 应使用的 datasource 名称。
pub async fn conn_for(datasource: &'static str) -> anyhow::Result<Conn> {
    validate_datasource_name(datasource)?;
    match CUR_TX.try_with(|ctx| (ctx.datasource, ctx.tx.clone())) {
        // 在事务里:锁住槽(OwnedMutexGuard 需要 Arc<Mutex>,故用 lock_owned),持有它到 query 跑完
        Ok((tx_datasource, slot)) => {
            if tx_datasource != datasource {
                return Err(anyhow::anyhow!(
                    "当前事务 datasource=`{tx_datasource}` 不能获取 datasource=`{datasource}` 的连接"
                ));
            }
            Ok(Conn::Tx(slot.lock_owned().await))
        }
        // 不在事务里:从池取一条独立连接
        Err(_) => Ok(Conn::Pool(pool_for(datasource)?.acquire().await?)),
    }
}

/// 同 [`conn`],但【必须】处于 ambient 事务中,否则返回 `Err`(**不** fallback 到 pool)。
///
/// 用于**关键写 repo**:确保该 SQL 一定参与当前事务,杜绝"上下文丢失(如 `tokio::spawn`)时
/// 静默换成 autocommit 连接、写入绕过回滚"的脏写。可独立(无事务)调用的 repo 仍用 [`conn`]。
pub async fn mandatory_conn() -> anyhow::Result<Conn> {
    mandatory_conn_for(DEFAULT_DATASOURCE).await
}

/// 同 [`mandatory_conn`]，但要求当前事务 datasource 与指定 datasource 相同。
///
/// # 参数
/// - `datasource`: 本次关键写 SQL 必须加入的 datasource 名称。
pub async fn mandatory_conn_for(datasource: &'static str) -> anyhow::Result<Conn> {
    validate_datasource_name(datasource)?;
    let (tx_datasource, slot) = CUR_TX.try_with(|ctx| (ctx.datasource, ctx.tx.clone())).map_err(|_| {
        anyhow::anyhow!("mandatory_conn_for({datasource}):当前不在 #[transactional] 事务中(关键写必须在事务内)")
    })?;
    if tx_datasource != datasource {
        return Err(anyhow::anyhow!(
            "当前事务 datasource=`{tx_datasource}` 不能获取 datasource=`{datasource}` 的 mandatory 连接"
        ));
    }
    Ok(Conn::Tx(slot.lock_owned().await))
}

/// "当前连接"句柄。两种来源统一暴露成 `&mut MySqlConnection`(它实现了 sqlx::Executor):
///   · 事务连接:Transaction 与 PoolConnection 都 DerefMut 到 MySqlConnection,故能统一。
/// ⚠️ 同一段代码里【不要同时持有两个 conn() 句柄】:事务分支会锁同一把 Mutex,嵌套持有会死锁。
///    正确用法是 query 跑完即让 Conn 离开作用域(锁随之释放),下一条 query 再 conn()。
pub enum Conn {
    /// 事务连接:持有槽的 OwnedMutexGuard(锁),内含 Transaction。
    Tx(OwnedMutexGuard<Option<Transaction<'static, MySql>>>),
    /// 普通连接:从池取走的一条连接(用完归还)。
    Pool(sqlx::pool::PoolConnection<MySql>),
}

impl Conn {
    /// 取出 `&mut MySqlConnection` 交给 sqlx 执行 query。
    /// 该方法让事务连接和普通池连接在业务调用点具有同一执行入口。
    ///
    /// Transaction / PoolConnection 都 DerefMut 到 MySqlConnection,这里靠 deref coercion 统一返回类型。
    /// 命名与 AsMut::as_mut 同形是刻意的(语义一致);改名会破坏既有业务调用方,仅消 lint。
    #[allow(clippy::should_implement_trait)]
    pub fn as_mut(&mut self) -> &mut MySqlConnection {
        match self {
            Conn::Tx(guard) => guard.as_mut().expect("事务已被取走"), // &mut Transaction → &mut MySqlConnection
            Conn::Pool(c) => c, // &mut PoolConnection → &mut MySqlConnection
        }
    }
}

// ── re-export 过程宏:业务项目 `use natx::transactional;` 即可(像 tokio re-export tokio-macros)──
pub use natx_macro::transactional;
