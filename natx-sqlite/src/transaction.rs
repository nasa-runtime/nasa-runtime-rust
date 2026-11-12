use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use natx_core::{
    DatabaseDriver, DatasourceRef, TxDecision, TxEntryError, TxRollbackCause, TxRunError,
    DEFAULT_DATASOURCE,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::executor::{Control, Lease, SqliteConn};
use crate::{datasource_for, SqliteDataSource};

/// 最外层 SQLite 事务的写锁取得策略。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TransactionMode {
    /// 使用 BEGIN DEFERRED，把读写锁取得延后到实际访问时。
    #[default]
    Deferred,
    /// 使用 BEGIN IMMEDIATE，在进入业务体前取得写事务资格。
    Immediate,
}

type Hook = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

struct HookTask(tokio::task::AbortHandle);

impl Drop for HookTask {
    /// 业务作用：事务调用方取消等待时终止未结束的提交后动作，避免失去预算约束。
    ///
    /// 参数说明：无。
    ///
    /// 返回：只请求取消回调，不改变已经确认的数据库提交。
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Context {
    lease: Arc<Lease>,
    mode: TransactionMode,
    deadline: Instant,
    slot: Arc<Semaphore>,
    hooks: Mutex<Vec<Hook>>,
}

struct NestedScope {
    lease: Arc<Lease>,
    completed: bool,
}

impl Drop for NestedScope {
    /// 业务作用：嵌套业务未正常完成时撤销共享事务的提交权，防止外层吞掉取消或 panic 后提交部分状态。
    ///
    /// 参数说明：无。
    ///
    /// 返回：异常退出或显式回滚将共享事务标记为 rollback-only，不直接结束外层事务。
    fn drop(&mut self) {
        if !self.completed {
            self.lease
                .state()
                .rollback_only
                .store(true, Ordering::Release);
        }
    }
}

impl Drop for Context {
    /// 业务作用：事务体取消或展开时撤销连接复用，确保未完成事务进入原生关闭回滚。
    ///
    /// 参数说明：无。
    ///
    /// 返回：仍在事务中的连接不可再次交给普通查询。
    fn drop(&mut self) {
        if self.lease.state().transaction.load(Ordering::Acquire) {
            self.lease.state().poisoned.store(true, Ordering::Release);
        }
    }
}

tokio::task_local! {
    static CURRENT: Arc<Context>;
}

pub(crate) struct BorrowTicket {
    _permit: OwnedSemaphorePermit,
}

/// 业务作用：检查当前调用栈是否加入 SQLite 环境事务。
///
/// 参数说明：无。
///
/// 返回：仅当前 task 的事务 scope 内返回真，不跨 spawn 传播。
pub fn in_transaction() -> bool {
    CURRENT.try_with(|_| ()).is_ok()
}

/// 业务作用：读取事务固定的数据源名称，供 Mapper 在缓存前复验传播合同。
///
/// 参数说明：无。
///
/// 返回：环境事务的来源名称或空值。
pub fn current_datasource() -> Option<DatasourceRef> {
    CURRENT
        .try_with(|context| context.lease.source.0.name.clone())
        .ok()
}

/// 业务作用：在取得连接或读取缓存前核验同 driver、同来源和事务连续性。
///
/// 参数说明：
/// - `source`：业务请求的来源句柄。
///
/// 返回：当前调用允许访问该来源时成功。
pub fn check_source(source: &SqliteDataSource) -> anyhow::Result<()> {
    natx_core::ensure_driver(DatabaseDriver::Sqlite)?;
    anyhow::ensure!(
        !source.business_expired(),
        "SQLite datasource is isolated or its drain deadline expired"
    );
    if let Ok(context) = CURRENT.try_with(Clone::clone) {
        // 名字相同不能替代 owner 身份；旧句柄不得借新 Application 的事务继续执行。
        anyhow::ensure!(
            context.lease.source.same_source(source),
            "SQLite ambient datasource owner mismatch"
        );
        anyhow::ensure!(
            !context.lease.state().poisoned.load(Ordering::Acquire)
                && !context.lease.state().rollback_only.load(Ordering::Acquire),
            "SQLite transaction cannot continue"
        );
        anyhow::ensure!(
            Instant::now() < context.deadline,
            "SQLite transaction deadline exceeded"
        );
    } else {
        if let Some(registry) = source.0.registry.get() {
            let registry = registry
                .upgrade()
                .ok_or_else(|| anyhow::anyhow!("SQLite datasource registry is unavailable"))?;
            anyhow::ensure!(
                registry.datasource(source.name())?.same_source(source),
                "SQLite datasource registry owner mismatch"
            );
        }
        anyhow::ensure!(
            source.0.accepting.load(Ordering::Acquire),
            "SQLite datasource is stopped"
        );
    }
    Ok(())
}

/// 业务作用：在实际轮询业务执行时复验连接与当前事务身份，禁止预借用或跨任务连接绕过传播合同。
///
/// 参数说明：
/// - `lease`：执行器固定的物理连接借用责任。
/// - `transactional`：连接取得时是否持有环境事务的连接槽。
///
/// 返回：独立连接只能在无环境事务时执行，事务连接必须仍属于当前同一事务。
pub(crate) fn check_lease(lease: &Arc<Lease>, transactional: bool) -> anyhow::Result<()> {
    natx_core::ensure_driver(DatabaseDriver::Sqlite)?;
    if let Ok(context) = CURRENT.try_with(Clone::clone) {
        // 来源相同仍可能是事务开始前借出的 autocommit 连接，必须核对本次物理借用身份。
        anyhow::ensure!(
            transactional && Arc::ptr_eq(&context.lease, lease),
            "SQLite connection does not belong to the current transaction"
        );
    } else {
        // task-local 不随 spawn 传播，转交事务连接不能成为隐式传播通道。
        anyhow::ensure!(
            !transactional,
            "SQLite transaction connection requires its original transaction scope"
        );
    }
    Ok(())
}

/// 业务作用：为默认数据源取得环境事务连接或独立借用。
///
/// 参数说明：无。
///
/// 返回：有界、受控的 SQL 执行句柄。
pub async fn conn() -> anyhow::Result<SqliteConn> {
    conn_for(DEFAULT_DATASOURCE).await
}

/// 业务作用：以名称选择当前事务固定来源或全局目录中的独立来源。
///
/// 参数说明：
/// - `datasource`：业务声明的数据源名称。
///
/// 返回：跨库、跨 driver 或已停机来源立即拒绝。
pub async fn conn_for(datasource: impl AsRef<str>) -> anyhow::Result<SqliteConn> {
    let source = source_for_call(datasource.as_ref())?;
    conn_from(&source).await
}

/// 业务作用：按拥有型来源借用连接，使 client 可以固定来源生命周期。
///
/// 参数说明：
/// - `source`：业务已经固定的数据源句柄。
///
/// 返回：事务内串行借用唯一连接，事务外借用来源池。
pub async fn conn_from(source: &SqliteDataSource) -> anyhow::Result<SqliteConn> {
    conn_from_deadline(source, None).await
}

/// 业务作用：在独立借用接纳前统一请求与配置期限，环境事务只收敛本次句柄的执行期限。
///
/// 参数说明：
/// - `source`：已固定身份和生命周期的数据源。
/// - `request_deadline`：上游请求的绝对截止点；None 表示仅使用来源配置或环境事务期限。
///
/// 返回：独立租约和执行句柄共享同一截止点；环境事务保持原有总期限与串行借用约束。
async fn conn_from_deadline(
    source: &SqliteDataSource,
    request_deadline: Option<Instant>,
) -> anyhow::Result<SqliteConn> {
    check_source(source)?;
    if let Ok(context) = CURRENT.try_with(Clone::clone) {
        let permit = context
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow::anyhow!("SQLite transaction connection is already held"))?;
        check_source(source)?;
        // 环境事务已经拥有原生租约；局部读取预算不能改写整个事务的存活与清理期限。
        return Ok(SqliteConn {
            lease: context.lease.clone(),
            deadline: request_deadline.map_or(context.deadline, |end| end.min(context.deadline)),
            ticket: Some(Arc::new(BorrowTicket {
                _permit: permit,
            })),
        });
    }
    let deadline = Instant::now() + Duration::from_millis(source.0.config.operation_timeout_ms);
    // 接纳前把共同截止点交给选池和原生监督，首次 SQL 前的持有及取连接等待都消耗同一预算。
    let deadline = request_deadline.map_or(deadline, |end| end.min(deadline));
    Ok(SqliteConn {
        lease: source.acquire(deadline).await?,
        deadline,
        ticket: None,
    })
}

/// 业务作用：强制写路径必须加入当前 SQLite 事务。
///
/// 参数说明：
/// - `datasource`：预期事务来源。
///
/// 返回：没有环境事务时失败，避免 spawn 后静默改为独立提交。
pub async fn mandatory_conn_for(datasource: impl AsRef<str>) -> anyhow::Result<SqliteConn> {
    anyhow::ensure!(in_transaction(), "SQLite transaction is required");
    conn_for(datasource).await
}

/// 业务作用：确保自治操作不加入任何 driver 的环境事务。
///
/// 参数说明：
/// - `datasource`：独立操作来源。
///
/// 返回：无环境事务才发放连接。
pub async fn never_conn_for(datasource: impl AsRef<str>) -> anyhow::Result<SqliteConn> {
    anyhow::ensure!(
        natx_core::current_driver().is_none(),
        "ambient transaction is forbidden"
    );
    conn_for(datasource).await
}

/// 业务作用：解析当前调用的固定来源，已接纳事务在停机排空期间仍可收口。
///
/// 参数说明：
/// - `name`：本次请求的 datasource。
///
/// 返回：同名环境事务来源或经过共享目录校验的来源。
pub fn source_for_call(name: &str) -> anyhow::Result<SqliteDataSource> {
    natx_core::ensure_driver(DatabaseDriver::Sqlite)?;
    let reference = DatasourceRef::new(name)?;
    if let Ok(context) = CURRENT.try_with(Clone::clone) {
        anyhow::ensure!(
            context.lease.source.name() == reference.as_str(),
            "SQLite ambient datasource mismatch"
        );
        return Ok(context.lease.source.clone());
    }
    datasource_for(reference.as_str()).map_err(Into::into)
}

/// 业务作用：按默认来源执行成功提交、失败回滚的本地事务。
///
/// 参数说明：
/// - `body`：需要共享本地事务的业务 future。
///
/// 返回：提交确认后的业务结果或原业务错误。
pub async fn run<T, F: Future<Output = anyhow::Result<T>>>(body: F) -> anyhow::Result<T> {
    run_for(DEFAULT_DATASOURCE, body).await
}

/// 业务作用：按名称执行本地事务，未指定模式的嵌套调用继承外层策略。
///
/// 参数说明：
/// - `datasource`：事务数据源名称。
/// - `body`：只执行一次的业务 future。
///
/// 返回：确认提交的业务结果或事务失败。
pub async fn run_for<T, F: Future<Output = anyhow::Result<T>>>(
    datasource: impl AsRef<str>,
    body: F,
) -> anyhow::Result<T> {
    run_for_mode(datasource, None, body).await
}

/// 业务作用：在外层选择事务模式，内层显式冲突时在执行体之前拒绝。
///
/// 参数说明：
/// - `datasource`：事务来源名称。
/// - `mode`：显式锁策略，None 表示外层 Deferred 或继承当前策略。
/// - `body`：成功提交、失败回滚的业务 future。
///
/// 返回：仅物理提交确认才返回成功。
pub async fn run_for_mode<T, F: Future<Output = anyhow::Result<T>>>(
    datasource: impl AsRef<str>,
    mode: Option<TransactionMode>,
    body: F,
) -> anyhow::Result<T> {
    let source = source_for_call(datasource.as_ref())?;
    let decision = async {
        match body.await {
            Ok(value) => TxDecision::Commit(value),
            Err(error) => TxDecision::Rollback(error),
        }
    };
    match run_decided_from(&source, mode, decision).await {
        Ok(value) => Ok(value),
        Err(TxRunError::Rollback(error)) => Err(error),
        Err(error) => Err(anyhow::anyhow!(error.to_string())),
    }
}

/// 业务作用：禁止自治业务进入任何 driver 的环境事务。
///
/// 参数说明：
/// - `body`：不允许被事务传播包装的业务 future。
///
/// 返回：没有环境事务时执行一次，否则拒绝执行体。
pub async fn run_never<T, F: Future<Output = anyhow::Result<T>>>(body: F) -> anyhow::Result<T> {
    anyhow::ensure!(
        natx_core::current_driver().is_none(),
        "ambient transaction is forbidden"
    );
    body.await
}

/// 业务作用：在指定来源保留入口错误和事务裁决错误的结构化边界。
///
/// 参数说明：
/// - `datasource`：业务数据源名称。
/// - `body`：显式提交或回滚裁决。
///
/// 返回：Lookup 表示尚未开始事务，Run 表示事务执行阶段失败。
pub async fn run_decided_for_checked<T, E, F: Future<Output = TxDecision<T, E>>>(
    datasource: impl AsRef<str>,
    body: F,
) -> Result<T, TxEntryError<E>> {
    let source = if let Ok(context) = CURRENT.try_with(Clone::clone) {
        if context.lease.source.name() != datasource.as_ref() {
            return Err(TxEntryError::Run(infrastructure(
                "SQLite ambient datasource mismatch",
            )));
        }
        context.lease.source.clone()
    } else {
        datasource_for(datasource.as_ref()).map_err(TxEntryError::Lookup)?
    };
    run_decided_from(&source, None, body)
        .await
        .map_err(TxEntryError::Run)
}

/// 业务作用：对固定来源执行显式裁决，不自动重放业务 future。
///
/// 参数说明：
/// - `source`：固定 owner 的数据源。
/// - `mode`：外层锁策略或内层继承。
/// - `body`：返回提交或回滚裁决的业务 future。
///
/// 返回：提交、回滚及不确定结果按 natx-core 合同区分；提交后回调超期或失败不撤销已确认提交。
pub async fn run_decided_from<T, E, F: Future<Output = TxDecision<T, E>>>(
    source: &SqliteDataSource,
    mode: Option<TransactionMode>,
    body: F,
) -> Result<T, TxRunError<E>> {
    check_source(source).map_err(|_| infrastructure("SQLite transaction entry rejected"))?;
    if let Ok(context) = CURRENT.try_with(Clone::clone) {
        if mode.is_some_and(|mode| mode != context.mode) {
            return Err(infrastructure("SQLite nested transaction mode mismatch"));
        }
        // 从首次进入内层执行起保留退出责任；future 被丢弃或 unwind 时也必须撤销提交权。
        let mut scope = NestedScope {
            lease: context.lease.clone(),
            completed: false,
        };
        return match body.await {
            TxDecision::Commit(value) => {
                scope.completed = true;
                Ok(value)
            }
            TxDecision::Rollback(error) => Err(TxRunError::Rollback(error)),
        };
    }
    let mode = mode.unwrap_or_default();
    if source.is_read_only() && mode == TransactionMode::Immediate {
        return Err(infrastructure(
            "SQLite read-only datasource cannot begin Immediate",
        ));
    }
    let deadline = Instant::now() + Duration::from_millis(source.0.config.transaction_timeout_ms);
    let lease = source
        .acquire(deadline)
        .await
        .map_err(|_| infrastructure("SQLite transaction acquisition failed"))?;
    lease
        .control(Control::Begin(mode), deadline)
        .await
        .map_err(|_| infrastructure("SQLite transaction begin failed"))?;
    let context = Arc::new(Context {
        lease,
        mode,
        deadline,
        slot: Arc::new(Semaphore::new(1)),
        hooks: Mutex::new(Vec::new()),
    });
    let decision = natx_core::scope_driver(
        DatabaseDriver::Sqlite,
        CURRENT.scope(context.clone(), tokio::time::timeout_at(deadline, body)),
    )
    .await
    .map_err(|_| infrastructure("SQLite transaction scope rejected"))?;
    // 提交之前必须归还连接槽；逃逸句柄不得在事务结束后继续访问原来的连接。
    if context.slot.available_permits() != 1 {
        context
            .lease
            .state()
            .poisoned
            .store(true, Ordering::Release);
        return Err(infrastructure(
            "SQLite transaction connection is still held",
        ));
    }
    let cleanup_deadline = Instant::now() + Duration::from_secs(5);
    let decision = match decision {
        Ok(decision) => decision,
        Err(_) => {
            let _ = context
                .lease
                .control(Control::Rollback, cleanup_deadline)
                .await;
            return Err(infrastructure("SQLite transaction deadline exceeded"));
        }
    };
    match decision {
        TxDecision::Commit(_)
            if context.lease.state().rollback_only.load(Ordering::Acquire)
                || context.lease.state().poisoned.load(Ordering::Acquire) =>
        {
            let reason = "SQLite transaction is rollback-only".to_owned();
            match context
                .lease
                .control(Control::Rollback, cleanup_deadline)
                .await
            {
                Ok(()) => Err(TxRunError::RollbackOnly {
                    reason,
                }),
                Err(_) => Err(TxRunError::RollbackFailed {
                    cause: TxRollbackCause::RollbackOnly {
                        reason,
                    },
                    reason: "SQLite rollback failed".to_owned(),
                }),
            }
        }
        TxDecision::Commit(value) => {
            match context.lease.control(Control::Commit, deadline).await {
                Ok(()) => {
                    let hooks = std::mem::take(
                        &mut *context
                            .hooks
                            .lock()
                            .unwrap_or_else(|error| error.into_inner()),
                    );
                    // 已确认提交后先释放连接与借用容量，回调才可在单连接池中读取提交事实。
                    drop(context);
                    let hooks_deadline = Instant::now() + Duration::from_secs(5);
                    for hook in hooks {
                        // 共享期限耗尽后不再创建任务，避免取消请求晚于新的外部动作启动。
                        if Instant::now() >= hooks_deadline {
                            break;
                        }
                        let task = tokio::spawn(async move {
                            // 排队也消耗同一预算；真正调用业务闭包之前再次复验，不按回调续期。
                            if Instant::now() < hooks_deadline {
                                hook().await;
                            }
                        });
                        let abort = HookTask(task.abort_handle());
                        if tokio::time::timeout_at(hooks_deadline, task).await.is_err() {
                            abort.0.abort();
                            // 等待超时即停止整个回调序列，数据库已提交的事实仍由成功结果保留。
                            break;
                        }
                    }
                    Ok(value)
                }
                Err(error) => {
                    source.0.cache_safe.store(false, Ordering::Release);
                    // 只有 worker 返回真实数据库错误且事务仍在时才能确认拒绝；等待超时不能猜测提交事实。
                    if matches!(
                        error.downcast_ref::<sqlx::Error>(),
                        Some(sqlx::Error::Database(_))
                    ) && context.lease.state().transaction.load(Ordering::Acquire)
                    {
                        let rollback = context
                            .lease
                            .control(Control::Rollback, cleanup_deadline)
                            .await;
                        if rollback.is_ok() {
                            Err(TxRunError::CommitRejected {
                                reason:
                                    "SQLite rejected transaction commit and rollback was confirmed"
                                        .to_owned(),
                            })
                        } else {
                            Err(TxRunError::CommitUncertain {
                                reason: "SQLite commit was rejected but rollback confirmation is unavailable".to_owned(),
                            })
                        }
                    } else {
                        Err(TxRunError::CommitUncertain {
                            reason: "SQLite commit outcome is uncertain".to_owned(),
                        })
                    }
                }
            }
        }
        TxDecision::Rollback(error) => match context
            .lease
            .control(Control::Rollback, cleanup_deadline)
            .await
        {
            Ok(()) => Err(TxRunError::Rollback(error)),
            Err(_) => Err(TxRunError::RollbackFailed {
                cause: TxRollbackCause::Decision(error),
                reason: "SQLite rollback failed".to_owned(),
            }),
        },
    }
}

/// 业务作用：登记只有提交确认后才能触发的外部动作。
///
/// 参数说明：
/// - `hook`：提交后执行一次的有界异步动作。
///
/// 返回：事务内且数量不超过 256 时成功，否则不登记；仅确认提交后按登记顺序执行，全部回调共享 5 秒预算，超期不再启动后续动作。
pub fn after_commit<F, Fut>(hook: F) -> anyhow::Result<()>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let context = CURRENT
        .try_with(Clone::clone)
        .map_err(|_| anyhow::anyhow!("SQLite after_commit requires a transaction"))?;
    let mut hooks = context
        .hooks
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    anyhow::ensure!(hooks.len() < 256, "SQLite after_commit hook limit exceeded");
    hooks.push(Box::new(move || Box::pin(hook())));
    Ok(())
}

/// 业务作用：统一生成不含 SQL、路径和业务值的事务入口错误。
///
/// 参数说明：
/// - `reason`：固定错误分类文本。
///
/// 返回：可由调用方结构化处理的基础设施失败。
fn infrastructure<E>(reason: &str) -> TxRunError<E> {
    TxRunError::Infrastructure {
        reason: reason.to_owned(),
    }
}

/// 业务作用：在默认数据源执行显式提交或回滚裁决。
///
/// 参数说明：
/// - `body`：只执行一次的业务裁决 future。
///
/// 返回：保留提交拒绝、提交未确认和回滚失败的结构化区别。
pub async fn run_decided<T, E, F: Future<Output = TxDecision<T, E>>>(
    body: F,
) -> Result<T, TxRunError<E>> {
    run_decided_for(DEFAULT_DATASOURCE, body).await
}

/// 业务作用：在默认数据源保留查找阶段与执行阶段的错误边界。
///
/// 参数说明：
/// - `body`：业务裁决 future。
///
/// 返回：开始事务前的错误为 Lookup，其余错误为 Run。
pub async fn run_decided_checked<T, E, F: Future<Output = TxDecision<T, E>>>(
    body: F,
) -> Result<T, TxEntryError<E>> {
    run_decided_for_checked(DEFAULT_DATASOURCE, body).await
}

/// 业务作用：按名称执行显式事务裁决，入口失败归类为基础设施失败。
///
/// 参数说明：
/// - `datasource`：本地事务唯一来源。
/// - `body`：只执行一次的业务裁决 future。
///
/// 返回：确认提交的业务结果或结构化事务错误。
pub async fn run_decided_for<T, E, F: Future<Output = TxDecision<T, E>>>(
    datasource: impl AsRef<str>,
    body: F,
) -> Result<T, TxRunError<E>> {
    let source = source_for_call(datasource.as_ref())
        .map_err(|_| infrastructure("SQLite datasource lookup failed"))?;
    run_decided_from(&source, None, body).await
}

/// 业务作用：让连接等待、独立租约监督和后续语句共享调用链剩余期限。
///
/// 参数说明：
/// - `datasource`：业务声明的来源名称。
/// - `budget`：调用链取消信号与截止点。
///
/// 返回：借用失败或预算结束时拒绝；独立租约受请求与配置的较短期限约束，环境事务仅收敛本次句柄期限。
pub async fn conn_for_budget(
    datasource: impl AsRef<str>,
    budget: &nabudget::RequestBudget,
) -> anyhow::Result<SqliteConn> {
    budget
        .run(async {
            let source = source_for_call(datasource.as_ref())?;
            conn_from_deadline(&source, Some(budget.deadline())).await
        })
        .await?
}

/// 业务作用：约束完整读取等待并在取消时让受控 worker 停止原生执行。
///
/// 参数说明：
/// - `budget`：读取共享的调用链预算。
/// - `read`：业务确认的读取 future，SQL 权限仍由 executor 检查。
///
/// 返回：读取结果或预算错误；不自动重放查询或推断已发出写入的结果。
pub async fn read_with_budget<T>(
    budget: &nabudget::RequestBudget,
    read: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    budget.run(read).await?
}
