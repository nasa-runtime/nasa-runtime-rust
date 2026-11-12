use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use futures_core::{future::BoxFuture, stream::BoxStream, Stream};
use futures_util::TryStreamExt;
use sqlx::sqlite::{
    SqliteArguments, SqliteConnectOptions, SqliteQueryResult, SqliteRow, SqliteStatement,
    SqliteTransactionManager, SqliteTypeInfo,
};
use sqlx::ConnectOptions;
use sqlx::{
    Arguments, Connection, Either, Execute, Executor, Row, SqlStr, Sqlite, SqliteConnection,
    TypeInfo, ValueRef,
};
use sqlx_core::transaction::TransactionManager;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit};
use tokio::time::Instant;

use crate::datasource::{JournalMode, SqliteDataSource, Synchronous};
use crate::native::{self, protocol, ExecutionPolicy, NativeState};
use crate::ownership::PhysicalLifetime;

type Step = Result<Either<SqliteQueryResult, SqliteRow>, sqlx::Error>;

pub(crate) struct Actor {
    sender: mpsc::Sender<Command>,
    pub(crate) state: Arc<NativeState>,
}

struct WorkerScope {
    source: SqliteDataSource,
    completed: bool,
}

impl Drop for WorkerScope {
    /// 业务作用：执行单元异常退出时撤销整个来源的业务权，禁止重建连接掩盖执行收据丢失。
    ///
    /// 参数说明：无。
    ///
    /// 返回：正常关闭不改变来源健康，异常展开保持隔离及文件保活责任。
    fn drop(&mut self) {
        if !self.completed {
            self.source.isolate(crate::IsolationReason::Worker);
        }
    }
}

impl Actor {
    /// 业务作用：只复用没有原生事务或不确定状态的物理连接。
    ///
    /// 参数说明：无。
    ///
    /// 返回：可作为下一个根调用的连接时为真。
    pub(crate) fn usable(&self) -> bool {
        !self.sender.is_closed()
            && !self.state.poisoned.load(Ordering::Acquire)
            && !self.state.rollback_only.load(Ordering::Acquire)
            && !self.state.transaction.load(Ordering::Acquire)
            && self
                .state
                .maintenance
                .lock()
                .is_ok_and(|gate| gate.is_none())
    }
}

pub(crate) struct Lease {
    pub(crate) source: SqliteDataSource,
    actor: Option<Actor>,
    read_id: std::sync::Mutex<Option<u64>>,
}

impl Lease {
    /// 业务作用：保留同文件维护门和连接，直到 worker 确认原生动作已经结束。
    ///
    /// 参数说明：
    /// - `operation`：固定维护动作。
    /// - `gate`：本次同文件维护的唯一许可。
    /// - `deadline`：不延长的总截止点。
    ///
    /// 返回：维护结果；取消只撤销等待，不提前释放原生责任。
    pub(crate) async fn maintain(
        self: &Arc<Self>,
        operation: crate::maintenance::Operation,
        gate: tokio::sync::OwnedMutexGuard<()>,
        deadline: Instant,
    ) -> anyhow::Result<crate::maintenance::ResultValue> {
        let (reply, response) = oneshot::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let _guard = Cancellation(cancel.clone());
        let command = Command::Maintenance {
            operation,
            deadline,
            cancel,
            reply,
            _gate: gate,
            _lease: self.clone(),
        };
        tokio::time::timeout_at(
            deadline,
            self.actor
                .as_ref()
                .expect("active SQLite lease")
                .sender
                .send(command),
        )
        .await?
        .map_err(|_| protocol("SQLite worker is closed"))?;
        tokio::time::timeout_at(deadline, response).await??
    }

    /// 业务作用：将调用期限、借用容量、来源身份和实际连接交接到同一生命周期。
    ///
    /// 参数说明：
    /// - `source`：受管来源。
    /// - `actor`：独占执行连接。
    /// - `permit`：本次借用的容量凭证。
    /// - `deadline`：调用链的固定截止点，包含借出后首次 SQL 之前的等待。
    ///
    /// 返回：可接纳时返回借用责任；关闭已决定或状态失效时交还凭证供选择其他候选。
    pub(crate) fn new(
        source: SqliteDataSource,
        actor: Actor,
        permit: OwnedSemaphorePermit,
        deadline: Instant,
    ) -> Result<Self, OwnedSemaphorePermit> {
        if !actor.usable() {
            return Err(permit);
        }
        actor.state.begin_lease(deadline.into_std(), permit)?;
        Ok(Self {
            source,
            actor: Some(actor),
            read_id: std::sync::Mutex::new(None),
        })
    }

    /// 业务作用：读取借用责任所固定的原生执行状态。
    ///
    /// 参数说明：无。
    ///
    /// 返回：连接尚未回池期间始终存在的状态。
    pub(crate) fn state(&self) -> &Arc<NativeState> {
        &self.actor.as_ref().expect("active SQLite lease").state
    }

    /// 业务作用：提交框架事务动作，调用取消时标记连接不可复用。
    ///
    /// 参数说明：
    /// - `action`：开始、提交或回滚动作。
    /// - `deadline`：动作允许的最晚完成时间。
    ///
    /// 返回：原生 worker 确认后的结果。
    pub(crate) async fn control(
        self: &Arc<Self>,
        action: Control,
        deadline: Instant,
    ) -> anyhow::Result<()> {
        let (reply, response) = oneshot::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let _guard = Cancellation(cancel.clone());
        let command = Command::Control {
            action,
            deadline,
            cancel,
            reply,
            _lease: self.clone(),
        };
        tokio::time::timeout_at(
            deadline,
            self.actor
                .as_ref()
                .expect("active SQLite lease")
                .sender
                .send(command),
        )
        .await?
        .map_err(|_| protocol("SQLite worker is closed"))?;
        tokio::time::timeout_at(deadline, response)
            .await??
            .map_err(Into::into)
    }
}

impl Drop for Lease {
    /// 业务作用：仅把确定可复用且仍接纳业务的连接交还池，其他连接转入物理关闭。
    ///
    /// 参数说明：无。
    ///
    /// 返回：活动事务与不确定写入不会泄漏给下一位调用者。
    fn drop(&mut self) {
        if let Some(id) = self
            .read_id
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            self.source
                .0
                .health
                .reads
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .remove(&id);
        }
        let actor = self.actor.take().expect("active SQLite lease");
        let mut idle = self
            .source
            .0
            .idle
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if (self.source.0.accepting.load(Ordering::Acquire)
            || self.source.0.draining.load(Ordering::Acquire))
            && self.source.isolation_reason().is_none()
            && actor.usable()
        {
            let capacity = actor.state.capacity.clone();
            idle.push(actor);
            capacity.release();
        }
        self.source.0.file.changed.notify_waiters();
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Control {
    Begin(crate::TransactionMode),
    BeginMigration {
        mode: crate::TransactionMode,
        execution_timeout: Duration,
    },
    Commit,
    Rollback,
}

enum Command {
    Maintenance {
        operation: crate::maintenance::Operation,
        deadline: Instant,
        cancel: Arc<AtomicBool>,
        reply: oneshot::Sender<anyhow::Result<crate::maintenance::ResultValue>>,
        _gate: tokio::sync::OwnedMutexGuard<()>,
        _lease: Arc<Lease>,
    },
    Validate {
        sql: String,
        deadline: Instant,
        cancel: Arc<AtomicBool>,
        reply: oneshot::Sender<Result<(), sqlx::Error>>,
        _lease: Arc<Lease>,
        _ticket: Option<Arc<crate::transaction::BorrowTicket>>,
    },
    Query {
        sql: SqlStr,
        arguments: SqliteArguments,
        policy: ExecutionPolicy,
        deadline: Instant,
        cancel: Arc<AtomicBool>,
        output: mpsc::Sender<Option<Step>>,
        budget: Option<crate::SqliteStreamBudget>,
        _lease: Arc<Lease>,
        _ticket: Option<Arc<crate::transaction::BorrowTicket>>,
    },
    Control {
        action: Control,
        deadline: Instant,
        cancel: Arc<AtomicBool>,
        reply: oneshot::Sender<Result<(), sqlx::Error>>,
        _lease: Arc<Lease>,
    },
}

struct Cancellation(Arc<AtomicBool>);

impl Drop for Cancellation {
    /// 业务作用：调用方离开后通知原生执行停止，连接责任由 worker 继续保留。
    ///
    /// 参数说明：无。
    ///
    /// 返回：后续 VM 和 busy 回调拒绝继续消耗预算。
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// 只开放受控执行能力，不实现 Deref 或暴露底层 SqliteConnection。
pub struct SqliteConn {
    pub(crate) lease: Arc<Lease>,
    pub(crate) deadline: Instant,
    pub(crate) ticket: Option<Arc<crate::transaction::BorrowTicket>>,
}

impl SqliteConn {
    /// 业务作用：在流交付给业务之前登记消费期限，覆盖从未开始轮询的拥有型流。
    ///
    /// 参数说明：无。
    ///
    /// 返回：与底层执行器共享、由原生执行单元监督的消费预算。
    pub fn stream_budget(&self) -> crate::SqliteStreamBudget {
        crate::SqliteStreamBudget::new(
            self.lease.state().clone(),
            Duration::from_millis(self.lease.source.0.config.stream_idle_timeout_ms),
        )
    }

    /// 业务作用：向固定框架流程授予对应阶段的权限，不向业务暴露原始连接。
    ///
    /// 参数说明：
    /// - `policy`：迁移脚本、历史操作或维护阶段。
    ///
    /// 返回：仅本 crate 可构造的阶段执行器。
    pub(crate) fn framework_executor(&mut self, policy: ExecutionPolicy) -> SqliteExecutor<'_> {
        SqliteExecutor {
            connection: self,
            policy,
        }
    }

    /// 业务作用：缓存命中前核验最终 SQL 的只读性、单语句和文件身份。
    ///
    /// 参数说明：
    /// - `sql`：已完成动态分支和列表展开的 SQL。
    ///
    /// 返回：原生 prepare 确认可读取才成功，不执行查询。
    pub async fn validate_read(&mut self, sql: &str) -> anyhow::Result<()> {
        let lease = self.lease.clone();
        let transactional = self.ticket.is_some();
        let operation = self.validate_read_inner(sql);
        tokio::pin!(operation);
        // future 可在创建后才进入事务或转交另一 task，每次 poll 都要复验实际调用上下文。
        futures_util::future::poll_fn(|context| {
            if let Err(error) = crate::transaction::check_lease(&lease, transactional) {
                return Poll::Ready(Err(error));
            }
            operation.as_mut().poll(context)
        })
        .await
    }

    /// 业务作用：把已通过调用上下文门禁的只读验证交给原生 worker，取消时保留 worker 清理责任。
    ///
    /// 参数说明：
    /// - `sql`：待确认只读性与单语句边界的最终 SQL。
    ///
    /// 返回：worker 完成 prepare 验证后成功，失败或取消不执行查询。
    async fn validate_read_inner(&mut self, sql: &str) -> anyhow::Result<()> {
        crate::transaction::check_source(&self.lease.source)?;
        let (reply, response) = oneshot::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let _guard = Cancellation(cancel.clone());
        let command = Command::Validate {
            sql: sql.to_owned(),
            deadline: self.deadline,
            cancel,
            reply,
            _lease: self.lease.clone(),
            _ticket: self.ticket.clone(),
        };
        tokio::time::timeout_at(
            self.deadline,
            self.lease
                .actor
                .as_ref()
                .expect("active SQLite lease")
                .sender
                .send(command),
        )
        .await?
        .map_err(|_| protocol("SQLite worker is closed"))?;
        tokio::time::timeout_at(self.deadline, response).await???;
        Ok(())
    }

    /// 业务作用：为业务 SQL 选择只读或显式写入合同。
    ///
    /// 参数说明：
    /// - `write`：调用方是否声明本次 SQL 可产生数据库写入。
    ///
    /// 返回：仍执行单语句、权限、预算和连接身份门禁的 SQLx executor。
    pub fn executor(&mut self, write: bool) -> SqliteExecutor<'_> {
        SqliteExecutor {
            connection: self,
            policy: if write {
                ExecutionPolicy::Write
            } else {
                ExecutionPolicy::Read
            },
        }
    }

    /// 业务作用：把写后映射失败传播到事务或数据源缓存隔离状态。
    ///
    /// 参数说明：无。
    ///
    /// 返回：显式事务只允许回滚，独立写入停止复用旧缓存。
    pub fn mark_write_failed(&self) {
        let state = self.lease.state();
        if state.transaction.load(Ordering::Acquire) {
            // 活动事务中的映射失败撤销提交权，调用方吞错也不能提交部分业务结果。
            state.rollback_only.store(true, Ordering::Release);
        } else {
            // 独立写入可能已提交；丢弃该连接而不把仅回滚状态交给后续独立调用。
            state.poisoned.store(true, Ordering::Release);
        }
        self.lease
            .source
            .0
            .cache_safe
            .store(false, Ordering::Release);
    }
}

/// 受控 SQLx 执行器的短期借用。
pub struct SqliteExecutor<'a> {
    connection: &'a mut SqliteConn,
    pub(crate) policy: ExecutionPolicy,
}

impl fmt::Debug for SqliteExecutor<'_> {
    /// 业务作用：提供不含 SQL、文件路径和绑定值的执行器诊断身份。
    ///
    /// 参数说明：
    /// - `formatter`：诊断输出目标。
    ///
    /// 返回：仅输出类型和数据源名称。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteExecutor")
            .field("datasource", &self.connection.lease.source.name())
            .finish()
    }
}

impl<'c> Executor<'c> for SqliteExecutor<'c> {
    type Database = Sqlite;

    /// 业务作用：把查询转交给有界 worker，并在取消后保留真实执行责任。
    ///
    /// 参数说明：
    /// - `query`：SQLx 已编码参数的业务查询。
    ///
    /// 返回：只有收到 worker 最终确认才正常结束的有界结果流。
    fn fetch_many<'e, 'q: 'e, E>(self, mut query: E) -> BoxStream<'e, Step>
    where
        'c: 'e,
        E: 'q + Execute<'q, Sqlite>,
    {
        let lease = self.connection.lease.clone();
        let transactional = self.connection.ticket.is_some();
        let business = matches!(self.policy, ExecutionPolicy::Read | ExecutionPolicy::Write);
        let budget = business.then(|| self.connection.stream_budget());
        let query_budget = budget.clone();
        let mut stream = Some(Box::pin(async_stream::try_stream! {
            if business {
                crate::transaction::check_source(&self.connection.lease.source)
                    .map_err(|error| protocol(&error.to_string()))?;
            }
            let arguments = query.take_arguments().map_err(sqlx::Error::Encode)?.unwrap_or_default();
            let sql = query.sql();
            let lease = self.connection.lease.clone();
            let deadline = if matches!(self.policy, ExecutionPolicy::Read | ExecutionPolicy::Write) {
                self.connection.deadline.min(Instant::now() + Duration::from_millis(lease.source.0.config.operation_timeout_ms))
            } else {
                self.connection.deadline
            };
            if let Some(budget) = &query_budget {
                budget.start_operation(deadline.into_std());
            }
            let cancel = Arc::new(AtomicBool::new(false));
            let _guard = Cancellation(cancel.clone());
            let (output, mut results) = mpsc::channel(8);
            let command = Command::Query {
                sql,
                arguments,
                policy: self.policy,
                deadline,
                cancel,
                output,
                budget: query_budget.clone(),
                _lease: lease.clone(),
                _ticket: self.connection.ticket.clone(),
            };
            tokio::time::timeout_at(
                deadline,
                lease.actor.as_ref().expect("active SQLite lease").sender.send(command),
            )
            .await
            .map_err(|_| protocol("SQLite operation deadline exceeded"))?
            .map_err(|_| protocol("SQLite worker is closed"))?;
            loop {
                if let Some(budget) = &query_budget {
                    budget.check_delivery()?;
                }
                match tokio::time::timeout_at(deadline, results.recv()).await.map_err(|_| {
                    query_budget.as_ref().and_then(|budget| budget.take_failure())
                        .unwrap_or_else(|| protocol("SQLite operation deadline exceeded"))
                })? {
                    Some(Some(step)) => yield step?,
                    Some(None) => {
                        if let Some(error) = query_budget.as_ref().and_then(|budget| budget.take_failure()) {
                            Err(error)?;
                        }
                        break;
                    }
                    None => Err(query_budget.as_ref().and_then(|budget| budget.take_failure())
                        .unwrap_or_else(|| protocol("SQLite worker ended without completion")))?,
                }
            }
        }));
        Box::pin(futures_util::stream::poll_fn(move |context| {
            if stream.is_none() {
                return Poll::Ready(None);
            }
            if let Some(budget) = &budget {
                if let Err(error) = budget.poll_started() {
                    budget.finish();
                    stream = None;
                    return Poll::Ready(Some(Err(error)));
                }
            }
            // 每次轮询复验身份；执行失败后仍需交付 worker 的原始错误，不能用 rollback-only 覆盖它。
            if business {
                if let Err(error) = crate::transaction::check_lease(&lease, transactional) {
                    if let Some(budget) = &budget {
                        budget.finish();
                    }
                    stream = None;
                    return Poll::Ready(Some(Err(protocol(&error.to_string()))));
                }
            }
            let mut result = stream
                .as_mut()
                .expect("active SQLite stream")
                .as_mut()
                .poll_next(context);
            if let Some(budget) = &budget {
                // recv 已有数据时 timeout 可以立即成功，必须在正常交付前显式复验，真实错误则原样保留。
                if matches!(result, Poll::Ready(Some(Ok(_))) | Poll::Ready(None)) {
                    if let Err(error) = budget.check_delivery() {
                        result = Poll::Ready(Some(Err(error)));
                    }
                }
                match &result {
                    Poll::Ready(Some(Ok(_))) => budget.row_ready(),
                    Poll::Ready(_) => budget.finish(),
                    Poll::Pending => {}
                }
            }
            if matches!(result, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
                stream = None;
            }
            result
        }))
    }

    /// 业务作用：读取首行同时耗尽语句，确保 RETURNING 的数据库动作完成后才返回。
    ///
    /// 参数说明：
    /// - `query`：已声明执行合同的查询。
    ///
    /// 返回：首行或空值；后续行或提交阶段失败仍向调用方传播。
    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<SqliteRow>, sqlx::Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Sqlite>,
    {
        Box::pin(async move {
            let mut stream = self.fetch_many(query);
            let mut first = None;
            while let Some(step) = stream.try_next().await? {
                if let Either::Right(row) = step {
                    if first.is_none() {
                        first = Some(row);
                    }
                }
            }
            Ok(first)
        })
    }

    /// 业务作用：拒绝脱离受控执行过程的持久 statement，避免权限阶段变化后的缓存复用。
    ///
    /// 参数说明：
    /// - `_sql`：外部请求准备的语句。
    /// - `_parameters`：外部传入的参数提示。
    ///
    /// 返回：调用方应直接使用 query、query_as 或 query_scalar 的明确错误。
    fn prepare_with<'e>(
        self,
        _sql: SqlStr,
        _parameters: &'e [SqliteTypeInfo],
    ) -> BoxFuture<'e, Result<SqliteStatement, sqlx::Error>>
    where
        'c: 'e,
    {
        Box::pin(async {
            Err(protocol(
                "SQLite controlled executor does not expose prepared statements",
            ))
        })
    }

    /// 业务作用：拒绝运行期推断 SQL 类型时绕过业务单语句权限边界。
    ///
    /// 参数说明：
    /// - `_sql`：外部请求推断的 SQL。
    ///
    /// 返回：明确不支持离线宏描述入口。
    fn describe<'e>(
        self,
        _sql: SqlStr,
    ) -> BoxFuture<'e, Result<sqlx::Describe<Sqlite>, sqlx::Error>>
    where
        'c: 'e,
    {
        Box::pin(async {
            Err(protocol(
                "SQLite controlled executor does not expose describe",
            ))
        })
    }
}

/// 业务作用：在不可被 async 取消提前释放的阻塞域内打开并持续驱动物理连接。
///
/// 参数说明：
/// - `source`：已持有文件权威的数据源。
/// - `created`：本次启动是否创建主文件。
/// - `deadline`：建立连接的绝对截止点。
///
/// 返回：校验完成才发布的 actor；等待取消不撤销原生关闭责任。
pub(crate) async fn connect_actor(
    source: &SqliteDataSource,
    created: bool,
    deadline: Instant,
) -> anyhow::Result<Actor> {
    let permit =
        tokio::time::timeout_at(deadline, source.0.physical.clone().acquire_owned()).await??;
    let lifetime = PhysicalLifetime::new(source.0.file.clone(), permit)?;
    let source = source.clone();
    let source_for_states = source.clone();
    let (sender, mut receiver) = mpsc::channel(1);
    let (ready, response) = oneshot::channel();
    tokio::task::spawn_blocking(move || {
        let mut scope = WorkerScope {
            source: source.clone(),
            completed: false,
        };
        let opened = futures_executor::block_on(open_connection(
            &source,
            created,
            lifetime.clone(),
            deadline,
        ));
        let (mut connection, state) = match opened {
            Ok(opened) => opened,
            Err(error) => {
                scope.completed = true;
                if let Some(sqlx_error) = error.downcast_ref::<sqlx::Error>() {
                    source.0.diagnostics.record_error(sqlx_error);
                    match crate::classify_sqlite_error(sqlx_error) {
                        crate::SqliteErrorCategory::Corrupt => {
                            source.isolate(crate::IsolationReason::Corrupt)
                        }
                        crate::SqliteErrorCategory::Busy
                        | crate::SqliteErrorCategory::Locked
                        | crate::SqliteErrorCategory::BusySnapshot => {}
                        _ => source.isolate(crate::IsolationReason::Policy),
                    }
                } else {
                    source.isolate(crate::IsolationReason::Policy);
                }
                let _ = ready.send(Err(error));
                return;
            }
        };
        if ready.send(Ok(state.clone())).is_ok() {
            loop {
                if state.close_if_expired() {
                    break;
                }
                match futures_executor::block_on(tokio::time::timeout(
                    Duration::from_millis(25),
                    receiver.recv(),
                )) {
                    Ok(Some(command)) => {
                        futures_executor::block_on(process(&mut connection, &state, command))
                    }
                    Ok(None) => break,
                    Err(_) => {}
                }
            }
        }
        // 即使上层 runtime 取消任务，阻塞域仍等待 SQLx worker 确认退出才放开文件。
        state.begin_operation(
            ExecutionPolicy::Cleanup,
            std::time::Instant::now() + Duration::from_secs(5),
            Arc::new(AtomicBool::new(false)),
        );
        if futures_executor::block_on(connection.close()).is_ok() {
            state.closed.store(true, Ordering::Release);
        } else {
            source.isolate(crate::IsolationReason::Worker);
        }
        drop(lifetime);
        scope.completed = true;
    });
    let state = tokio::time::timeout_at(deadline, response).await???;
    {
        let mut states = source_for_states
            .0
            .health
            .connections
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        states.retain(|state| state.strong_count() > 0);
        states.push(Arc::downgrade(&state));
    }
    Ok(Actor {
        sender,
        state,
    })
}

/// 业务作用：建立私有连接并回读关键 PRAGMA，已有文件不隐式切换 journal_mode。
///
/// 参数说明：
/// - `source`：冻结配置与文件身份。
/// - `created`：允许初始化日志模式的新文件标记。
/// - `lifetime`：物理连接保活责任。
/// - `deadline`：初始化总预算。
///
/// 返回：原生能力及配置验证通过的连接和状态。
async fn open_connection(
    source: &SqliteDataSource,
    created: bool,
    lifetime: Arc<PhysicalLifetime>,
    deadline: Instant,
) -> anyhow::Result<(SqliteConnection, Arc<NativeState>)> {
    source.verify_file()?;
    anyhow::ensure!(
        unsafe { libsqlite3_sys::sqlite3_libversion_number() } >= 3_051_003,
        "SQLite engine must be at least 3.51.3"
    );
    let config = &source.0.config.sqlite;
    let opening = crate::open_guard::OpenGuard::new(lifetime.clone())?;
    let thread_name = opening.name();
    let options = SqliteConnectOptions::new()
        .disable_statement_logging()
        .thread_name(move |_| thread_name.clone())
        .filename(&source.0.file.path)
        .read_only(config.read_only)
        .create_if_missing(false)
        .shared_cache(false)
        .foreign_keys(true)
        .statement_cache_capacity(0)
        .command_buffer_size(1)
        .row_buffer_size(8)
        .busy_timeout(Duration::from_millis(config.busy_timeout_ms))
        .pragma("read_uncommitted", "OFF")
        .pragma("temp_store", "MEMORY")
        .pragma(
            "fullfsync",
            if cfg!(target_os = "macos") {
                "ON"
            } else {
                "OFF"
            },
        )
        .pragma(
            "checkpoint_fullfsync",
            if cfg!(target_os = "macos") {
                "ON"
            } else {
                "OFF"
            },
        )
        .pragma("trusted_schema", "OFF");
    let mut connection = SqliteConnection::connect_with(&options).await?;
    let busy_timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
        .fetch_one(&mut connection)
        .await?;
    anyhow::ensure!(
        busy_timeout == config.busy_timeout_ms as i64,
        "SQLite busy timeout differs from configuration"
    );
    let state = NativeState::install(
        &mut connection,
        Duration::from_millis(config.busy_timeout_ms),
        source,
        lifetime.capacity.clone(),
    )
    .await?;
    state.begin_operation(
        ExecutionPolicy::Internal,
        deadline.into_std(),
        Arc::new(AtomicBool::new(false)),
    );
    let expected = match config.journal_mode {
        JournalMode::Wal => "wal",
        JournalMode::Delete => "delete",
    };
    if created {
        let sql = match config.journal_mode {
            JournalMode::Wal => "PRAGMA journal_mode = WAL",
            JournalMode::Delete => "PRAGMA journal_mode = DELETE",
        };
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .execute(&mut connection)
            .await?;
    }
    let actual: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&mut connection)
        .await?;
    anyhow::ensure!(
        actual.eq_ignore_ascii_case(expected),
        "SQLite journal_mode differs from configuration"
    );
    if !config.read_only {
        let sql =
            match config
                .synchronous
                .unwrap_or(if config.journal_mode == JournalMode::Delete {
                    Synchronous::Extra
                } else {
                    Synchronous::Full
                }) {
                Synchronous::Normal => "PRAGMA synchronous = NORMAL",
                Synchronous::Full => "PRAGMA synchronous = FULL",
                Synchronous::Extra => "PRAGMA synchronous = EXTRA",
            };
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .execute(&mut connection)
            .await?;
        if config.journal_mode == JournalMode::Wal {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "PRAGMA wal_autocheckpoint = {}",
                config.wal_autocheckpoint_pages.unwrap_or(1000)
            )))
            .execute(&mut connection)
            .await?;
        }
    }
    let foreign_keys: i32 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&mut connection)
        .await?;
    anyhow::ensure!(foreign_keys == 1, "SQLite foreign keys are not enforced");
    let synchronous: i32 = sqlx::query_scalar("PRAGMA synchronous")
        .fetch_one(&mut connection)
        .await?;
    if !config.read_only {
        let expected =
            match config
                .synchronous
                .unwrap_or(if config.journal_mode == JournalMode::Delete {
                    Synchronous::Extra
                } else {
                    Synchronous::Full
                }) {
                Synchronous::Normal => 1,
                Synchronous::Full => 2,
                Synchronous::Extra => 3,
            };
        anyhow::ensure!(
            synchronous == expected,
            "SQLite synchronous policy differs from configuration"
        );
    }
    let read_uncommitted: i32 = sqlx::query_scalar("PRAGMA read_uncommitted")
        .fetch_one(&mut connection)
        .await?;
    let trusted_schema: i32 = sqlx::query_scalar("PRAGMA trusted_schema")
        .fetch_one(&mut connection)
        .await?;
    let fullfsync: i32 = sqlx::query_scalar("PRAGMA fullfsync")
        .fetch_one(&mut connection)
        .await?;
    let checkpoint_fullfsync: i32 = sqlx::query_scalar("PRAGMA checkpoint_fullfsync")
        .fetch_one(&mut connection)
        .await?;
    anyhow::ensure!(
        read_uncommitted == 0
            && trusted_schema == 0
            && fullfsync == i32::from(cfg!(target_os = "macos"))
            && checkpoint_fullfsync == fullfsync,
        "SQLite connection policy is unavailable"
    );
    if !config.read_only && config.journal_mode == JournalMode::Wal {
        let pages: i64 = sqlx::query_scalar("PRAGMA wal_autocheckpoint")
            .fetch_one(&mut connection)
            .await?;
        anyhow::ensure!(
            pages == i64::from(config.wal_autocheckpoint_pages.unwrap_or(1000)),
            "SQLite checkpoint policy differs from configuration"
        );
    }
    let json: i32 = sqlx::query_scalar("SELECT json_valid('{\"enabled\":true}')")
        .fetch_one(&mut connection)
        .await?;
    anyhow::ensure!(json == 1, "SQLite JSON capability is unavailable");
    // 私有内存 TEMP 表只探测语法能力，不触及主库数据或迁移历史，验证结束后立即撤销。
    sqlx::query("CREATE TEMP TABLE __nasa_capability_probe(id INTEGER PRIMARY KEY)")
        .execute(&mut connection)
        .await?;
    sqlx::query("EXPLAIN INSERT INTO temp.__nasa_capability_probe DEFAULT VALUES RETURNING id")
        .fetch_all(&mut connection)
        .await?;
    sqlx::query("DROP TABLE temp.__nasa_capability_probe")
        .execute(&mut connection)
        .await?;
    let variable_limit = {
        let mut handle = connection.lock_handle().await?;
        unsafe {
            libsqlite3_sys::sqlite3_limit(
                handle.as_raw_handle().as_ptr(),
                libsqlite3_sys::SQLITE_LIMIT_VARIABLE_NUMBER,
                -1,
            )
        }
    };
    anyhow::ensure!(
        variable_limit > 0,
        "SQLite parameter binding is unavailable"
    );
    let capabilities = crate::ConnectionCapabilities {
        engine_version: sqlx::query_scalar("SELECT sqlite_version()")
            .fetch_one(&mut connection)
            .await?,
        variable_limit,
        synchronous,
        foreign_keys: true,
        read_uncommitted: false,
        trusted_schema: false,
        fullfsync: fullfsync != 0,
        busy_timeout_ms: config.busy_timeout_ms,
        json: true,
        returning: true,
    };
    let installed = source.0.capabilities.get_or_init(|| capabilities.clone());
    anyhow::ensure!(
        *installed == capabilities,
        "SQLite physical connection policies disagree"
    );
    source.verify_file()?;
    source.check_pressure(false)?;
    state.begin_operation(
        ExecutionPolicy::Read,
        std::time::Instant::now() + Duration::from_millis(source.0.config.operation_timeout_ms),
        Arc::new(AtomicBool::new(false)),
    );
    Ok((connection, state))
}

/// 业务作用：顺序执行同一连接的动作，并按引擎实际事务状态决定后续准入。
///
/// 参数说明：
/// - `connection`：worker 独占的物理连接。
/// - `state`：连接权限与事务连续性状态。
/// - `command`：包含独立预算及保活责任的操作。
///
/// 返回：向调用方发出最终确认，事务失败或不确定写入不会被静默复用。
async fn process(connection: &mut SqliteConnection, state: &Arc<NativeState>, command: Command) {
    match command {
        Command::Maintenance {
            operation,
            deadline,
            cancel,
            reply,
            _gate,
            _lease,
        } => {
            let started = std::time::Instant::now();
            let backup = matches!(&operation, crate::maintenance::Operation::Backup(..));
            state.begin_operation(
                ExecutionPolicy::Internal,
                deadline.into_std(),
                cancel.clone(),
            );
            let result = if state.poisoned.load(Ordering::Acquire)
                || state.transaction.load(Ordering::Acquire)
            {
                Err(anyhow::anyhow!(
                    "SQLite maintenance connection is unavailable"
                ))
            } else if let Err(error) = _lease.source.verify_file() {
                Err(error)
            } else {
                crate::maintenance::execute_native(
                    connection,
                    &_lease.source,
                    operation,
                    deadline,
                    cancel,
                )
                .await
            };
            // 真实维护失败同样可以证明源库失去业务资格；目标路径或发布失败不构成源故障。
            _lease.source.observe_maintenance(&result);
            let diagnostics = &_lease.source.0.diagnostics;
            if let Ok(crate::maintenance::ResultValue::Checkpoint(result)) = &result {
                diagnostics
                    .checkpoint_wal
                    .store(result.wal_frames.map_or(-1, i64::from), Ordering::Relaxed);
                diagnostics.checkpoint_done.store(
                    result.checkpointed_frames.map_or(-1, i64::from),
                    Ordering::Relaxed,
                );
                diagnostics
                    .checkpoint_busy
                    .store(i64::from(result.busy), Ordering::Relaxed);
            }
            if backup {
                diagnostics.backup_millis.store(
                    started.elapsed().as_millis().min(u64::MAX as u128) as u64,
                    Ordering::Relaxed,
                );
                nametrics_core::atomic::add(
                    if result.is_ok() {
                        &diagnostics.backup_ok
                    } else {
                        &diagnostics.backup_failed
                    },
                    1,
                );
            }
            let _ = reply.send(result);
        }
        Command::Validate {
            sql,
            deadline,
            cancel,
            reply,
            _lease,
            _ticket,
        } => {
            state.begin_operation(ExecutionPolicy::Read, deadline.into_std(), cancel.clone());
            let result = if state.poisoned.load(Ordering::Acquire)
                || state.rollback_only.load(Ordering::Acquire)
                || cancel.load(Ordering::Acquire)
                || Instant::now() >= deadline
                || _lease.source.business_expired()
                || _lease.source.verify_file().is_err()
            {
                Err(protocol("SQLite read validation rejected"))
            } else {
                let validation =
                    native::validate(connection, &sql, usize::MAX, ExecutionPolicy::Read)
                        .await
                        .map(|_| ());
                _lease.source.observe_result(&validation);
                validation.and_then(|()| {
                    _lease
                        .source
                        .check_pressure(false)
                        .map_err(|_| protocol("SQLite storage status is unavailable"))
                })
            };
            // 验证已结束后先归还 worker 持有的事务槽，再唤醒可能立即继续执行的调用方。
            drop(_ticket);
            let _ = reply.send(result);
        }

        Command::Control {
            action,
            deadline,
            cancel,
            reply,
            _lease,
        } => {
            state.begin_operation(
                if matches!(action, Control::Rollback) {
                    ExecutionPolicy::Cleanup
                } else {
                    ExecutionPolicy::Internal
                },
                deadline.into_std(),
                cancel.clone(),
            );
            let result = if cancel.load(Ordering::Acquire)
                || Instant::now() >= deadline
                || (!matches!(action, Control::Rollback) && _lease.source.business_expired())
            {
                state.poisoned.store(true, Ordering::Release);
                Err(protocol("SQLite transaction deadline exceeded"))
            } else {
                match action {
                    Control::Begin(mode)
                    | Control::BeginMigration {
                        mode,
                        ..
                    } => {
                        let statement = match mode {
                            crate::TransactionMode::Deferred => "BEGIN DEFERRED",
                            crate::TransactionMode::Immediate => "BEGIN IMMEDIATE",
                        };
                        let result = SqliteTransactionManager::begin(
                            connection,
                            Some(SqlStr::from_static(statement)),
                        )
                        .await;
                        if result.is_ok() {
                            state.transaction.store(true, Ordering::Release);
                        }
                        result
                    }
                    Control::Commit
                        if state.rollback_only.load(Ordering::Acquire)
                            || state.poisoned.load(Ordering::Acquire) =>
                    {
                        Err(protocol("SQLite transaction is rollback-only"))
                    }
                    Control::Commit => SqliteTransactionManager::commit(connection).await,
                    Control::Rollback => {
                        match native::autocommit(connection).await {
                            Ok(true) => {
                                // 引擎已自动结束事务，SQLx 私有深度不能复位；确认无活动事务后只关闭该连接。
                                state.poisoned.store(true, Ordering::Release);
                                Ok(())
                            }
                            Ok(false) => SqliteTransactionManager::rollback(connection).await,
                            Err(error) => Err(error),
                        }
                    }
                }
            };
            let active = !native::autocommit(connection).await.unwrap_or(false);
            state.transaction.store(active, Ordering::Release);
            // BEGIN 收据之前固定总期限；迁移取得锁后才开始执行预算，业务事务沿用入口截止点。
            if result.is_ok() && active {
                match action {
                    Control::Begin(_) => state.set_transaction_deadline(Some(deadline.into_std())),
                    Control::BeginMigration {
                        execution_timeout,
                        ..
                    } => state.set_transaction_deadline(Some(
                        std::time::Instant::now() + execution_timeout,
                    )),
                    _ => {}
                }
            } else if !active {
                // 只有引擎确认事务结束才解除；业务语句和嵌套调用不能续期。
                state.set_transaction_deadline(None);
            }
            if result.is_ok() && matches!(action, Control::Commit) {
                _lease.source.0.cache_epoch.fetch_add(1, Ordering::AcqRel);
            }
            if result.is_ok() && !active {
                state.rollback_only.store(false, Ordering::Release);
            }
            _lease.source.observe_result(&result);
            let failed = result.is_err();
            if reply.send(result).is_err() || failed {
                state.poisoned.store(true, Ordering::Release);
            }
        }
        Command::Query {
            sql,
            arguments,
            policy,
            deadline,
            cancel,
            output,
            budget,
            _lease,
            _ticket,
        } => {
            state.begin_operation(policy, deadline.into_std(), cancel.clone());
            let validation = if state.poisoned.load(Ordering::Acquire)
                || state.rollback_only.load(Ordering::Acquire)
                || cancel.load(Ordering::Acquire)
                || Instant::now() >= deadline
                || _lease.source.business_expired()
                || _lease.source.verify_file().is_err()
            {
                Err(protocol("SQLite connection cannot accept this operation"))
            } else {
                native::validate(connection, sql.as_str(), arguments.len(), policy).await
            };
            let write = validation.as_ref().copied().unwrap_or(false);
            let validation = validation.and_then(|write| {
                _lease
                    .source
                    .check_pressure(write)
                    .map_err(|_| protocol("SQLite storage pressure prevents this operation"))?;
                if !write {
                    let mut id = _lease
                        .read_id
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    if id.is_none() {
                        *id = Some(_lease.source.track_read());
                    }
                }
                Ok(write)
            });
            let result = match validation {
                Err(error) => Err(error),
                Ok(_) => {
                    execute_rows(
                        connection,
                        sql,
                        arguments,
                        RowDelivery {
                            deadline,
                            cancel: &cancel,
                            output: &output,
                        },
                        &_lease.source,
                        state,
                    )
                    .await
                }
            };
            // SQLite 对 FULL、IOERR、INTERRUPT 等故障可能自行回滚；事务上下文不得继续写入新事务。
            if state.transaction.load(Ordering::Acquire)
                && native::autocommit(connection).await.unwrap_or(true)
            {
                state.poisoned.store(true, Ordering::Release);
                state.rollback_only.store(true, Ordering::Release);
            }
            if result.is_err() {
                if state.transaction.load(Ordering::Acquire) {
                    state.rollback_only.store(true, Ordering::Release);
                }
                if write {
                    _lease.source.0.cache_safe.store(false, Ordering::Release);
                }
            }
            _lease.source.observe_result(&result);
            if write && result.is_ok() && !state.transaction.load(Ordering::Acquire) {
                _lease.source.0.cache_epoch.fetch_add(1, Ordering::AcqRel);
            }
            // 事务连续性状态必须先发布，worker 的槽责任也必须先结束，最终收据才能交给调用方。
            drop(_ticket);
            let final_step = match result {
                Ok(()) => None,
                Err(error) => match &budget {
                    Some(budget) => {
                        // 错误收据独立于有界行通道，超期或隔离后的交付门禁仍能保留原始数据库失败。
                        budget.record_failure(error);
                        None
                    }
                    None => Some(Err(error)),
                },
            };
            state.consumer_ready();
            if !matches!(
                tokio::time::timeout_at(deadline, output.send(final_step)).await,
                Ok(Ok(()))
            ) && write
            {
                state.rollback_only.store(true, Ordering::Release);
                _lease.source.0.cache_safe.store(false, Ordering::Release);
            }
        }
    }
}

/// 单次行交付共用的截止点、取消信号和有界输出；不延长连接或事务的生命周期。
struct RowDelivery<'a> {
    /// 当前操作固定的总截止点，逐行输出不得重新计算总预算。
    deadline: Instant,
    /// 消费者退出或预算耗尽时通知原生工作停止的共享信号。
    cancel: &'a Arc<AtomicBool>,
    /// 接收行与查询完成事实的有界通道，等待受总预算和空闲预算共同限制。
    output: &'a mpsc::Sender<Option<Step>>,
}

/// 业务作用：有界消费原生结果并对停滞消费者及时中断，避免连接被无限持有。
///
/// 参数说明：
/// - `connection`：已通过权限验证的连接。
/// - `sql`：唯一受控语句或框架脚本。
/// - `arguments`：全部已编码参数。
/// - `delivery`：同一次消费的截止点、取消信号及输出通道，保证每行沿用原有交付预算。
/// - `source`：结果和空闲预算配置。
/// - `state`：接收结果缓冲可消费通知的原生连接状态。
///
/// 返回：全部结果与语句结束均成功时完成，否则保留原始数据库错误。
async fn execute_rows(
    connection: &mut SqliteConnection,
    sql: SqlStr,
    arguments: SqliteArguments,
    delivery: RowDelivery<'_>,
    source: &SqliteDataSource,
    state: &NativeState,
) -> Result<(), sqlx::Error> {
    let RowDelivery {
        deadline,
        cancel,
        output,
    } = delivery;
    let mut rows = 0usize;
    let mut bytes = 0usize;
    let query = sqlx::query_with(sql, arguments).persistent(false);
    let mut stream = connection.fetch_many(query);
    while let Some(step) = stream.try_next().await? {
        if cancel.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(protocol("SQLite operation deadline exceeded"));
        }
        if let Either::Right(row) = &step {
            rows += 1;
            for index in 0..row.len() {
                let value = row.try_get_raw(index)?;
                bytes = bytes.saturating_add(match value.type_info().name() {
                    "TEXT" | "BLOB" if !value.is_null() => {
                        row.try_get_unchecked::<Vec<u8>, _>(index)?.len()
                    }
                    _ => 8,
                });
            }
            if rows > source.0.config.max_result_rows || bytes > source.0.config.max_result_bytes {
                return Err(protocol("SQLite result limit exceeded"));
            }
        }
        let idle_deadline = deadline
            .min(Instant::now() + Duration::from_millis(source.0.config.stream_idle_timeout_ms));
        // 先登记可消费事实再发布行，避免快速消费者已经请求下一行后才收到上一行的空闲计时。
        state.consumer_ready();
        if !matches!(
            tokio::time::timeout_at(idle_deadline, output.send(Some(Ok(step)))).await,
            Ok(Ok(()))
        ) {
            cancel.store(true, Ordering::Release);
            return Err(protocol("SQLite result consumer stopped"));
        }
    }
    Ok(())
}
