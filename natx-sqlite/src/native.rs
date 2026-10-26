use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use libsqlite3_sys as ffi;
use sqlx::SqliteConnection;

/// 权限阶段只由框架调度，业务 SQL 不取得控制事务和迁移记账的权威。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExecutionPolicy {
    Read = 0,
    Write = 1,
    #[doc(hidden)]
    Migration = 2,
    #[doc(hidden)]
    History = 3,
    #[doc(hidden)]
    Internal = 4,
    Cleanup = 5,
}

#[derive(Default)]
struct LeaseBudget {
    deadline: Option<Instant>,
    closing: bool,
}

pub(crate) struct NativeState {
    source: std::sync::Weak<crate::datasource::Source>,
    policy: AtomicU8,
    schema_change: AtomicBool,
    busy_timeout: Duration,
    busy_started: Mutex<Option<Instant>>,
    budget: Mutex<Option<(Instant, Arc<AtomicBool>)>>,
    lease_budget: Mutex<LeaseBudget>,
    transaction_deadline: Mutex<Option<Instant>>,
    pub(crate) transaction: AtomicBool,
    pub(crate) rollback_only: AtomicBool,
    pub(crate) poisoned: AtomicBool,
    pub(crate) closed: AtomicBool,
    pub(crate) maintenance: Mutex<Option<tokio::sync::OwnedMutexGuard<()>>>,
    pub(crate) capacity: Arc<crate::ownership::LeaseCapacity>,
    pub(crate) consumer: Mutex<std::sync::Weak<crate::stream::Consumer>>,
}

impl NativeState {
    /// 业务作用：把权限与取消绑定到真实 SQLite handle，并让回调状态保活到原生析构。
    /// 参数说明：
    /// - `connection`：尚未向业务发布的物理连接。
    /// - `busy_timeout`：单次锁等待允许的最大时长。
    /// - `source`：只保留弱引用，读取排空截止点而不延长来源生命周期。
    /// - `capacity`：由真实原生生命周期负责最终撤销的逻辑借用容量。
    /// 返回：安装完成后的共享状态；失败不发布该连接。
    pub(crate) async fn install(
        connection: &mut SqliteConnection,
        busy_timeout: Duration,
        source: &crate::SqliteDataSource,
        capacity: Arc<crate::ownership::LeaseCapacity>,
    ) -> Result<Arc<Self>, sqlx::Error> {
        let state = Arc::new(Self {
            source: Arc::downgrade(&source.0),
            busy_timeout,
            busy_started: Mutex::new(None),
            policy: AtomicU8::new(ExecutionPolicy::Internal as u8),
            schema_change: AtomicBool::new(false),
            budget: Mutex::new(None),
            lease_budget: Mutex::new(LeaseBudget::default()),
            transaction_deadline: Mutex::new(None),
            transaction: AtomicBool::new(false),
            rollback_only: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            maintenance: Mutex::new(None),
            capacity,
            consumer: Mutex::new(std::sync::Weak::new()),
        });
        let mut locked = connection.lock_handle().await?;
        let handle = locked.as_raw_handle().as_ptr();
        let retained = Arc::into_raw(state.clone()) as *mut c_void;
        // xDestroy 随原生连接销毁，避免 authorizer/progress 在 Rust 等待方退出后访问已释放状态。
        let result = unsafe {
            ffi::sqlite3_create_function_v2(
                handle,
                c"__nasa_connection_owner".as_ptr(),
                0,
                ffi::SQLITE_UTF8 | ffi::SQLITE_DIRECTONLY,
                retained,
                Some(owner_function),
                None,
                None,
                Some(release_state),
            )
        };
        if result != ffi::SQLITE_OK {
            return Err(locked
                .last_error()
                .map(sqlx::Error::from)
                .unwrap_or_else(|| protocol("SQLite native operation failed")));
        }
        // 回调在任何业务 prepare 前安装，禁止准备期 PRAGMA 先产生副作用再被拒绝。
        let result = unsafe {
            ffi::sqlite3_set_authorizer(handle, Some(authorize), Arc::as_ptr(&state) as *mut c_void)
        };
        if result != ffi::SQLITE_OK {
            return Err(locked
                .last_error()
                .map(sqlx::Error::from)
                .unwrap_or_else(|| protocol("SQLite native operation failed")));
        }
        // 迁移可包含多条语句，必须在原生语句结束时撤销 ALTER 的临时检查权，不能延续到下一条 SQL。
        let result = unsafe {
            ffi::sqlite3_trace_v2(
                handle,
                ffi::SQLITE_TRACE_PROFILE as u32,
                Some(finish_statement),
                Arc::as_ptr(&state) as *mut c_void,
            )
        };
        if result != ffi::SQLITE_OK {
            return Err(protocol(
                "SQLite statement boundary callback installation failed",
            ));
        }
        unsafe {
            if ffi::sqlite3_busy_handler(handle, Some(busy), Arc::as_ptr(&state) as *mut c_void)
                != ffi::SQLITE_OK
            {
                return Err(protocol("SQLite busy handler is unavailable"));
            }
            ffi::sqlite3_progress_handler(
                handle,
                1000,
                Some(progress),
                Arc::as_ptr(&state) as *mut c_void,
            );
            let mut enabled = 0;
            if ffi::sqlite3_db_config(handle, ffi::SQLITE_DBCONFIG_DEFENSIVE, 1, &mut enabled)
                != ffi::SQLITE_OK
                || enabled != 1
            {
                return Err(protocol("SQLite defensive mode is unavailable"));
            }
            ffi::sqlite3_limit(handle, ffi::SQLITE_LIMIT_SQL_LENGTH, 1024 * 1024);
            ffi::sqlite3_limit(handle, ffi::SQLITE_LIMIT_LENGTH, 16 * 1024 * 1024);
        }
        Ok(state)
    }

    /// 业务作用：每次操作重置独立取消信号，防止旧查询的取消影响连接下一位使用者。
    /// 参数说明：
    /// - `policy`：当前执行阶段允许的操作。
    /// - `deadline`：执行和消费共同遵守的绝对截止点。
    /// - `cancel`：当前操作的显式取消信号。
    /// 返回：后续 prepare、锁等待和 VM 执行读取同一份规则。
    pub(crate) fn begin_operation(
        &self,
        policy: ExecutionPolicy,
        deadline: Instant,
        cancel: Arc<AtomicBool>,
    ) {
        self.schema_change.store(false, Ordering::Release);
        self.policy.store(policy as u8, Ordering::Release);
        *self
            .budget
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some((deadline, cancel));
    }

    /// 业务作用：与空闲关闭决定互斥地接纳新借用，交接调用期限及真实连接上的逻辑容量。
    /// 参数说明：
    /// - `deadline`：调用链固定的绝对截止点，不是 acquire 的局部等待期限。
    /// - `permit`：本次调用已经取得的逻辑容量凭证。
    /// 返回：仍可接纳时完成交接；已开始关闭、状态失效或本次期限已过时交还凭证，不恢复旧连接。
    pub(crate) fn begin_lease(
        &self,
        deadline: Instant,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<(), tokio::sync::OwnedSemaphorePermit> {
        let mut lease = self
            .lease_budget
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // 关闭一经决定便不可逆；与 worker 使用同一锁，防止交接后仍按前一位使用者的期限关闭。
        if lease.closing
            || self.poisoned.load(Ordering::Acquire)
            || self.closed.load(Ordering::Acquire)
            || self.rollback_only.load(Ordering::Acquire)
            || self.transaction.load(Ordering::Acquire)
            || self.business_expired()
            || Instant::now() >= deadline
        {
            return Err(permit);
        }
        self.capacity.acquire(permit)?;
        lease.deadline = Some(deadline);
        // 已完成借用的消费状态不能约束下一位使用者；旧流的原生责任未结束时连接不会回池。
        *self
            .consumer
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = std::sync::Weak::new();
        Ok(())
    }

    /// 业务作用：单独保留已确认事务的总期限，语句完成不能缩短或续期该事务。
    /// 参数说明：
    /// - `deadline`：业务根入口的总截止点或迁移取得锁后的批次截止点；确认事务已结束时传 None。
    /// 返回：空闲监督使用事务总期限，原生语句回调仍使用各自的操作预算。
    pub(crate) fn set_transaction_deadline(&self, deadline: Option<Instant>) {
        *self
            .transaction_deadline
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = deadline;
    }

    /// 业务作用：让空闲监督及迁移调用方共用已固定的事务截止点，避免收据等待重新计时。
    /// 参数说明：无。
    /// 返回：已确认事务的绝对期限；未建立或已确认结束时为 None。
    pub(crate) fn transaction_deadline(&self) -> Option<Instant> {
        *self
            .transaction_deadline
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    /// 业务作用：让原生回调以无 I/O 的方式读取当前操作是否应停止。
    /// 参数说明：无。
    /// 返回：业务来源失效、消费者空闲到期、操作预算终止或取消时为真；清理仍使用自己的预算。
    fn expired(&self) -> bool {
        let policy = self.policy.load(Ordering::Acquire);
        if policy != ExecutionPolicy::Cleanup as u8 && self.business_expired() {
            return true;
        }
        // VM 或锁等待可能长期不返回命令循环，必须在原生合作式取消点监督已失去消费许可的业务查询。
        if (policy == ExecutionPolicy::Read as u8 || policy == ExecutionPolicy::Write as u8)
            && self.consumer_expired()
        {
            return true;
        }
        self.budget.lock().map_or(true, |budget| {
            budget.as_ref().is_some_and(|(deadline, cancel)| {
                Instant::now() >= *deadline || cancel.load(Ordering::Acquire)
            })
        })
    }

    /// 业务作用：让原生取消与结果交付共同读取来源隔离、排空期限和最终关闭状态。
    /// 参数说明：无。
    /// 返回：来源失效时为真；仅停止新接纳、仍在排空期限内的既有查询可以继续。
    pub(crate) fn business_expired(&self) -> bool {
        self.source.upgrade().is_none_or(|source| {
            source.borrowers.is_closed() || crate::SqliteDataSource(source).business_expired()
        })
    }

    /// 业务作用：在命令之间原子决定到期关闭，避免与新借用的期限交接竞争。
    /// 参数说明：无。
    /// 返回：来源或消费资格失效、事务或借用期限结束时标记不可再接纳并返回真；未到期时保留原生责任。
    pub(crate) fn close_if_expired(&self) -> bool {
        let mut lease = self
            .lease_budget
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // worker 只在命令间调用空闲监督；事务中的业务间隔不能继承已完成语句的短预算。
        let deadline = self.transaction_deadline().or(lease.deadline);
        let expired = deadline.map_or_else(
            || {
                self.budget.lock().map_or(true, |budget| {
                    budget
                        .as_ref()
                        .is_some_and(|(deadline, _)| Instant::now() >= *deadline)
                })
            },
            |deadline| Instant::now() >= deadline,
        );
        if lease.closing || self.business_expired() || self.consumer_expired() || expired {
            // 先在交接锁内撤销后续接纳，再由 worker 关闭；容量与文件 owner 仍等真实原生退出。
            lease.closing = true;
            self.poisoned.store(true, Ordering::Release);
            return true;
        }
        false
    }
}

/// 业务作用：在专有阻塞执行域中核验 SQL 的单语句、绑定和只读合同。
/// 参数说明：
/// - `connection`：操作期间独占的 SQLx 连接。
/// - `sql`：完整 SQL 文本。
/// - `arguments`：本次实际绑定的值数量。
/// - `policy`：业务读取、写入或框架执行阶段。
/// 返回：保留连接并返回实际写入可能性；不执行 SQL step。
pub(crate) async fn validate(
    connection: &mut SqliteConnection,
    sql: &str,
    arguments: usize,
    policy: ExecutionPolicy,
) -> Result<bool, sqlx::Error> {
    if sql.len() > 1024 * 1024 || sql.contains('\0') {
        return Err(protocol("SQLite SQL exceeds the supported boundary"));
    }
    if matches!(
        policy,
        ExecutionPolicy::Migration | ExecutionPolicy::Internal | ExecutionPolicy::Cleanup
    ) {
        return Ok(true);
    }
    let sql = CString::new(sql).map_err(|_| protocol("SQLite SQL contains NUL"))?;
    let mut locked = connection.lock_handle().await?;
    let handle = locked.as_raw_handle().as_ptr();
    let limit = unsafe { ffi::sqlite3_limit(handle, ffi::SQLITE_LIMIT_VARIABLE_NUMBER, -1) };
    if arguments != usize::MAX && arguments > limit as usize {
        return Err(protocol(
            "SQLite parameter count exceeds the actual connection limit",
        ));
    }
    let mut statement = std::ptr::null_mut();
    let mut tail = std::ptr::null();
    let result =
        unsafe { ffi::sqlite3_prepare_v3(handle, sql.as_ptr(), -1, 0, &mut statement, &mut tail) };
    if result != ffi::SQLITE_OK {
        if !statement.is_null() {
            unsafe { ffi::sqlite3_finalize(statement) };
        }
        return Err(locked
            .last_error()
            .map(sqlx::Error::from)
            .unwrap_or_else(|| protocol("SQLite native operation failed")));
    }
    if statement.is_null() {
        return Err(protocol("SQLite business SQL must contain one statement"));
    }
    let readonly = unsafe { ffi::sqlite3_stmt_readonly(statement) != 0 };
    let expected = unsafe { ffi::sqlite3_bind_parameter_count(statement) as usize };
    unsafe { ffi::sqlite3_finalize(statement) };
    if arguments != usize::MAX && expected != arguments {
        return Err(protocol(
            "SQLite bind count does not match prepared parameters",
        ));
    }
    let mut extra = std::ptr::null_mut();
    // 由 SQLite 自己识别尾部注释与 trigger 内分号，不能依靠字符串切分判定语句个数。
    let result =
        unsafe { ffi::sqlite3_prepare_v3(handle, tail, -1, 0, &mut extra, std::ptr::null_mut()) };
    if !extra.is_null() {
        unsafe { ffi::sqlite3_finalize(extra) };
        return Err(protocol("SQLite business SQL accepts only one statement"));
    }
    if result != ffi::SQLITE_OK {
        return Err(locked
            .last_error()
            .map(sqlx::Error::from)
            .unwrap_or_else(|| protocol("SQLite native operation failed")));
    }
    if policy == ExecutionPolicy::Read && !readonly {
        return Err(protocol(
            "SQLite Query requires a read-only statement or an explicit write contract",
        ));
    }
    Ok(!readonly)
}

/// 业务作用：在 worker 不访问句柄时读取真实 autocommit 状态，识别引擎自行回滚。
/// 参数说明：
/// - `connection`：待确认事务连续性的物理连接。
/// 返回：SQLite 当前未处于显式事务时为真。
pub(crate) async fn autocommit(connection: &mut SqliteConnection) -> Result<bool, sqlx::Error> {
    let mut handle = connection.lock_handle().await?;
    Ok(unsafe { ffi::sqlite3_get_autocommit(handle.as_raw_handle().as_ptr()) != 0 })
}

/// 业务作用：原生连接释放应用函数时归还回调状态引用，物理文件责任由独立锚点管理。
/// 参数说明：
/// - `data`：由 Arc::into_raw 唯一转交的状态引用。
/// 返回：释放引用，不执行数据库调用或跨 FFI 展开。
unsafe extern "C" fn release_state(data: *mut c_void) {
    drop(unsafe { Arc::from_raw(data as *const NativeState) });
}

/// 业务作用：为连接析构保活提供不暴露内部状态的原生函数载体。
/// 参数说明：
/// - `context`：SQLite 函数调用上下文。
/// - `_count`：不接收业务参数。
/// - `_values`：不接收业务值。
/// 返回：固定 NULL；普通业务 authorizer 拒绝调用本函数。
unsafe extern "C" fn owner_function(
    context: *mut ffi::sqlite3_context,
    _count: c_int,
    _values: *mut *mut ffi::sqlite3_value,
) {
    unsafe { ffi::sqlite3_result_null(context) };
}

/// 业务作用：在原生 VM 合作式中断点执行当前操作的取消与预算门禁。
/// 参数说明：
/// - `data`：连接持有的状态引用。
/// 返回：非零要求 SQLite 停止当前语句。
unsafe extern "C" fn progress(data: *mut c_void) -> c_int {
    c_int::from(unsafe { &*(data as *const NativeState) }.expired())
}

/// 业务作用：让原生锁等待遵守同一操作截止点，避免 async timeout 后仍无限占锁。
/// 参数说明：
/// - `data`：当前连接状态。
/// - `attempt`：SQLite 已调用该 handler 的次数。
/// 返回：预算允许时短暂退避并重试，否则停止本次锁等待。
unsafe extern "C" fn busy(data: *mut c_void, attempt: c_int) -> c_int {
    let state = unsafe { &*(data as *const NativeState) };
    let elapsed = {
        let mut start = state
            .busy_started
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if attempt == 0 {
            *start = Some(Instant::now());
        }
        start.map_or(state.busy_timeout, |time| time.elapsed())
    };
    if state.expired() || elapsed >= state.busy_timeout {
        return 0;
    }
    std::thread::sleep(Duration::from_millis(1));
    c_int::from(!state.expired())
}

/// 业务作用：在原生语句完成、重置或结束执行时撤销临时 schema 检查权，防止迁移内的后续语句继承。
/// 参数说明：
/// - `_event`：仅登记的语句完成事件。
/// - `data`：由原生连接保活的权限状态。
/// - `_statement`：本次结束执行的原生语句。
/// - `_elapsed`：SQLite 提供的耗时，本入口不采集。
/// 返回：继续原生收尾，不执行 SQL 或改变事务结果。
unsafe extern "C" fn finish_statement(
    _event: u32,
    data: *mut c_void,
    _statement: *mut c_void,
    _elapsed: *mut c_void,
) -> c_int {
    let state = unsafe { &*(data as *const NativeState) };
    state.schema_change.store(false, Ordering::Release);
    0
}

/// 业务作用：把编译期间的数据库操作限制到当前框架阶段，保护事务和迁移历史。
/// 参数说明：
/// - `data`：连接持有的权限状态。
/// - `action`：SQLite authorizer 操作代码。
/// - `first`：操作对象或函数名称。
/// - `second`：操作所属对象或函数名称。
/// - `_database`：SQLite 解析出的 schema。
/// - `_trigger`：触发此操作的 trigger 名称。
/// 返回：允许或整体拒绝，不使用静默忽略改写 SQL。
unsafe extern "C" fn authorize(
    data: *mut c_void,
    action: c_int,
    first: *const c_char,
    second: *const c_char,
    _database: *const c_char,
    _trigger: *const c_char,
) -> c_int {
    let state = unsafe { &*(data as *const NativeState) };
    let policy = state.policy.load(Ordering::Acquire);
    if policy == ExecutionPolicy::Internal as u8
        || policy == ExecutionPolicy::History as u8
        || policy == ExecutionPolicy::Cleanup as u8
    {
        return ffi::SQLITE_OK;
    }
    let first = if first.is_null() {
        &[][..]
    } else {
        unsafe { CStr::from_ptr(first) }.to_bytes()
    };
    let second = if second.is_null() {
        &[][..]
    } else {
        unsafe { CStr::from_ptr(second) }.to_bytes()
    };
    let forbidden = matches!(
        action,
        ffi::SQLITE_TRANSACTION
            | ffi::SQLITE_SAVEPOINT
            | ffi::SQLITE_ATTACH
            | ffi::SQLITE_DETACH
            | ffi::SQLITE_CREATE_TEMP_TABLE
            | ffi::SQLITE_CREATE_TEMP_INDEX
            | ffi::SQLITE_CREATE_TEMP_VIEW
            | ffi::SQLITE_CREATE_TEMP_TRIGGER
            | ffi::SQLITE_DROP_TEMP_TABLE
            | ffi::SQLITE_DROP_TEMP_INDEX
            | ffi::SQLITE_DROP_TEMP_VIEW
            | ffi::SQLITE_DROP_TEMP_TRIGGER
            | ffi::SQLITE_CREATE_VTABLE
            | ffi::SQLITE_DROP_VTABLE
    );
    let history = first.eq_ignore_ascii_case(b"_sqlx_migrations")
        || second.eq_ignore_ascii_case(b"_sqlx_migrations");
    let history_write = history && !matches!(action, ffi::SQLITE_READ | ffi::SQLITE_SELECT);
    let dangerous_function = action == ffi::SQLITE_FUNCTION
        && [
            b"load_extension".as_slice(),
            b"writefile",
            b"readfile",
            b"eval",
            b"__nasa_connection_owner",
            b"__nasa_physical_owner",
        ]
        .iter()
        .any(|name| second.eq_ignore_ascii_case(name));
    // ADD COLUMN 的 CHECK 由 SQLite 自身通过 quick_check 校验已有行，只有本次原生 ALTER
    // 授权事件可以打开这一只读内部步骤；独立 PRAGMA 和其它配置操作仍被拒绝。
    if action == ffi::SQLITE_ALTER_TABLE && policy != ExecutionPolicy::Read as u8 {
        state.schema_change.store(true, Ordering::Release);
    }
    let pragma = action == ffi::SQLITE_PRAGMA
        && !(first.eq_ignore_ascii_case(b"quick_check")
            && state.schema_change.load(Ordering::Acquire));
    if forbidden || history_write || dangerous_function || pragma {
        return ffi::SQLITE_DENY;
    }
    if policy == ExecutionPolicy::Read as u8
        && matches!(
            action,
            ffi::SQLITE_INSERT
                | ffi::SQLITE_UPDATE
                | ffi::SQLITE_DELETE
                | ffi::SQLITE_CREATE_TABLE
                | ffi::SQLITE_DROP_TABLE
                | ffi::SQLITE_ALTER_TABLE
                | ffi::SQLITE_CREATE_INDEX
                | ffi::SQLITE_DROP_INDEX
                | ffi::SQLITE_CREATE_TRIGGER
                | ffi::SQLITE_DROP_TRIGGER
                | ffi::SQLITE_CREATE_VIEW
                | ffi::SQLITE_DROP_VIEW
        )
    {
        return ffi::SQLITE_DENY;
    }
    ffi::SQLITE_OK
}

/// 业务作用：输出不包含路径或绑定内容的执行边界错误。
/// 参数说明：
/// - `message`：框架固定的失败摘要。
/// 返回：可由 SQLx 查询 API 传播的协议错误。
pub(crate) fn protocol(message: &str) -> sqlx::Error {
    sqlx::Error::Protocol(message.to_owned())
}
