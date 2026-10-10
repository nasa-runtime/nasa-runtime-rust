//! 通用多注册中心 owner。宿主决定本地接流资格，owner 管理观察、注册和有界关闭。
//! 连接通过 qualifier 精确选择，默认名为 primary；注册清单单独决定发布目标。
//! 连接失败不跨环境回退，提交点复验证据，关闭报告保留结果不明的远端责任。
//! 发现句柄只有读权；关闭等待者的取消不会撤销已经发起的清理。

pub mod assembly;
pub mod config;
mod observation;
mod owner;
pub mod providers;
mod registration;
mod status;
mod transport;
pub use status::{ConnectionStatus, RuntimeObserver, RuntimeStatus};
pub use transport::TransportLease;

pub use nadisc::managed;
pub use observation::{DiscoveryHandle, Observation, Snapshot};
pub use owner::{
    CloseReport, CloseTicket, ConnectionSpec, DiscoveryOwner, OwnerBuilder, OwnerPhase,
    QuiesceTicket,
};
pub use registration::{
    Eligibility, EligibilityController, RegistrationSpec, RegistrationState, RegistrationStatus,
};

use nadisc::managed::{DiscoveryError, ErrorCode, ManagedProvider, Result};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};
use tokio::{
    sync::{watch, Semaphore},
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

pub(crate) struct Core {
    pub id: String,
    pub gate: Mutex<Gate>,
    pub connections: BTreeMap<String, Arc<Connection>>,
    pub default_connection: Option<String>,
    pub limits: config::Limits,
    pub quota: Arc<Mutex<Quota>>,
    pub operations: Semaphore,
    pub watches: Semaphore,
    pub tasks: Mutex<Vec<JoinHandle<()>>>,
    pub registration_tasks: Mutex<Vec<JoinHandle<()>>>,
    pub withdrawal_task: Mutex<Option<JoinHandle<()>>>,
    pub close_task: Mutex<Option<JoinHandle<()>>>,
    pub bridges: Arc<Semaphore>,
    pub transports: Arc<Semaphore>,
    pub stop: CancellationToken,
    pub registrations_stop: CancellationToken,
    pub changes: watch::Sender<u64>,
    pub close: watch::Sender<Option<CloseReport>>,
    pub withdrawn: watch::Sender<Option<Vec<RegistrationStatus>>>,
}

pub(crate) struct Gate {
    pub phase: OwnerPhase,
    pub registrations: BTreeMap<String, Arc<registration::Record>>,
    pub frozen: bool,
    pub close_started: bool,
    pub failed: bool,
    pub shutdown_deadline: Option<Instant>,
}

pub(crate) struct Connection {
    pub name: String,
    pub provider: Arc<dyn ManagedProvider>,
    pub policy: config::ConnectionPolicy,
    pub slots: Mutex<BTreeMap<String, Arc<observation::Slot>>>,
    pub prepared: watch::Sender<bool>,
    pub error: Mutex<Option<DiscoveryError>>,
    pub epoch: AtomicU64,
    pub revocation: AtomicU64,
    pub queries: AtomicU64,
    pub query_errors: AtomicU64,
    pub query_micros: AtomicU64,
}

#[derive(Default)]
pub(crate) struct Quota {
    instances: usize,
    bytes: usize,
}

/// 业务作用：在内存状态锁被中断后仍允许执行撤权和清理。
/// 参数说明：`mutex` 仅保护短时内存操作，不得跨网络 await 持有。
/// 返回：状态保护锁；业务错误由显式状态字段表达。
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// 业务作用：将有限毫秒配置转换为 Tokio 使用的持续时间。
/// 参数说明：`value` 已经过策略预检。
/// 返回：对应持续时间。
pub(crate) fn millis(value: u64) -> Duration {
    Duration::from_millis(value)
}

/// 业务作用：向所有状态等待者发布不携带配置内容的变更通知。
/// 参数说明：`core` 是共享状态，不代表资源关闭权。
/// 返回：等待者可重新检查当前事实，不依赖中间通知逐条到达。
pub(crate) fn changed(core: &Core) {
    core.changes
        .send_modify(|sequence| *sequence = sequence.wrapping_add(1));
}

/// 业务作用：限制单次操作的排队与执行总时间，并响应最终关闭。
/// 参数说明：`core` 提供并发额度，`deadline` 是总截止点，`operation` 是一次后端操作。
/// 返回：额度等待和网络操作共享期限；关闭或超时返回稳定错误。
pub(crate) async fn operation<T>(
    core: &Core,
    deadline: Instant,
    operation: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    if Instant::now() >= deadline {
        return Err(timeout());
    }
    tokio::select! {
        biased;
        _ = core.stop.cancelled() => Err(closed()),
        result = tokio::time::timeout_at(deadline, async {
            let _permit = core.operations.acquire().await.map_err(|_| closed())?;
            tokio::pin!(operation);
            // 排队和后端挂起都可能跨过期限，不能让恰好 Ready 的迟到操作绕过超时裁决。
            futures_util::future::poll_fn(|context| {
                if Instant::now() >= deadline { std::task::Poll::Ready(Err(timeout())) }
                else { operation.as_mut().poll(context) }
            }).await
        }) => result.unwrap_or_else(|_| Err(timeout())),
    }
}

/// 业务作用：在监督边界捕获后台任务异常并同步撤销所有发现资格。
/// 参数说明：`core` 接管任务，`future` 必须服从 owner 取消和有限网络期限。
/// 返回：任务归 owner 持有；异常不会留下仍被认为健康的观察。
pub(crate) fn spawn_owned(
    core: &Arc<Core>,
    future: impl std::future::Future<Output = ()> + Send + 'static,
) -> tokio::task::AbortHandle {
    spawn_task(core, false, future)
}

/// 业务作用：登记会在摘流阶段单独停止的注册任务。
/// 参数说明：`core` 接管任务，`future` 只处理本 owner 的注册记录。
/// 返回：任务可在保留出站发现时被停止并回收。
pub(crate) fn spawn_registration(
    core: &Arc<Core>,
    future: impl std::future::Future<Output = ()> + Send + 'static,
) {
    spawn_task(core, true, future);
}

/// 业务作用：按关闭阶段登记任务，并在异常退出时撤销共享健康事实。
/// 参数说明：`core` 提供监督边界，`registration` 指定摘流阶段，`future` 是有界任务。
/// 返回：任务持有共享状态，不持有控制 owner。
fn spawn_task(
    core: &Arc<Core>,
    registration: bool,
    future: impl std::future::Future<Output = ()> + Send + 'static,
) -> tokio::task::AbortHandle {
    use futures_util::FutureExt;
    let shared = core.clone();
    let task = tokio::spawn(async move {
        if std::panic::AssertUnwindSafe(future)
            .catch_unwind()
            .await
            .is_err()
        {
            // 监督失效后不能继续根据最后成功值发布路由。
            let mut gate = lock(&shared.gate);
            gate.failed = true;
            gate.phase = OwnerPhase::Quiescing;
            shared.stop.cancel();
            shared.registrations_stop.cancel();
            drop(gate);
            changed(&shared);
        }
    });
    let mut tasks = lock(if registration {
        &core.registration_tasks
    } else {
        &core.tasks
    });
    tasks.retain(|task| !task.is_finished());
    let abort = task.abort_handle();
    tasks.push(task);
    abort
}

/// 业务作用：表达已经关闭或失去监督的资源不可继续接纳操作。
/// 参数说明：无。
/// 返回：不包含后端细节的稳定错误。
pub(crate) fn closed() -> DiscoveryError {
    DiscoveryError::new(ErrorCode::OwnerClosed, "discovery owner is closed")
}

/// 业务作用：统一表示共享期限耗尽。
/// 参数说明：无。
/// 返回：可由上层识别的期限错误。
pub(crate) fn timeout() -> DiscoveryError {
    DiscoveryError::new(ErrorCode::DeadlineExceeded, "discovery deadline elapsed")
}

/// 业务作用：错开不同进程和连接的恢复请求，同时保留有限退避上界。
/// 参数说明：`core` 提供进程实例身份，`connection` 限定连接，`maximum_ms` 是当前指数上界，`attempt` 是本地尝试序号。
/// 返回：位于上界一半到上界之间的毫秒时长，不读取业务数据或秘密。
pub(crate) fn retry_delay(
    core: &Core,
    connection: &Connection,
    maximum_ms: u64,
    attempt: u64,
) -> Duration {
    use std::hash::{Hash, Hasher};
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    (&core.id, &connection.name, attempt).hash(&mut hash);
    let floor = maximum_ms.div_ceil(2);
    millis(floor + hash.finish() % (maximum_ms - floor + 1))
}

/// 业务作用：使同一连接的认证撤销同步影响发现与注册健康证据，并通知宿主更新就绪。
/// 参数说明：`core` 提供状态通知，`gate` 必须是其已持有的门禁，`connection` 是发生身份撤销的连接。
/// 返回：递增权限撤销计数并撤销已有资格；其他连接不受影响，旧代迟到结果不能恢复健康。
pub(crate) fn revoke_connection(core: &Core, gate: &Gate, connection: &Connection) {
    connection.revocation.fetch_add(1, Ordering::AcqRel);
    for slot in lock(&connection.slots).values() {
        let mut state = lock(&slot.state);
        // None 还表示静态证据无期限；明确记为已过期，后续失败只能收紧而不能从旧 observed_at 重新生成宽限。
        state.valid_until = Some(Instant::now());
        state.error = Some(DiscoveryError::new(
            ErrorCode::Unauthenticated,
            "connection identity is unavailable",
        ));
        slot.changes
            .send_modify(|value| *value = value.wrapping_add(1));
    }
    // 先在共同门禁内撤销该身份覆盖的全部注册，再通知宿主；不能等待各注册下一次网络回查。
    for record in gate
        .registrations
        .values()
        .filter(|record| std::ptr::eq(record.connection.as_ref(), connection))
    {
        let mut status = lock(&record.status);
        status.valid_until = None;
        if matches!(
            status.state,
            RegistrationState::Active
                | RegistrationState::PendingHealth
                | RegistrationState::Suspended
        ) {
            status.state = RegistrationState::Suspended;
            status.error = Some(DiscoveryError::new(
                ErrorCode::Unauthenticated,
                "registration identity is unavailable",
            ));
        }
    }
    changed(core);
}
