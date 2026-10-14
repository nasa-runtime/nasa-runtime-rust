//! 资源关闭权与只读句柄分离。停机发起同步撤权，等待凭据不拥有清理任务。

use crate::{
    changed, closed,
    config::{invalid, validate_name, ConnectionPolicy, Limits},
    lock, millis, operation,
    registration::{Record, RegistrationSpec, RegistrationStatus},
    spawn_owned, timeout, Connection, Core, DiscoveryHandle, Gate, Quota,
};
use nadisc::managed::{DiscoveryError, ErrorCode, ManagedProvider, Result};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{atomic::AtomicU64, Arc, Mutex},
};
use tokio::{
    sync::{watch, Semaphore},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

/// 发现 owner 的生命周期阶段；关闭后不能重新开放路由。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnerPhase {
    /// 目录已构造，尚未启动后端连接。
    Prepared,
    /// 正在准备连接或确认首次注册。
    Starting,
    /// 已完成发布阶段，可按有效证据提供发现。
    Serving,
    /// 已撤销发布资格，正在摘流或等待业务排空。
    Quiescing,
    /// 资源关闭已开始或完成，不再接纳新发现工作。
    Closed,
}

/// 由宿主提供的具名后端及其冻结策略。
pub struct ConnectionSpec {
    /// 连接的本地 qualifier，不是后端服务名称。
    pub name: String,
    /// 后端类型或实例，决定该连接支持的发现及注册能力。
    pub provider: Arc<dyn ManagedProvider>,
    /// 目标连接或注册的冻结策略。
    pub policy: ConnectionPolicy,
}

/// 在网络活动之前完成的类型化连接目录。
pub struct OwnerBuilder {
    limits: Limits,
    connections: BTreeMap<String, ConnectionSpec>,
    default_connection: Option<String>,
}

impl Default for OwnerBuilder {
    /// 业务作用：建立不连接任何后端的空目录。
    /// 参数说明：无。
    /// 返回：显式添加连接后才产生网络活动。
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            connections: BTreeMap::new(),
            default_connection: None,
        }
    }
}

impl OwnerBuilder {
    /// 业务作用：设置所有连接共享的有限资源预算。
    /// 参数说明：`limits` 是实例、任务和并发容量。
    /// 返回：构造时统一校验的 builder。
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// 业务作用：兼容使用显式默认引用的宿主；新目录可直接将默认连接命名为 primary。
    /// 参数说明：`name` 必须引用已登记的连接，不影响显式 qualifier 的匹配。
    /// 返回：构造时校验引用的 builder；省略此入口时只采用 primary。
    pub fn default_connection(mut self, name: impl Into<String>) -> Self {
        self.default_connection = Some(name.into());
        self
    }

    /// 业务作用：登记一个独立 provider 实例并拒绝重复连接别名。
    /// 参数说明：`spec` 包含名称、后端和冻结策略。
    /// 返回：合法连接加入目录；错误不触发网络访问。
    pub fn connection(mut self, spec: ConnectionSpec) -> Result<Self> {
        validate_name(&spec.name)?;
        spec.policy.validate()?;
        if self.connections.contains_key(&spec.name) {
            return Err(invalid("duplicate connection name"));
        }
        self.connections.insert(spec.name.clone(), spec);
        Ok(self)
    }

    /// 业务作用：创建唯一控制 owner，使初始化取消后仍有资源清理权。
    /// 参数说明：无。
    /// 返回：无网络副作用的 owner；非法显式默认引用拒绝，无 primary 的目录只允许具名选择。
    pub fn build(self) -> Result<DiscoveryOwner> {
        self.limits.validate()?;
        if self.connections.len() > self.limits.max_connections {
            return Err(invalid("connection capacity exceeded"));
        }
        let default_connection = match self.default_connection {
            Some(name) if self.connections.contains_key(&name) => Some(name),
            Some(_) => return Err(invalid("unknown default connection")),
            None => self
                .connections
                .contains_key("primary")
                .then(|| "primary".into()),
        };
        let connections = self
            .connections
            .into_iter()
            .map(|(name, spec)| {
                let connection = Arc::new(Connection {
                    name: name.clone(),
                    provider: spec.provider,
                    policy: spec.policy,
                    slots: Mutex::new(BTreeMap::new()),
                    prepared: watch::channel(false).0,
                    error: Mutex::new(None),
                    epoch: AtomicU64::new(1),
                    revocation: AtomicU64::new(0),
                    queries: AtomicU64::new(0),
                    query_errors: AtomicU64::new(0),
                    query_micros: AtomicU64::new(0),
                });
                (name, connection)
            })
            .collect();
        let core = Arc::new(Core {
            id: uuid::Uuid::new_v4().to_string(),
            gate: Mutex::new(Gate {
                phase: OwnerPhase::Prepared,
                registrations: BTreeMap::new(),
                frozen: false,
                close_started: false,
                failed: false,
                shutdown_deadline: None,
            }),
            connections,
            default_connection,
            operations: Semaphore::new(self.limits.max_parallel_operations),
            watches: Semaphore::new(self.limits.max_watch_streams),
            quota: Arc::new(Mutex::new(Quota::default())),
            bridges: Arc::new(Semaphore::new(
                self.limits
                    .max_service_watches_per_connection
                    .saturating_mul(self.limits.max_connections),
            )),
            transports: Arc::new(Semaphore::new(self.limits.max_transport_tasks)),
            limits: self.limits,
            tasks: Mutex::new(Vec::new()),
            registration_tasks: Mutex::new(Vec::new()),
            withdrawal_task: Mutex::new(None),
            close_task: Mutex::new(None),
            stop: CancellationToken::new(),
            registrations_stop: CancellationToken::new(),
            changes: watch::channel(0).0,
            close: watch::channel(None).0,
            withdrawn: watch::channel(None).0,
        });
        Ok(DiscoveryOwner { core })
    }
}

/// 非 Clone 的控制对象；查询句柄和等待凭据不继承关闭权。
pub struct DiscoveryOwner {
    core: Arc<Core>,
}

impl DiscoveryOwner {
    /// 业务作用：启动独立连接监督并等待必需连接达到所需作用域的访问条件。
    /// 参数说明：`deadline` 是宿主原启动截止点，取消等待不会丢失后台资源归属。
    /// 返回：必需连接准备成功；关闭或期限耗尽返回错误。
    pub async fn prepare(&self, deadline: Instant) -> Result<()> {
        if Instant::now() >= deadline {
            return Err(timeout());
        }
        {
            let mut gate = lock(&self.core.gate);
            match gate.phase {
                OwnerPhase::Prepared => {
                    gate.phase = OwnerPhase::Starting;
                    for connection in self.core.connections.values() {
                        let (core, connection) = (self.core.clone(), connection.clone());
                        spawn_owned(
                            &self.core,
                            async move { prepare_loop(core, connection).await },
                        );
                    }
                }
                OwnerPhase::Starting => {}
                _ => return Err(invalid("owner cannot prepare in its current phase")),
            }
        }
        let mut changes = self.core.changes.subscribe();
        loop {
            if self.core.stop.is_cancelled() {
                return Err(closed());
            }
            if Instant::now() >= deadline {
                return Err(timeout());
            }
            let ready = self
                .core
                .connections
                .values()
                .filter(|connection| connection.policy.startup_required)
                .all(|connection| *connection.prepared.borrow());
            if ready {
                return Ok(());
            }
            tokio::time::timeout_at(deadline, changes.changed())
                .await
                .map_err(|_| timeout())?
                .map_err(|_| closed())?;
        }
    }

    /// 业务作用：取得精确连接或默认 primary，不跨环境回退。
    /// 参数说明：`qualifier` 为空时采用 primary；兼容宿主显式设置的默认引用优先。
    /// 返回：只有查询权的句柄；不存在或已关闭时失败。
    pub fn client(&self, qualifier: Option<&str>) -> Result<DiscoveryHandle> {
        let gate = lock(&self.core.gate);
        if gate.close_started || gate.failed {
            return Err(closed());
        }
        let name = qualifier
            .or(self.core.default_connection.as_deref())
            .ok_or_else(|| invalid("no default discovery connection"))?;
        let connection = self
            .core
            .connections
            .get(name)
            .cloned()
            .ok_or_else(|| invalid("unknown discovery connection"))?;
        Ok(DiscoveryHandle {
            core: Arc::downgrade(&self.core),
            connection,
        })
    }

    /// 业务作用：一次性校验并冻结全部最终注册计划，避免部分发布后才发现引用非法。
    /// 参数说明：`specs` 已解析实际 listener、对外端点和本地资格来源。
    /// 返回：全部计划合法时原子登记，任何错误均不发出注册请求。
    pub fn freeze_registrations(&self, specs: Vec<RegistrationSpec>) -> Result<()> {
        let mut gate = lock(&self.core.gate);
        if gate.frozen || gate.phase != OwnerPhase::Starting {
            return Err(invalid(
                "registration plans are already frozen or owner is not starting",
            ));
        }
        if specs.len() > self.core.limits.max_registrations {
            return Err(invalid("registration capacity exceeded"));
        }
        let mut records = BTreeMap::new();
        let mut keys = BTreeSet::new();
        for mut spec in specs {
            validate_name(&spec.plan.id)?;
            let connection = self
                .core
                .connections
                .get(&spec.connection_ref)
                .cloned()
                .ok_or_else(|| invalid("unknown registration connection"))?;
            spec.policy.validate(&connection.policy)?;
            if !connection.provider.capabilities().registration {
                return Err(invalid("registration provider is read only"));
            }
            if spec
                .plan
                .instance
                .metadata
                .contains_key("nasa.discovery.owner")
            {
                return Err(invalid("registration ownership metadata is reserved"));
            }
            spec.plan
                .instance
                .metadata
                .insert("nasa.discovery.owner".into(), self.core.id.clone());
            let instance = &spec.plan.instance;
            if instance.ip.trim() != instance.ip
                || instance.ip.is_empty()
                || instance.port == 0
                || instance.ip == "0.0.0.0"
                || instance.ip == "::"
                || instance.ip == "[::]"
                || !instance.weight.is_finite()
                || instance.weight < 0.0
                || spec.plan.service.is_empty()
            {
                return Err(invalid("registration endpoint or weight is invalid"));
            }
            connection.provider.validate_registration(&spec.plan)?;
            if !keys.insert(connection.provider.registration_key(&spec.plan))
                || records.contains_key(&spec.plan.id)
            {
                return Err(invalid(
                    "duplicate external registration key or local registration name",
                ));
            }
            records.insert(
                spec.plan.id.clone(),
                Arc::new(Record::new(spec, connection)),
            );
        }
        gate.registrations = records;
        gate.frozen = true;
        Ok(())
    }

    /// 业务作用：在宿主监听器可接流后启动注册监督，等待启动必需目标的真实确认。
    /// 参数说明：`deadline` 延续原启动期限，不为注册重新计时。
    /// 返回：全部启动必需注册确认后进入 Serving；失败由宿主发起有界清理。
    pub async fn publish(&self, deadline: Instant) -> Result<()> {
        if Instant::now() >= deadline {
            return Err(timeout());
        }
        {
            let gate = lock(&self.core.gate);
            if !gate.frozen
                || gate.phase != OwnerPhase::Starting
                || gate.failed
                || gate.close_started
            {
                return Err(invalid("owner is not ready to publish frozen plans"));
            }
            for record in gate.registrations.values() {
                if !record
                    .started
                    .swap(true, std::sync::atomic::Ordering::AcqRel)
                {
                    let (core, record) = (self.core.clone(), record.clone());
                    crate::spawn_registration(&self.core, async move {
                        crate::registration::run(core, record).await
                    });
                }
            }
        }
        let mut changes = self.core.changes.subscribe();
        loop {
            {
                let mut gate = lock(&self.core.gate);
                if gate.phase != OwnerPhase::Starting || gate.failed || gate.close_started {
                    return Err(closed());
                }
                // 全部证据同时有效也不能延长宿主原启动期限。
                if Instant::now() >= deadline {
                    return Err(timeout());
                }
                let ready = gate
                    .registrations
                    .values()
                    .filter(|record| record.spec.policy.startup_required)
                    .all(|record| record.current_status(&gate).is_ready());
                if ready {
                    gate.phase = OwnerPhase::Serving;
                    return Ok(());
                }
            }
            tokio::time::timeout_at(deadline, changes.changed())
                .await
                .map_err(|_| timeout())?
                .map_err(|_| closed())?;
        }
    }

    /// 业务作用：同步撤销所有新注册和健康上报许可，保留出站发现供业务排空。
    /// 参数说明：`deadline` 是摘流、业务排空与最终关闭共享的停机截止点。
    /// 返回：重复调用等待同一摘流结果；出站资源保留到 begin_close。
    pub fn begin_quiesce(&self, deadline: Instant) -> QuiesceTicket {
        begin_quiesce(self.core.clone(), deadline)
    }

    /// 业务作用：发起唯一有界清理并同步撤销后续出站发送，等待取消不终止清理。
    /// 参数说明：`deadline` 是宿主停机总截止点，首次调用后不能延长。
    /// 返回：可复制的只读关闭等待凭据。
    pub fn begin_close(&self, deadline: Instant) -> CloseTicket {
        begin_close(self.core.clone(), deadline)
    }

    /// 业务作用：读取当前 owner 阶段。
    /// 参数说明：无。
    /// 返回：单调推进的生命周期阶段。
    pub fn phase(&self) -> OwnerPhase {
        lock(&self.core.gate).phase
    }

    /// 业务作用：读取注册监督事实，供宿主映射就绪贡献和运维状态。
    /// 参数说明：无。
    /// 返回：按当前连接身份代次及本地资格裁决的状态；已撤权的注册不得继续贡献就绪，不输出凭据或 metadata。
    pub fn registrations(&self) -> Vec<RegistrationStatus> {
        let gate = lock(&self.core.gate);
        gate.registrations
            .values()
            .map(|record| record.current_status(&gate))
            .collect()
    }

    /// 业务作用：提供状态变化通知，允许宿主按同一事实监督而不复制后端轮询。
    /// 参数说明：无。
    /// 返回：只读通知流，收到通知后重新读取完整状态。
    pub fn changes(&self) -> watch::Receiver<u64> {
        self.core.changes.subscribe()
    }

    /// 业务作用：向监控与运维代码提供没有关闭权的有界状态入口。
    /// 参数说明：无。
    /// 返回：观察器不延长 owner 的生命周期，也不创建后端任务。
    pub fn observer(&self) -> crate::RuntimeObserver {
        crate::RuntimeObserver {
            core: Arc::downgrade(&self.core),
        }
    }

    /// 业务作用：在冻结发布计划前让宿主比较后端查询作用域，防止别名掩盖协议投影冲突。
    /// 参数说明：`connection` 和 `service` 必须属于当前 owner 的冻结目录。
    /// 返回：provider 的安全查询键；未知连接或服务拒绝，不产生网络请求。
    pub fn service_key(&self, connection: &str, service: &str) -> Result<String> {
        self.core
            .connections
            .get(connection)
            .ok_or_else(|| invalid("unknown discovery connection"))?
            .provider
            .service_key(service)
    }
}

impl Drop for DiscoveryOwner {
    /// 业务作用：控制对象丢弃时立即撤权，并在 Tokio 仍存在时触发有限清理。
    /// 参数说明：无。
    /// 返回：显式关闭以外的兜底不保证 runtime 退出后的网络清理。
    fn drop(&mut self) {
        if !lock(&self.core.gate).close_started {
            if tokio::runtime::Handle::try_current().is_ok() {
                begin_close(self.core.clone(), Instant::now() + millis(3000));
            } else {
                let mut gate = lock(&self.core.gate);
                gate.phase = OwnerPhase::Closed;
                gate.close_started = true;
                self.core.stop.cancel();
                self.core.registrations_stop.cancel();
                for task in lock(&self.core.tasks).drain(..) {
                    task.abort();
                }
                for task in lock(&self.core.registration_tasks).drain(..) {
                    task.abort();
                }
            }
        }
    }
}

/// 关闭完成后的注册残留、后端错误和期限摘要。
#[derive(Clone, Debug)]
pub struct CloseReport {
    /// 注册目标集合，包含各目标独立的状态或发布策略。
    pub registrations: Vec<RegistrationStatus>,
    /// 按连接 qualifier 记录的后端关闭错误。
    pub provider_errors: BTreeMap<String, DiscoveryError>,
    /// 关闭总期限是否耗尽；为真时仍需核对注册残留。
    pub deadline_elapsed: bool,
}

/// 关闭结果的等待凭据；丢弃凭据不取消后台清理。
#[derive(Clone)]
pub struct CloseTicket {
    receiver: watch::Receiver<Option<CloseReport>>,
}
impl CloseTicket {
    /// 业务作用：等待已发起清理的同一结果，取消本次等待不取消注销。
    /// 参数说明：无。
    /// 返回：包括结果不明和残留的关闭摘要。
    pub async fn wait(mut self) -> Result<CloseReport> {
        loop {
            if let Some(report) = self.receiver.borrow().clone() {
                return Ok(report);
            }
            self.receiver.changed().await.map_err(|_| closed())?;
        }
    }
}

/// 摘流结果的等待凭据；保留业务排空所需的发现连接。
#[derive(Clone)]
pub struct QuiesceTicket {
    receiver: watch::Receiver<Option<Vec<RegistrationStatus>>>,
}
impl QuiesceTicket {
    /// 业务作用：等待注册摘流而不关闭业务排空需要的发现连接。
    /// 参数说明：无。
    /// 返回：全部目标的注销或残留事实，等待取消不停止摘流。
    pub async fn wait(mut self) -> Result<Vec<RegistrationStatus>> {
        loop {
            if let Some(statuses) = self.receiver.borrow().clone() {
                return Ok(statuses);
            }
            self.receiver.changed().await.map_err(|_| closed())?;
        }
    }
}

/// 业务作用：冻结停机总期限并在撤权后接管注册摘流工作。
/// 参数说明：`core` 是受管状态，`deadline` 首次调用后不能延长。
/// 返回：单次摘流的只读凭据。
fn begin_quiesce(core: Arc<Core>, deadline: Instant) -> QuiesceTicket {
    let mut gate = lock(&core.gate);
    let ticket = QuiesceTicket {
        receiver: core.withdrawn.subscribe(),
    };
    if gate.shutdown_deadline.is_none() {
        gate.shutdown_deadline = Some(deadline);
        gate.phase = OwnerPhase::Quiescing;
        core.registrations_stop.cancel();
        let shared = core.clone();
        *lock(&core.withdrawal_task) = Some(tokio::spawn(async move {
            use futures_util::FutureExt;
            let _ = std::panic::AssertUnwindSafe(cleanup_registrations(shared.clone(), deadline))
                .catch_unwind()
                .await;
            finish_withdrawal(&shared);
        }));
    }
    ticket
}

/// 业务作用：在资源关闭门禁内冻结首次截止点并接管清理工作。
/// 参数说明：`core` 是共享资源，`deadline` 只在第一次关闭时生效。
/// 返回：所有调用观察同一关闭结果。
fn begin_close(core: Arc<Core>, deadline: Instant) -> CloseTicket {
    begin_quiesce(core.clone(), deadline);
    let mut gate = lock(&core.gate);
    let ticket = CloseTicket {
        receiver: core.close.subscribe(),
    };
    if !gate.close_started {
        // 先关闭发送与发布许可，再启动异步清理，避免等待者取消后继续接流。
        gate.close_started = true;
        gate.phase = OwnerPhase::Quiescing;
        core.registrations_stop.cancel();
        core.stop.cancel();
        let deadline = gate.shutdown_deadline.unwrap_or(deadline).min(deadline);
        let shared = core.clone();
        *lock(&core.close_task) = Some(tokio::spawn(async move {
            use futures_util::FutureExt;
            if std::panic::AssertUnwindSafe(cleanup(shared.clone(), deadline))
                .catch_unwind()
                .await
                .is_err()
            {
                finish_withdrawal(&shared);
                lock(&shared.gate).phase = OwnerPhase::Closed;
                shared.close.send_replace(Some(CloseReport {
                    registrations: lock(&shared.gate)
                        .registrations
                        .values()
                        .map(|r| lock(&r.status).clone())
                        .collect(),
                    provider_errors: shared
                        .connections
                        .keys()
                        .map(|name| {
                            (
                                name.clone(),
                                DiscoveryError::new(
                                    nadisc::managed::ErrorCode::Unavailable,
                                    "provider cleanup was interrupted",
                                ),
                            )
                        })
                        .collect(),
                    deadline_elapsed: Instant::now() >= deadline,
                }));
            }
        }));
    }
    ticket
}

/// 业务作用：每连接独立准备和退避，不让可选连接阻塞其他环境。
/// 参数说明：`core` 提供共享预算，`connection` 是单一身份作用域。
/// 返回：准备成功或关闭后退出；后续发现和注册各自更新真实证据。
async fn prepare_loop(core: Arc<Core>, connection: Arc<Connection>) {
    let mut backoff = connection.policy.initial_backoff_ms;
    let mut attempt = 0u64;
    loop {
        let result = operation(
            &core,
            Instant::now() + millis(connection.policy.request_timeout_ms),
            connection.provider.prepare(),
        )
        .await;
        if core.stop.is_cancelled() {
            break;
        }
        match result {
            Ok(()) => {
                connection.prepared.send_replace(true);
                *lock(&connection.error) = None;
                changed(&core);
                return;
            }
            Err(error) => {
                if matches!(
                    error.code,
                    ErrorCode::Unauthenticated
                        | ErrorCode::PermissionDenied
                        | ErrorCode::PermissionFiltered
                ) {
                    let gate = lock(&core.gate);
                    crate::revoke_connection(&core, &gate, &connection);
                }
                *lock(&connection.error) = Some(error);
                changed(&core);
            }
        }
        tokio::select! { _ = core.stop.cancelled() => break, _ = tokio::time::sleep(crate::retry_delay(&core, &connection, backoff, attempt)) => {} }
        attempt = attempt.wrapping_add(1);
        backoff = backoff
            .saturating_mul(2)
            .min(connection.policy.max_backoff_ms);
    }
}

/// 业务作用：先终止本地写任务，再并行尝试各目标清理并报告不能证明删除的记录。
/// 参数说明：`core` 保留所有已尝试写入事实，`deadline` 覆盖全部等待与后端关闭。
/// 返回：发布一次最终摘要，不因单个后端失败放弃其他目标。
async fn cleanup_registrations(core: Arc<Core>, deadline: Instant) {
    use futures_util::{stream, StreamExt};
    let tasks = std::mem::take(&mut *lock(&core.registration_tasks));
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        let _ = tokio::time::timeout_at(deadline, task).await;
    }
    let records: Vec<_> = lock(&core.gate).registrations.values().cloned().collect();
    let count = records.len().max(1);
    let share = deadline.saturating_duration_since(Instant::now())
        / count.div_ceil(core.limits.max_parallel_operations) as u32;
    stream::iter(records.iter().cloned())
        .for_each_concurrent(core.limits.max_parallel_operations, |record| async move {
            use futures_util::FutureExt;
            let _ = std::panic::AssertUnwindSafe(crate::registration::withdraw(
                record, deadline, share,
            ))
            .catch_unwind()
            .await;
        })
        .await;
}

/// 业务作用：将未完成的摘流记录转换为明确残留，并唤醒全部等待者。
/// 参数说明：`core` 的注册写任务必须已停止，或正在进入全局失败关闭。
/// 返回：最终摘要不会包含看似仍在正常执行的 Withdrawing 状态。
fn finish_withdrawal(core: &Core) {
    let gate = lock(&core.gate);
    for record in gate.registrations.values() {
        crate::registration::finish_withdrawal(record);
    }
    core.withdrawn.send_replace(Some(
        gate.registrations
            .values()
            .map(|record| lock(&record.status).clone())
            .collect(),
    ));
    changed(core);
}

/// 业务作用：业务排空后结束出站观察，等待既有摘流并关闭全部 provider。
/// 参数说明：`core` 保留清理记录，`deadline` 不晚于首次摘流确定的总期限。
/// 返回：发布最终资源清理摘要，不抹去注销残留。
async fn cleanup(core: Arc<Core>, deadline: Instant) {
    use futures_util::{stream, StreamExt};
    let withdrawal = lock(&core.withdrawal_task).take();
    if let Some(mut task) = withdrawal {
        if tokio::time::timeout_at(deadline, &mut task).await.is_err() {
            task.abort();
            let _ = task.await;
        }
    }
    // 先终止摘流任务再冻结摘要，避免关闭报告与迟到状态写入互相覆盖。
    finish_withdrawal(&core);
    let tasks = std::mem::take(&mut *lock(&core.tasks));
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        let _ = tokio::time::timeout_at(deadline, task).await;
    }
    let records: Vec<_> = lock(&core.gate).registrations.values().cloned().collect();
    let providers = stream::iter(core.connections.values().cloned())
        .map(|connection| async move {
            use futures_util::FutureExt;
            let result = tokio::time::timeout_at(
                deadline,
                std::panic::AssertUnwindSafe(connection.provider.close()).catch_unwind(),
            )
            .await
            .map(|result| {
                result.unwrap_or_else(|_| {
                    Err(DiscoveryError::new(
                        nadisc::managed::ErrorCode::Unavailable,
                        "provider cleanup was interrupted",
                    ))
                })
            })
            .unwrap_or_else(|_| Err(timeout()));
            (connection.name.clone(), result)
        })
        .buffer_unordered(core.limits.max_parallel_operations)
        .collect::<Vec<_>>()
        .await;
    let provider_errors = providers
        .into_iter()
        .filter_map(|(name, result)| result.err().map(|error| (name, error)))
        .collect();
    let registrations = records
        .iter()
        .map(|record| lock(&record.status).clone())
        .collect();
    lock(&core.gate).phase = OwnerPhase::Closed;
    // 清理结果保留 Residual，不能把补偿响应当作迟到旧写已结束的证明。
    core.close.send_replace(Some(CloseReport {
        registrations,
        provider_errors,
        deadline_elapsed: Instant::now() >= deadline,
    }));
    changed(&core);
}
