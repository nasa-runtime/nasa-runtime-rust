//! 共享观察与发送资格。有效性槽独立于快照分配额度，撤权不需要分配实例数组。

use crate::{
    closed, lock, millis, operation, spawn_owned, timeout, Connection, Core, OwnerPhase, Quota,
};
use nadisc::managed::{DiscoveryError, ErrorCode, EvidenceKind, Result};
use nadisc::{
    is_traffic_instance, DiscoveryClient, Instance, ServiceWatch, ServiceWatchGuard, WatchOptions,
};
use std::{
    collections::BTreeSet,
    sync::{atomic::Ordering, Arc, Mutex, Weak},
};
use tokio::{sync::watch, time::Instant};
use tokio_util::sync::CancellationToken;

pub(crate) struct Slot {
    pub service: String,
    pub state: Mutex<State>,
    pub changes: watch::Sender<u64>,
}

pub(crate) struct State {
    data: Option<Arc<Data>>,
    pub valid_until: Option<Instant>,
    pub observed_at: Option<Instant>,
    pub error: Option<DiscoveryError>,
    sequence: u64,
    epoch: u64,
    revocation: u64,
    touched: Instant,
}

struct Data {
    instances: Vec<Instance>,
    identities: Vec<String>,
    evidence: EvidenceKind,
    bytes: usize,
    quota: Arc<Mutex<Quota>>,
}

impl Drop for Data {
    /// 业务作用：最后一个受管引用释放时归还旧快照占用的配额。
    /// 参数说明：无。
    /// 返回：慢消费者持有的旧代在释放前持续计入总容量。
    fn drop(&mut self) {
        let mut quota = lock(&self.quota);
        quota.instances -= self.instances.len();
        quota.bytes -= self.bytes;
    }
}

/// 保留受管数据的只读凭据；数据用于诊断或选址，提交请求仍需再次检查 Observation。
#[derive(Clone)]
pub struct Snapshot {
    data: Arc<Data>,
    /// 产生证据的 owner 标识，用于区分独立运行时。
    pub owner_id: String,
    /// 证据所属连接的 qualifier。
    pub connection: String,
    /// 证据对应的逻辑服务名。
    pub service: String,
    /// 后端身份代次；失权前的证据不能跨代继续使用。
    pub epoch: u64,
    /// 同一观察中的快照序号，用于区分实例集合变化。
    pub sequence: u64,
    /// 当前证据被确认的本地单调时刻。
    pub observed_at: Instant,
    /// 当前证据的有效截止时刻；不替代提交请求时的复验。
    pub valid_until: Option<Instant>,
}

impl Snapshot {
    /// 业务作用：读取后端完整实例事实，包括不承载流量的实例。
    /// 参数说明：无。
    /// 返回：共享只读列表；该切片本身不构成持续发送许可。
    pub fn instances(&self) -> &[Instance] {
        &self.data.instances
    }

    /// 业务作用：提供与完整实例列表逐项绑定的后端身份，供诊断和自定义协议保留所有权边界。
    /// 参数说明：无。
    /// 返回：当前连接及服务作用域内的稳定键，不能用作无界指标标签。
    pub fn identities(&self) -> &[String] {
        &self.data.identities
    }

    /// 业务作用：区分地址解析、静态配置与后端健康证据。
    /// 参数说明：无。
    /// 返回：该快照真实采用的证据等级。
    pub fn evidence(&self) -> EvidenceKind {
        self.data.evidence
    }
}

/// 只有观察权的具名连接句柄，不能关闭其他消费者使用的连接。
#[derive(Clone)]
pub struct DiscoveryHandle {
    pub(crate) core: Weak<Core>,
    pub(crate) connection: Arc<Connection>,
}

/// 服务观察凭据。变更通知可合并，当前有效性始终从固定状态槽重新读取。
pub struct Observation {
    core: Weak<Core>,
    connection: Arc<Connection>,
    slot: Arc<Slot>,
    changes: watch::Receiver<u64>,
}

impl Clone for Observation {
    /// 业务作用：增加当前目标的观察需求，共享已有后端查询。
    /// 参数说明：无。
    /// 返回：独立通知游标，不启动第二个轮询。
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
            connection: self.connection.clone(),
            slot: self.slot.clone(),
            changes: self.changes.clone(),
        }
    }
}

impl DiscoveryHandle {
    /// 业务作用：取得连接内服务的共享观察，首个等待者取消不影响其他需求。
    /// 参数说明：`service` 是后端名或明确绑定别名，`deadline` 是调用方总期限。
    /// 返回：总期限内取得共享证据后返回凭据；过期调用不取得缓存或创建需求，失败、容量不足和关闭返回错误。
    pub async fn observe(&self, service: &str, deadline: Instant) -> Result<Observation> {
        // 调用方已经失去等待预算时不能触碰缓存或占用观察额度，也不能派生无需求的查询。
        if Instant::now() >= deadline {
            return Err(timeout());
        }
        let core = self.core.upgrade().ok_or_else(closed)?;
        if service.is_empty() || service.len() > 512 || service.trim() != service {
            return Err(crate::config::invalid("invalid discovery service"));
        }
        let mut observation = {
            let gate = lock(&core.gate);
            if gate.failed
                || matches!(gate.phase, OwnerPhase::Prepared | OwnerPhase::Closed)
                || gate.close_started
            {
                return Err(closed());
            }
            let mut slots = lock(&self.connection.slots);
            // 门禁与观察槽的锁等待也消耗总期限，必须在复用或分配需求之前再次核验。
            if Instant::now() >= deadline {
                return Err(timeout());
            }
            let slot = if let Some(slot) = slots.get(service) {
                let mut state = lock(&slot.state);
                if Instant::now() >= deadline {
                    return Err(timeout());
                }
                state.touched = Instant::now();
                slot.clone()
            } else {
                if slots.len() >= core.limits.max_service_watches_per_connection {
                    return Err(DiscoveryError::new(
                        ErrorCode::CapacityExceeded,
                        "service observation capacity reached",
                    ));
                }
                let slot = Arc::new(Slot {
                    service: service.into(),
                    state: Mutex::new(State {
                        data: None,
                        valid_until: None,
                        observed_at: None,
                        error: None,
                        sequence: 0,
                        epoch: 0,
                        revocation: 0,
                        touched: Instant::now(),
                    }),
                    changes: watch::channel(0).0,
                });
                slots.insert(service.into(), slot.clone());
                let (shared, connection, target) =
                    (core.clone(), self.connection.clone(), slot.clone());
                spawn_owned(&core, async move {
                    observe_loop(shared, connection, target).await
                });
                slot
            };
            Observation {
                core: self.core.clone(),
                connection: self.connection.clone(),
                changes: slot.changes.subscribe(),
                slot,
            }
        };
        loop {
            // 通知与期限可能同时就绪；单个等待者过期只释放自己的需求，不取消其他等待者的共享查询。
            if Instant::now() >= deadline {
                return Err(timeout());
            }
            if lock(&observation.slot.state).sequence != 0 {
                let snapshot = observation.snapshot(false);
                // 读取证据时的锁竞争不能使成功结果越过调用方总期限。
                if Instant::now() >= deadline {
                    return Err(timeout());
                }
                snapshot?;
                return Ok(observation);
            }
            tokio::select! {
                _ = core.stop.cancelled() => return Err(closed()),
                result = tokio::time::timeout_at(deadline, observation.changes.changed()) => {
                    result.map_err(|_| timeout())?.map_err(|_| closed())?;
                }
            }
        }
    }

    /// 业务作用：显示连接的精确名称，避免跨环境猜测默认目标。
    /// 参数说明：无。
    /// 返回：构造时冻结的连接名。
    pub fn name(&self) -> &str {
        &self.connection.name
    }
}

impl Observation {
    /// 业务作用：按当前资格读取完整快照，严格调用可拒绝任何最近一次查询错误。
    /// 参数说明：`strict` 为 true 时不接受异常宽限。
    /// 返回：有效证据快照；关闭、超龄或撤权不返回旧路由。
    pub fn snapshot(&self, strict: bool) -> Result<Snapshot> {
        let core = self.core.upgrade().ok_or_else(closed)?;
        let gate = lock(&core.gate);
        if gate.failed || gate.close_started || gate.phase == OwnerPhase::Closed {
            return Err(closed());
        }
        let state = lock(&self.slot.state);
        check(&self.connection, &state, strict)?;
        Ok(Snapshot {
            data: state.data.as_ref().ok_or_else(closed)?.clone(),
            owner_id: core.id.clone(),
            connection: self.connection.name.clone(),
            service: self.slot.service.clone(),
            epoch: state.epoch,
            sequence: state.sequence,
            observed_at: state.observed_at.ok_or_else(closed)?,
            valid_until: state.valid_until,
        })
    }

    /// 业务作用：等待证据变化或有效期结束，避免无变化时永久保留旧值。
    /// 参数说明：`deadline` 是本次等待上限。
    /// 返回：收到通知或真实证据期限到达时成功，调用方须重新读取 snapshot；关闭或仅调用方等待超时返回错误，静态证据没有到期期限。
    pub async fn changed(&mut self, deadline: Instant) -> Result<()> {
        let core = self.core.upgrade().ok_or_else(closed)?;
        let expiry = lock(&self.slot.state).valid_until;
        let wake_at = expiry.map_or(deadline, |expiry| expiry.min(deadline));
        tokio::select! {
            _ = core.stop.cancelled() => Err(closed()),
            result = self.changes.changed() => result.map_err(|_| closed()),
            // 等待上限不是证据事件；只有存在且先到达的证据期限才能触发成功通知。
            _ = tokio::time::sleep_until(wake_at) => if expiry.is_some_and(|expiry| expiry <= deadline) { Ok(()) } else { Err(timeout()) },
        }
    }

    /// 业务作用：在撤权共用的同步保护范围内提交一次传输。
    /// 参数说明：`instance` 必须仍是当前健康实例；`submit` 不得阻塞或重入发现接口。
    /// 返回：资格仍成立时执行回调；否则不提交。
    pub fn submit_if_current<T>(
        &self,
        instance: &Instance,
        submit: impl FnOnce() -> T,
    ) -> Result<T> {
        self.submit(instance, false, submit)
    }

    /// 业务作用：在强一致业务提交处同时拒绝最新查询失败和候选撤销，不使用异常宽限。
    /// 参数说明：`instance` 是此前选中的完整实例，`submit` 是不阻塞且不重入的同步提交。
    /// 返回：当前完整证据允许该候选才执行动作。
    pub fn submit_if_current_strict<T>(
        &self,
        instance: &Instance,
        submit: impl FnOnce() -> T,
    ) -> Result<T> {
        self.submit(instance, true, submit)
    }

    /// 业务作用：在同一保护范围执行严格或普通提交裁决。
    /// 参数说明：`instance` 是完整候选，`strict` 禁止异常宽限，`submit` 只能同步提交。
    /// 返回：失权时不调用动作，成功不代表远端已执行。
    fn submit<T>(
        &self,
        instance: &Instance,
        strict: bool,
        submit: impl FnOnce() -> T,
    ) -> Result<T> {
        let core = self.core.upgrade().ok_or_else(closed)?;
        let gate = lock(&core.gate);
        if gate.failed || gate.close_started || gate.phase == OwnerPhase::Closed {
            return Err(closed());
        }
        let state = lock(&self.slot.state);
        check(&self.connection, &state, strict)?;
        if !is_traffic_instance(instance)
            || !state
                .data
                .as_ref()
                .is_some_and(|data| data.instances.contains(instance))
        {
            return Err(DiscoveryError::new(
                ErrorCode::NoInstances,
                "candidate no longer has routing eligibility",
            ));
        }
        Ok(submit())
    }
}

/// 业务作用：统一检查数据代次、错误宽限和固定截止点。
/// 参数说明：`connection` 提供当前身份代次，`state` 是锁定的证据槽，`strict` 禁用宽限。
/// 返回：只有仍可用于调用的证据通过；读取不会刷新时间。
pub(crate) fn check(connection: &Connection, state: &State, strict: bool) -> Result<()> {
    if state.epoch != connection.epoch.load(Ordering::Acquire)
        || state.revocation != connection.revocation.load(Ordering::Acquire)
    {
        return Err(closed());
    }
    if strict && state.error.is_some() {
        return Err(state.error.clone().unwrap());
    }
    if state.data.is_none()
        || state.observed_at.is_none()
        || state
            .valid_until
            .is_some_and(|deadline| Instant::now() >= deadline)
        || (state.valid_until.is_none()
            && state
                .data
                .as_ref()
                .is_none_or(|data| data.evidence != EvidenceKind::Static))
    {
        return Err(state.error.clone().unwrap_or_else(|| {
            DiscoveryError::new(
                ErrorCode::Unavailable,
                "discovery evidence is unavailable or expired",
            )
        }));
    }
    Ok(())
}

/// 业务作用：让轮询和 watch 失败使用相同的证据裁决，重复失败只能缩短既有发送期限。
/// 参数说明：`connection` 提供冻结的年龄与宽限，`state` 是当前证据槽，`error` 区分同步不完整和明确撤权事实。
/// 返回：保存错误供严格读取拒绝；普通读取最多沿用最后一次成功时刻起的较短宽限，不恢复已经失效的路由。
fn record_failure(connection: &Connection, state: &mut State, error: &DiscoveryError) {
    let grace = if error.revokes_evidence() {
        0
    } else {
        connection
            .policy
            .stale_if_error_ms
            .min(connection.policy.max_age_ms)
    };
    let failure_deadline = state
        .observed_at
        .map_or(Instant::now(), |at| at + millis(grace));
    // 错误不会产生新证据；保留已撤权的更早截止点，避免后续普通网络失败重新开放旧路由。
    state.valid_until = Some(
        state
            .valid_until
            .map_or(failure_deadline, |current| current.min(failure_deadline)),
    );
    state.error = Some(error.clone());
}

/// 业务作用：执行一个服务的唯一完整查询序列并在无人使用后回收。
/// 参数说明：`core` 接管任务，`connection` 提供作用域，`slot` 保存固定有效性槽。
/// 返回：关闭或空闲回收后终止，不在内部启动其他轮询。
async fn observe_loop(core: Arc<Core>, connection: Arc<Connection>, slot: Arc<Slot>) {
    let mut backoff = connection.policy.initial_backoff_ms;
    loop {
        let epoch = connection.epoch.load(Ordering::Acquire);
        let revocation = connection.revocation.load(Ordering::Acquire);
        let deadline = Instant::now() + millis(connection.policy.request_timeout_ms);
        let started = Instant::now();
        let result = operation(&core, deadline, connection.provider.observe(&slot.service)).await;
        let failed = result.is_err();
        connection.queries.fetch_add(1, Ordering::Relaxed);
        connection.query_micros.fetch_add(
            started.elapsed().as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        if result.is_err() {
            connection.query_errors.fetch_add(1, Ordering::Relaxed);
        }
        {
            let gate = lock(&core.gate);
            if gate.failed || gate.close_started || core.stop.is_cancelled() {
                break;
            }
            if epoch == connection.epoch.load(Ordering::Acquire)
                && revocation == connection.revocation.load(Ordering::Acquire)
            {
                let mut state = lock(&slot.state);
                let result = result.and_then(|snapshot| {
                    if let Some(data) = state.data.as_ref().filter(|data| {
                        data.evidence == snapshot.evidence && data.instances == snapshot.instances
                    }) {
                        Ok(data.clone())
                    } else {
                        // 完整结果已明确撤下旧候选时，即使新代因容量拒绝，也不能借同步失败宽限继续发送旧候选。
                        if state.data.as_ref().is_some_and(|data| {
                            data.instances
                                .iter()
                                .filter(|instance| is_traffic_instance(instance))
                                .any(|instance| !snapshot.instances.contains(instance))
                        }) {
                            state.valid_until = Some(Instant::now());
                        }
                        allocate(
                            &core,
                            &connection,
                            snapshot.instances,
                            snapshot.evidence,
                            &mut state.data,
                        )
                    }
                });
                state.sequence = state.sequence.wrapping_add(1);
                state.epoch = epoch;
                state.revocation = revocation;
                match result {
                    Ok(data) => {
                        let now = Instant::now();
                        state.valid_until = (data.evidence != EvidenceKind::Static)
                            .then(|| now + millis(connection.policy.max_age_ms));
                        state.data = Some(data);
                        state.observed_at = Some(now);
                        state.error = None;
                    }
                    Err(error) => {
                        record_failure(&connection, &mut state, &error);
                        if matches!(
                            error.code,
                            ErrorCode::Unauthenticated
                                | ErrorCode::PermissionDenied
                                | ErrorCode::PermissionFiltered
                        ) {
                            drop(state);
                            crate::revoke_connection(&core, &gate, &connection);
                        }
                    }
                }
                slot.changes
                    .send_modify(|value| *value = value.wrapping_add(1));
            }
        }
        let delay = if failed {
            let delay = crate::retry_delay(
                &core,
                &connection,
                backoff,
                connection.queries.load(Ordering::Relaxed),
            );
            backoff = backoff
                .saturating_mul(2)
                .min(connection.policy.max_backoff_ms);
            delay
        } else {
            backoff = connection.policy.initial_backoff_ms;
            millis(connection.policy.poll_interval_ms)
        };
        let wait_for_change = async {
            if failed {
                return std::future::pending().await;
            }
            if let Some(timeout) = connection.provider.change_wait_timeout() {
                // 长连接使用独立有限额度；排队期间周期性全量查询仍可推进，不占用短请求和注销额度。
                let streams = connection.provider.change_wait_streams().max(1);
                let _permit = core
                    .watches
                    .acquire_many(streams)
                    .await
                    .map_err(|_| closed())?;
                tokio::time::timeout(timeout, connection.provider.wait_for_change(&slot.service))
                    .await
                    .unwrap_or_else(|_| Err(crate::timeout()))
            } else {
                std::future::pending().await
            }
        };
        tokio::select! {
            _ = core.stop.cancelled() => break,
            _ = tokio::time::sleep(delay) => {},
            result = wait_for_change => {
                if let Err(error) = result {
                    {
                        let gate = lock(&core.gate);
                        let mut state = lock(&slot.state);
                        record_failure(&connection, &mut state, &error);
                        drop(state);
                        if matches!(error.code, ErrorCode::Unauthenticated | ErrorCode::PermissionDenied | ErrorCode::PermissionFiltered) {
                            crate::revoke_connection(&core, &gate, &connection);
                        }
                        slot.changes.send_modify(|value| *value = value.wrapping_add(1));
                    }
                    // 立即断开的 watch 必须退避，防止权限拒绝或持续 EOF 形成热循环。
                    tokio::select! {
                        _ = core.stop.cancelled() => break,
                        _ = tokio::time::sleep(millis(connection.policy.initial_backoff_ms)) => {},
                    }
                } else {
                    // watch 持续立即返回仍须限制同步频率，避免空事件或索引回退占满控制队列。
                    tokio::select! {
                        _ = core.stop.cancelled() => break,
                        _ = tokio::time::sleep(millis(connection.policy.poll_interval_ms.min(100))) => {},
                    }
                }
            }
        }
        let _gate = lock(&core.gate);
        let mut slots = lock(&connection.slots);
        if slot.changes.receiver_count() == 0
            && lock(&slot.state).touched.elapsed() >= millis(connection.policy.idle_timeout_ms)
        {
            slots.remove(&slot.service);
            break;
        }
    }
}

/// 业务作用：在发布前核对实例内容并计入包括旧代在内的全局配额。
/// 参数说明：`core` 提供限额，`connection` 定义后端实例身份，`instances` 为完整候选，`evidence` 是证据等级，`previous` 是当前完整旧代。
/// 返回：验证和配额均通过才替换旧代；失败保留其数据与计费，不发布部分集合。无消费者持有的旧代可原位替换以复用额度。
fn allocate(
    core: &Core,
    connection: &Connection,
    instances: Vec<Instance>,
    evidence: EvidenceKind,
    previous: &mut Option<Arc<Data>>,
) -> Result<Arc<Data>> {
    let mut keys = BTreeSet::new();
    let mut identities = Vec::with_capacity(instances.len());
    let mut bytes = instances
        .capacity()
        .saturating_mul(std::mem::size_of::<Instance>());
    for instance in &instances {
        let identity = connection.provider.instance_key(instance);
        if instance.ip.is_empty()
            || instance.port == 0
            || !instance.weight.is_finite()
            || identity.is_empty()
            || identity.len() > 2048
            || !keys.insert(identity.clone())
        {
            return Err(DiscoveryError::new(
                ErrorCode::InvalidSnapshot,
                "invalid or duplicate instance in complete observation",
            ));
        }
        bytes = bytes.saturating_add(identity.capacity());
        identities.push(identity);
        bytes = bytes
            .saturating_add(instance.ip.capacity())
            .saturating_add(instance.cluster_name.as_ref().map_or(0, String::capacity))
            .saturating_add(
                instance
                    .metadata
                    .capacity()
                    .saturating_mul(std::mem::size_of::<(String, String)>() + 16),
            );
        for (key, value) in &instance.metadata {
            bytes = bytes
                .saturating_add(key.capacity())
                .saturating_add(value.capacity());
        }
    }
    bytes = bytes.saturating_add(
        identities
            .capacity()
            .saturating_mul(std::mem::size_of::<String>()),
    );
    let mut quota = lock(&core.quota);
    // 只有独占旧代才可复用额度；外部 Snapshot 仍持有的旧代必须与新代同时计费。
    let reusable = previous.as_mut().and_then(Arc::get_mut);
    let old_instances = reusable.as_ref().map_or(0, |data| data.instances.len());
    let old_bytes = reusable.as_ref().map_or(0, |data| data.bytes);
    let next_instances = (quota.instances - old_instances).saturating_add(instances.len());
    let next_bytes = (quota.bytes - old_bytes).saturating_add(bytes);
    if instances.len() > core.limits.max_instances_per_service
        || next_instances > core.limits.max_total_instances
        || next_bytes > core.limits.max_snapshot_bytes
    {
        return Err(DiscoveryError::new(
            ErrorCode::CapacityExceeded,
            "snapshot capacity reached",
        ));
    }
    // 容量裁决成功后才一起更新计费与数据，失败不能提前丢弃尚在宽限内的完整证据。
    quota.instances = next_instances;
    quota.bytes = next_bytes;
    if let Some(data) = reusable {
        data.instances = instances;
        data.identities = identities;
        data.evidence = evidence;
        data.bytes = bytes;
        return Ok(previous.as_ref().unwrap().clone());
    }
    Ok(Arc::new(Data {
        instances,
        identities,
        evidence,
        bytes,
        quota: core.quota.clone(),
    }))
}

#[async_trait::async_trait]
impl DiscoveryClient for DiscoveryHandle {
    /// 业务作用：要求兼容调用方逐次检查发送许可。
    /// 参数说明：无。
    /// 返回：受管连接始终需要提交门禁。
    fn requires_admission(&self) -> bool {
        true
    }

    /// 业务作用：使旧发现接口的发送路径服从统一观察门禁。
    /// 参数说明：`service`、`instance` 是本次候选，`submit` 是同步提交动作。
    /// 返回：当前目标已存在且仍有效时才提交。
    fn submit_if_current(
        &self,
        service: &str,
        instance: &Instance,
        submit: &mut dyn FnMut(),
    ) -> anyhow::Result<()> {
        let slot = lock(&self.connection.slots)
            .get(service)
            .cloned()
            .ok_or_else(closed)?;
        let observation = Observation {
            core: self.core.clone(),
            connection: self.connection.clone(),
            changes: slot.changes.subscribe(),
            slot,
        };
        Ok(observation.submit_if_current(instance, submit)?)
    }

    /// 业务作用：列出当前连接明确允许枚举的服务。
    /// 参数说明：无。
    /// 返回：不支持枚举时失败，不扩大后端权限范围。
    async fn list_services(&self) -> anyhow::Result<Vec<String>> {
        let core = self.core.upgrade().ok_or_else(closed)?;
        Ok(operation(
            &core,
            Instant::now() + millis(self.connection.policy.request_timeout_ms),
            self.connection.provider.services(),
        )
        .await?)
    }

    /// 业务作用：为兼容调用方返回仍有效的可接流实例。
    /// 参数说明：`service` 是明确服务目标。
    /// 返回：过滤后的业务自有数组；后续提交仍须复验。
    async fn discover(&self, service: &str) -> anyhow::Result<Vec<Instance>> {
        Ok(self
            .discover_all(service)
            .await?
            .into_iter()
            .filter(is_traffic_instance)
            .collect())
    }

    /// 业务作用：通过共享观察提供完整实例，不另起每请求轮询。
    /// 参数说明：`service` 是明确服务目标。
    /// 返回：有效完整快照的副本，副本不计入受管快照配额。
    async fn discover_all(&self, service: &str) -> anyhow::Result<Vec<Instance>> {
        let observation = self
            .observe(
                service,
                Instant::now() + millis(self.connection.policy.request_timeout_ms),
            )
            .await?;
        Ok(observation.snapshot(false)?.instances().to_vec())
    }

    /// 业务作用：桥接旧 Vec watch，在失效时关闭 channel 而非伪造成功空集合。
    /// 参数说明：`service` 是目标，`options` 不得要求快于冻结观察周期的校正。
    /// 返回：共享观察的兼容订阅，guard 释放即停止桥接任务。
    async fn watch_with_options(
        &self,
        service: &str,
        options: WatchOptions,
    ) -> anyhow::Result<ServiceWatch> {
        options.validate()?;
        if options.poll_interval < millis(self.connection.policy.poll_interval_ms) {
            return Err(crate::config::invalid(
                "watch interval is faster than frozen observation policy",
            )
            .into());
        }
        let core = self.core.upgrade().ok_or_else(closed)?;
        let bridge = core.bridges.clone().try_acquire_owned().map_err(|_| {
            DiscoveryError::new(
                ErrorCode::CapacityExceeded,
                "compatibility watch capacity reached",
            )
        })?;
        let mut observation = self
            .observe(
                service,
                Instant::now() + millis(self.connection.policy.request_timeout_ms),
            )
            .await?;
        let initial = observation
            .snapshot(false)?
            .instances()
            .iter()
            .filter(|i| is_traffic_instance(i))
            .cloned()
            .collect();
        let (sender, receiver) = watch::channel(initial);
        let cancel = CancellationToken::new();
        let stopped = cancel.clone();
        spawn_owned(&core, async move {
            let _bridge = bridge;
            loop {
                tokio::select! {
                    _ = stopped.cancelled() => break,
                    result = observation.changed(Instant::now() + millis(86_400_000)) => if result.is_err() { break; },
                }
                let Ok(snapshot) = observation.snapshot(false) else {
                    break;
                };
                if sender
                    .send(
                        snapshot
                            .instances()
                            .iter()
                            .filter(|i| is_traffic_instance(i))
                            .cloned()
                            .collect(),
                    )
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(ServiceWatch {
            guard: Box::new(BridgeGuard(cancel)),
            receiver,
        })
    }
}

struct BridgeGuard(CancellationToken);
impl Drop for BridgeGuard {
    /// 业务作用：释放兼容订阅时停止本消费者桥接。
    /// 参数说明：无。
    /// 返回：共享后端观察仍由其他需求维持。
    fn drop(&mut self) {
        self.0.cancel();
    }
}
#[async_trait::async_trait]
impl ServiceWatchGuard for BridgeGuard {
    /// 业务作用：显式解除本消费者订阅。
    /// 参数说明：无。
    /// 返回：桥接任务收到取消，不关闭共享连接。
    async fn unsubscribe(self: Box<Self>) -> anyhow::Result<()> {
        self.0.cancel();
        Ok(())
    }
}
