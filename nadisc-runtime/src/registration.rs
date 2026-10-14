//! 注册写入串行化与本地资格。远端结果不明时停止相反写入后的重新发布。

use crate::{
    changed, closed, config::RegistrationPolicy, lock, millis, operation, timeout, Connection,
    Core, Gate, OwnerPhase,
};
use nadisc::managed::{
    DiscoveryError, ErrorCode, RegistrationEvidence, RegistrationPlan, Result, WriteAdmission,
};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{sync::watch, time::Instant};

/// 宿主提供的本地业务资格，不应由包含远端注册结果的整体 readiness 反向驱动。
#[derive(Clone)]
pub struct Eligibility {
    inner: Arc<Mutex<bool>>,
    changes: watch::Sender<bool>,
    check: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    valid_until: Arc<Mutex<Option<Instant>>>,
    source: Option<std::sync::Weak<()>>,
}

/// 手动宿主的唯一资格发送者；丢弃后读句柄立即失效。
pub struct EligibilityController {
    signal: Eligibility,
    _lifetime: Arc<()>,
}
impl EligibilityController {
    /// 业务作用：发布有期限的本地资格，不让接收者保留永久有效的最后成功值。
    /// 参数说明：`ready` 为本地事实，`valid_for` 必须在一毫秒到三十秒之间。
    /// 返回：有效输入同步改变资格；非法期限拒绝且不开放接流。
    pub fn update(&self, ready: bool, valid_for: Duration) -> Result<()> {
        if valid_for < Duration::from_millis(1) || valid_for > Duration::from_secs(30) {
            self.signal.set(false);
            return Err(DiscoveryError::new(
                ErrorCode::InvalidConfig,
                "eligibility validity is out of bounds",
            ));
        }
        let mut value = lock(&self.signal.inner);
        *value = ready;
        *lock(&self.signal.valid_until) = ready.then(|| Instant::now() + valid_for);
        self.signal.changes.send_replace(ready);
        Ok(())
    }
}
impl Drop for EligibilityController {
    /// 业务作用：发送者退出时立即撤销发布许可并唤醒注册监督。
    /// 参数说明：无。
    /// 返回：保留的读句柄不能延续最后一次 true。
    fn drop(&mut self) {
        self.signal.set(false);
    }
}

impl Eligibility {
    /// 业务作用：创建默认不接流的资格源，监听器准备后再显式开放。
    /// 参数说明：无。
    /// 返回：可由宿主保留的资格控制句柄。
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(false)),
            changes: watch::channel(false).0,
            check: None,
            valid_until: Arc::new(Mutex::new(None)),
            source: None,
        }
    }

    /// 业务作用：为手动宿主分离资格更新责任与传入 owner 的只读信号。
    /// 参数说明：无。
    /// 返回：唯一发送者和默认不接流的信号，发送者退出或证据过期均撤权。
    pub fn channel() -> (EligibilityController, Self) {
        let lifetime = Arc::new(());
        let mut signal = Self::new();
        signal.source = Some(Arc::downgrade(&lifetime));
        (
            EligibilityController {
                signal: signal.clone(),
                _lifetime: lifetime,
            },
            signal,
        )
    }

    /// 业务作用：同步改变本地发布资格，与注册提交前复验使用同一把锁。
    /// 参数说明：`ready` 表示该注册对应的本地 listener 和业务依赖是否可接流。
    /// 返回：后续发布立即受约束；手动 true 最多有效三十秒，需要宿主持续更新。
    pub fn set(&self, ready: bool) {
        let mut value = lock(&self.inner);
        *value = ready;
        *lock(&self.valid_until) = ready.then(|| Instant::now() + Duration::from_secs(30));
        self.changes.send_replace(ready);
    }

    /// 业务作用：读取本地资格供独立健康端点返回真实状态。
    /// 参数说明：无。
    /// 返回：本地事实，不包含注册中心回查结果。
    pub fn is_ready(&self) -> bool {
        self.current(*lock(&self.inner))
    }

    /// 业务作用：绑定宿主可同步读取的本地资格，在实际发布点重新读取而不依赖轮询延迟。
    /// 参数说明：`check` 必须无网络、有限时间返回，且不得重入 discovery 或改变资格对象。
    /// 返回：只依赖宿主当前事实的资格源；闭包必须自行包含停机和证据期限门禁。
    pub fn from_check(check: impl Fn() -> bool + Send + Sync + 'static) -> Self {
        Self {
            inner: Arc::new(Mutex::new(true)),
            changes: watch::channel(true).0,
            check: Some(Arc::new(check)),
            valid_until: Arc::new(Mutex::new(None)),
            source: None,
        }
    }

    /// 业务作用：在已持有本地许可锁时复验证据期限或宿主实时状态。
    /// 参数说明：`ready` 是保护范围内的显式许可。
    /// 返回：停止或证据到期均为 false。
    fn current(&self, ready: bool) -> bool {
        ready
            && self
                .source
                .as_ref()
                .is_none_or(|source| source.strong_count() > 0)
            && self.check.as_ref().map_or_else(
                || lock(&self.valid_until).is_some_and(|until| Instant::now() < until),
                |check| check(),
            )
    }
}

impl Default for Eligibility {
    /// 业务作用：默认关闭发布许可，避免构造组件即对外注册。
    /// 参数说明：无。
    /// 返回：未就绪的本地资格源。
    fn default() -> Self {
        Self::new()
    }
}

/// 冻结前的注册计划、目标连接与本地发布资格。
pub struct RegistrationSpec {
    /// 目标连接的 qualifier，必须引用 owner 中已配置的连接。
    pub connection_ref: String,
    /// 冻结后提交给后端的完整注册计划。
    pub plan: RegistrationPlan,
    /// 目标连接或注册的冻结策略。
    pub policy: RegistrationPolicy,
    /// 发布前复验的本地业务资格，不依赖远端注册健康回查。
    pub eligibility: Eligibility,
}

/// 单个注册目标的发布、健康确认与注销状态。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistrationState {
    /// 计划已冻结，尚未写入后端。
    Planned,
    /// 正在提交注册写入。
    Registering,
    /// 远端写入结果不明，需要协调后才能继续发布。
    OutcomeUnknown,
    /// 已提交注册，尚未获得有效健康证据。
    PendingHealth,
    /// 已获得健康证据，仍需按 valid_until 判断时效。
    Active,
    /// 本地资格或后端证据不再允许继续接流。
    Suspended,
    /// 已停止发布，正在注销远端实例。
    Withdrawing,
    /// 注销未能确认完成，可能存在远端残留。
    Residual,
    /// 已确认目标注销完成。
    Removed,
}

/// 注册目标的只读证据摘要；Active 仍需检查证据有效期。
#[derive(Clone, Debug)]
pub struct RegistrationStatus {
    /// 注册目标的本地唯一标识。
    pub id: String,
    /// 承载该注册的连接 qualifier。
    pub connection: String,
    /// 当前注册阶段；远端写入结果不明时不能当作成功。
    pub state: RegistrationState,
    /// 是否曾完成启动所要求的首次注册确认。
    pub startup_confirmed: bool,
    /// 远端注册失去有效健康证据时是否影响宿主整体就绪。
    pub readiness_critical: bool,
    /// 最近一次有效健康证据的本地单调时刻。
    pub observed_at: Option<Instant>,
    /// 当前健康证据的截止时刻；缺失或过期均不代表就绪。
    pub valid_until: Option<Instant>,
    /// 最近的注册或回查错误，不包含凭据原文。
    pub error: Option<DiscoveryError>,
}

impl RegistrationStatus {
    /// 业务作用：读取无需网络访问的当前发布证据。
    /// 参数说明：无。
    /// 返回：已确认为 Active 且证据尚未到期才算远端就绪。
    pub fn is_ready(&self) -> bool {
        self.state == RegistrationState::Active
            && self
                .valid_until
                .is_some_and(|deadline| Instant::now() < deadline)
    }
}

pub(crate) struct Record {
    pub spec: RegistrationSpec,
    pub connection: Arc<Connection>,
    pub status: Mutex<RegistrationStatus>,
    pub started: AtomicBool,
    pub confirmed: AtomicBool,
    health_revocation: AtomicU64,
    attempted: AtomicBool,
    uncertain: AtomicBool,
    unsettled_renewal: AtomicBool,
}

impl Record {
    /// 业务作用：在第一项外部写入前持有完整计划和状态记录。
    /// 参数说明：`spec` 已完成预检，`connection` 提供真实后端身份。
    /// 返回：尚未发布的注册记录。
    pub(crate) fn new(spec: RegistrationSpec, connection: Arc<Connection>) -> Self {
        let health_revocation = connection.revocation.load(Ordering::Acquire);
        Self {
            status: Mutex::new(RegistrationStatus {
                id: spec.plan.id.clone(),
                connection: spec.connection_ref.clone(),
                state: RegistrationState::Planned,
                startup_confirmed: false,
                readiness_critical: spec.policy.readiness_critical,
                observed_at: None,
                valid_until: None,
                error: None,
            }),
            spec,
            connection,
            started: AtomicBool::new(false),
            confirmed: AtomicBool::new(false),
            health_revocation: AtomicU64::new(health_revocation),
            attempted: AtomicBool::new(false),
            uncertain: AtomicBool::new(false),
            unsettled_renewal: AtomicBool::new(false),
        }
    }

    /// 业务作用：统一裁决启动确认、宿主就绪与诊断读取的注册健康资格。
    /// 参数说明：`gate` 是 owner 已持有的状态门禁，与连接撤权及健康证据发布串行。
    /// 返回：旧身份代次、本地失权或停机的证据不能继续用于就绪；保留注销残留等独立生命周期事实。
    pub(crate) fn current_status(&self, gate: &Gate) -> RegistrationStatus {
        let mut status = lock(&self.status).clone();
        // 身份代次只由有效的新回查推进；复制状态不能使被撤销的旧证据重新有效。
        if self.health_revocation.load(Ordering::Acquire)
            != self.connection.revocation.load(Ordering::Acquire)
        {
            status.valid_until = None;
            if matches!(
                status.state,
                RegistrationState::Active | RegistrationState::PendingHealth
            ) {
                status.state = RegistrationState::Suspended;
                status.error = Some(DiscoveryError::new(
                    ErrorCode::Unauthenticated,
                    "registration evidence belongs to a revoked identity",
                ));
            }
        }
        if !self.spec.eligibility.is_ready() || gate.failed || gate.close_started {
            status.valid_until = None;
        }
        status
    }
}

struct Admission<'a> {
    core: &'a Core,
    record: &'a Record,
}
impl WriteAdmission for Admission<'_> {
    /// 业务作用：在 adapter 的实际传输提交点检查发布权并保存未知结果事实。
    /// 参数说明：`submit` 是已经完成鉴权准备的同步提交动作。
    /// 返回：许可有效时启动写入，失权时拒绝且不执行动作。
    fn submit(&self, submit: &mut dyn FnMut()) -> Result<()> {
        let (core, record) = (self.core, self.record);
        let gate = lock(&core.gate);
        let eligible = lock(&record.spec.eligibility.inner);
        if gate.failed
            || gate.close_started
            || !matches!(gate.phase, OwnerPhase::Starting | OwnerPhase::Serving)
            || !record.spec.eligibility.current(*eligible)
            || core.registrations_stop.is_cancelled()
        {
            return Err(closed());
        }
        // 写前保留结果不明事实；等待 future 被取消时清理仍知道可能已经写入。
        record.attempted.store(true, Ordering::Release);
        record.uncertain.store(true, Ordering::Release);
        submit();
        Ok(())
    }
}

/// 业务作用：每个注册目标只运行一个写入与健康监督序列。
/// 参数说明：`core` 管理停止信号，`record` 保存不可变外部身份。
/// 返回：关闭后退出；已确认身份的幂等续约可协调重试，管理端禁用、缺失及未解决的相反写入不授予重新注册权限。
pub(crate) async fn run(core: Arc<Core>, record: Arc<Record>) {
    let mut eligibility = record.spec.eligibility.changes.subscribe();
    let mut prepared = record.connection.prepared.subscribe();
    let admission = Admission {
        core: &core,
        record: &record,
    };
    let monitor = millis(record.spec.policy.monitor_interval_ms);
    let interval = record
        .connection
        .provider
        .renewal_interval(&record.spec.plan)
        .unwrap_or(monitor)
        .min(monitor);
    let idempotent_renewal = record
        .connection
        .provider
        .supports_idempotent_renewal(&record.spec.plan);
    let mut registered = false;
    let mut renewal_allowed = false;
    let mut registration_blocked = false;
    let mut backoff = record.connection.policy.initial_backoff_ms;
    let mut attempt = 0u64;
    loop {
        let mut renewal_failed = false;
        if core.registrations_stop.is_cancelled() {
            return;
        }
        if !record.spec.eligibility.is_ready() || !*prepared.borrow() {
            if registered {
                // 本地失权先停止健康发布；只有本 owner 明确完成的删除才允许资格恢复后重建。
                set_state(&core, &record, RegistrationState::Suspended, None);
                withdraw(
                    record.clone(),
                    Instant::now() + millis(record.spec.policy.deregister_timeout_ms),
                    millis(record.spec.policy.deregister_timeout_ms),
                )
                .await;
                registered = false;
                registration_blocked |= lock(&record.status).state != RegistrationState::Removed;
                changed(&core);
            }
            tokio::select! {
                _ = core.registrations_stop.cancelled() => return,
                _ = eligibility.changed() => {},
                _ = prepared.changed() => {},
                _ = tokio::time::sleep(interval) => {},
            }
            continue;
        }
        if !registered && !registration_blocked {
            set_state(&core, &record, RegistrationState::Registering, None);
            let result = operation(
                &core,
                Instant::now() + millis(record.connection.policy.request_timeout_ms),
                record
                    .connection
                    .provider
                    .register(&record.spec.plan, &admission),
            )
            .await;
            match result {
                Ok(()) => {
                    record.uncertain.store(false, Ordering::Release);
                    registered = true;
                    renewal_allowed = true;
                    set_state(&core, &record, RegistrationState::PendingHealth, None);
                }
                Err(error) => {
                    {
                        let gate = lock(&core.gate);
                        revoke_identity(&core, &gate, &record.connection, &error);
                    }
                    if definite_rejection(&error) {
                        record.uncertain.store(false, Ordering::Release);
                    }
                    registration_blocked = record.uncertain.load(Ordering::Acquire)
                        || matches!(
                            error.code,
                            ErrorCode::InvalidConfig | ErrorCode::Unsupported
                        );
                    // 首次占位冲突可能来自旧 owner 的延迟删除；只允许重新核对，不授予覆盖既有记录的权限。
                    set_state(
                        &core,
                        &record,
                        if record.uncertain.load(Ordering::Acquire) {
                            RegistrationState::OutcomeUnknown
                        } else {
                            RegistrationState::Suspended
                        },
                        Some(error),
                    );
                }
            }
        }
        if registered
            && renewal_allowed
            // 只有已确认身份且 provider 承诺相同 payload 幂等时，才可协调未知续约；不重放首次注册。
            && (!record.uncertain.load(Ordering::Acquire) || idempotent_renewal)
            && record
                .connection
                .provider
                .renewal_interval(&record.spec.plan)
                .is_some()
        {
            let result = operation(
                &core,
                Instant::now() + millis(record.connection.policy.request_timeout_ms),
                record
                    .connection
                    .provider
                    .renew(&record.spec.plan, true, &admission),
            )
            .await;
            match result {
                // 本次续约确认后才恢复健康回查；更早未决续约的清理限制由独立事实保留。
                Ok(()) => record.uncertain.store(false, Ordering::Release),
                Err(error) => {
                    renewal_failed = true;
                    {
                        let gate = lock(&core.gate);
                        revoke_identity(&core, &gate, &record.connection, &error);
                    }
                    if definite_rejection(&error) {
                        record.uncertain.store(false, Ordering::Release);
                    }
                    if record.uncertain.load(Ordering::Acquire) {
                        // 新续约成功只能证明当前同向状态；更早请求仍可能迟到，注销及后续重建必须保留此事实。
                        record.unsettled_renewal.store(true, Ordering::Release);
                        renewal_allowed = idempotent_renewal && !error.revokes_evidence();
                        set_state(
                            &core,
                            &record,
                            RegistrationState::OutcomeUnknown,
                            Some(error),
                        );
                    } else {
                        renewal_allowed = false;
                        let mut status = lock(&record.status);
                        if error.revokes_evidence() {
                            status.valid_until = None;
                            status.state = RegistrationState::Suspended;
                        }
                        status.error = Some(error);
                    }
                }
            }
        }
        if registered && !record.uncertain.load(Ordering::Acquire) {
            let revocation = record.connection.revocation.load(Ordering::Acquire);
            let result = operation(
                &core,
                Instant::now() + millis(record.connection.policy.request_timeout_ms),
                record.connection.provider.inspect(&record.spec.plan),
            )
            .await;
            let gate = lock(&core.gate);
            if core.registrations_stop.is_cancelled() || gate.close_started {
                return;
            }
            if let Err(error) = &result {
                if error.revokes_evidence() {
                    renewal_allowed = false;
                }
                revoke_identity(&core, &gate, &record.connection, error);
            }
            // 回查期间发生撤权时，旧请求的 Healthy 或其他结果都不能发布；等待原身份下的新一轮证据。
            if revocation == record.connection.revocation.load(Ordering::Acquire) {
                let mut status = lock(&record.status);
                match result {
                    Ok(RegistrationEvidence::Healthy) if record.spec.eligibility.is_ready() => {
                        registration_blocked = false;
                        renewal_allowed = true;
                        let now = Instant::now();
                        status.state = RegistrationState::Active;
                        status.observed_at = Some(now);
                        status.valid_until =
                            Some(now + millis(record.spec.policy.observation_grace_ms));
                        status.error = None;
                        status.startup_confirmed = true;
                        record.confirmed.store(true, Ordering::Release);
                        record
                            .health_revocation
                            .store(revocation, Ordering::Release);
                    }
                    Ok(RegistrationEvidence::Disabled | RegistrationEvidence::Missing) => {
                        // 管理端移除和禁用只允许继续观察，不能据此取得重新注册的权限。
                        renewal_allowed = false;
                        registration_blocked = true;
                        status.state = RegistrationState::Suspended;
                        status.valid_until = None;
                        status.error = Some(DiscoveryError::new(
                            ErrorCode::NotReady,
                            "registration was disabled or removed",
                        ));
                    }
                    Ok(_) => {
                        renewal_allowed = true;
                        status.state = RegistrationState::PendingHealth;
                        status.valid_until = None;
                        status.error = None;
                    }
                    Err(error) => {
                        // 不完整回查不产生新的健康事实，保留最后一次 Healthy 的固定期限；明确撤权仍立即关闭注册资格。
                        if error.revokes_evidence() {
                            status.state = RegistrationState::Suspended;
                            status.valid_until = None;
                            renewal_allowed = false;
                        }
                        status.error = Some(error);
                    }
                }
            }
        }
        changed(&core);
        let delay = if registered && !renewal_failed {
            backoff = record.connection.policy.initial_backoff_ms;
            attempt = 0;
            interval
        } else {
            let delay = crate::retry_delay(&core, &record.connection, backoff, attempt);
            attempt = attempt.wrapping_add(1);
            backoff = backoff
                .saturating_mul(2)
                .min(record.connection.policy.max_backoff_ms);
            // 续约响应持续失败也要退避，且不能比正常健康上报周期更频繁地重发。
            delay.max(interval)
        };
        let next_attempt = Instant::now() + delay;
        loop {
            tokio::select! {
                _ = core.registrations_stop.cancelled() => return,
                // 持续刷新 true 不能绕过退避；实际失权立即唤醒，以便停止续约并执行摘流。
                _ = eligibility.changed() => if !record.spec.eligibility.is_ready() { break; },
                _ = tokio::time::sleep_until(next_attempt) => break,
            }
        }
    }
}

/// 业务作用：注册或健康回查的认证撤销必须同时关闭该身份下的全部发现路由。
/// 参数说明：`core` 提供通知，`gate` 是已持有的状态门禁，`connection` 是明确连接，`error` 是后端稳定错误类别。
/// 返回：只有身份或权限失败会撤销连接证据，单注册冲突不影响其他服务。
fn revoke_identity(core: &Core, gate: &Gate, connection: &Connection, error: &DiscoveryError) {
    if matches!(
        error.code,
        ErrorCode::Unauthenticated | ErrorCode::PermissionDenied | ErrorCode::PermissionFiltered
    ) {
        crate::revoke_connection(core, gate, connection);
    }
}

/// 业务作用：区分明确拒绝与请求可能已在远端执行的故障。
/// 参数说明：`error` 由 adapter 将服务端拒绝映射为稳定类别。
/// 返回：仅明确未执行的拒绝允许清除本次写入的不确定状态。
fn definite_rejection(error: &DiscoveryError) -> bool {
    matches!(
        error.code,
        ErrorCode::InvalidConfig
            | ErrorCode::Unauthenticated
            | ErrorCode::PermissionDenied
            | ErrorCode::Conflict
            | ErrorCode::Unsupported
    )
}

/// 业务作用：发布注册状态变化，不在状态锁内执行后端调用。
/// 参数说明：`core` 提供通知，`record` 是目标，`state`、`error` 是当前裁决。
/// 返回：非 Active 状态同步清除就绪有效期。
fn set_state(
    core: &Core,
    record: &Record,
    state: RegistrationState,
    error: Option<DiscoveryError>,
) {
    let mut status = lock(&record.status);
    status.state = state;
    status.error = error;
    if state != RegistrationState::Active {
        status.valid_until = None;
    }
    drop(status);
    changed(core);
}

/// 业务作用：尝试删除本 owner 曾提交的注册，结果不明时保留残留事实。
/// 参数说明：`record` 是精确计划，`deadline` 是全局期限，`share` 防止单目标耗尽全部预算。
/// 返回：没有写过或明确删除且无旧未知写时标记 Removed，其余标记 Residual。
pub(crate) async fn withdraw(record: Arc<Record>, deadline: Instant, share: Duration) {
    if !record.attempted.load(Ordering::Acquire) {
        lock(&record.status).state = RegistrationState::Removed;
        return;
    }
    if lock(&record.status).state == RegistrationState::Removed {
        return;
    }
    // 后续同向续约或普通回查不能证明旧请求已终止，清理 ACK 不得清除其迟到风险或开放重新注册。
    let uncertain = record.uncertain.load(Ordering::Acquire)
        || record.unsettled_renewal.load(Ordering::Acquire);
    lock(&record.status).state = RegistrationState::Withdrawing;
    let until =
        deadline.min(Instant::now() + share.min(millis(record.spec.policy.deregister_timeout_ms)));
    let result = tokio::time::timeout_at(
        until,
        record.connection.provider.deregister(&record.spec.plan),
    )
    .await
    .unwrap_or_else(|_| Err(timeout()));
    let mut status = lock(&record.status);
    status.valid_until = None;
    match result {
        Ok(()) if !uncertain => {
            status.state = RegistrationState::Removed;
            status.error = None;
            record.attempted.store(false, Ordering::Release);
        }
        Ok(()) => {
            status.state = RegistrationState::Residual;
            status.error = Some(DiscoveryError::new(
                ErrorCode::OutcomeUnknown,
                "an earlier write may still complete after cleanup",
            ));
        }
        Err(error) => {
            record.uncertain.store(true, Ordering::Release);
            status.state = RegistrationState::Residual;
            status.error = Some(error);
        }
    }
}

/// 业务作用：摘流终止后保留不能证明已删除的外部记录。
/// 参数说明：`record` 的写入和摘流任务均已结束。
/// 返回：未写入的目标为 Removed，其余未完成目标为 Residual。
pub(crate) fn finish_withdrawal(record: &Record) {
    let mut status = lock(&record.status);
    status.valid_until = None;
    if !record.attempted.load(Ordering::Acquire) {
        status.state = RegistrationState::Removed;
    } else if !matches!(
        status.state,
        RegistrationState::Removed | RegistrationState::Residual
    ) {
        status.state = RegistrationState::Residual;
        status.error = Some(DiscoveryError::new(
            ErrorCode::OutcomeUnknown,
            "registration cleanup did not complete",
        ));
    }
}
