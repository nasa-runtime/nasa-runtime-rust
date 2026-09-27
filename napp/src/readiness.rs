//! 通用动态 Readiness 核心。
//!
//! 该模块治理**运行期**动态就绪:每个运行依赖(datasource、Redis client、Kafka group、
//! naweb 安全运行时等)注册一个贡献项并周期性 `observe` 原始观测;`/readyz` 只读内存快照
//! 得出对流量的最终裁决,不做任何网络 I/O。启动期一次性协议探针的错误质量另。
//!
//! 设计要点:
//! - 观测(`observe`)与裁决(`snapshot`)分离:`observe` 在很短的 per-entry 临界区内推进
//!   失败/恢复阈值,`snapshot` 无锁读各 entry 的已发布状态并聚合。两者都不做 I/O。
//! - `Unknown` 关键依赖(`affects_ready`)在 Ready 前视为 NotReady;非关键依赖视为 Degraded。
//! - `reason` 只使用编译期 `&'static str`(见 [`reason`]),结构上防止把 URL/SQL/key/凭据
//!   等动态内容写进就绪诊断。
//! - 时间由调用方以 [`std::time::Instant`] 传入(stale 窗口均为相对 `Duration`),便于验证
//!   确定性推进;后续可改注入 `MonotonicClock`。
//!
//! 兼容:旧布尔契约(`register_readiness(component, name)`、`ReadinessContributor::set_ready`、
//! `ReadinessRegistry::all_ready`)保留,kafka 侧无需改动;各组件逐步迁移到
//! `observe`/policy 富合同。

use std::{
    collections::BTreeMap,
    sync::{Mutex, RwLock},
    time::Duration,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

/// 稳定就绪原因码。只允许编译期常量,禁止把动态错误文本(地址、SQL、key、凭据)当 reason。
pub mod reason {
    /// 依赖最近一次观测就绪且未过 stale 窗。
    #[cfg_attr(
        not(any(
            feature = "redis",
            feature = "kafka",
            feature = "outbox",
            feature = "db",
            feature = "web",
            feature = "grpc",
            feature = "saga",
            feature = "nacos-discovery",
            feature = "partition",
            feature = "nacos-config",
            feature = "config-watch",
            feature = "object-store",
            feature = "cache",
            feature = "telemetry",
            feature = "ws-client",
            feature = "hystrix"
        )),
        allow(dead_code)
    )]
    pub const HEALTHY: &str = "healthy";
    /// 贡献项已注册但尚无任何观测(初始 `Unknown`)。
    pub const UNOBSERVED: &str = "unobserved";
    /// 探针超时。
    #[cfg(any(feature = "db", feature = "db-pgsql", feature = "nacos-discovery"))]
    pub const PROBE_TIMEOUT: &str = "probe_timeout";
    /// 观测长时间未更新,已超过 `stale_after`。
    pub const WATCH_STALE: &str = "watch_stale";
    /// naweb 路由/安全审计失败。
    #[cfg(feature = "web")]
    pub const ROUTE_AUDIT_FAILED: &str = "route_audit_failed";
    /// 依赖被判为不可服务。
    #[cfg(any(
        feature = "object-store",
        feature = "kafka",
        feature = "db",
        feature = "db-pgsql",
        feature = "redis",
        feature = "cache",
        feature = "partition",
        feature = "grpc",
        feature = "nacos-config",
        feature = "nacos-discovery",
        feature = "web"
    ))]
    pub const NOT_READY: &str = "not_ready";
    /// 保序执行器失去 worker 或 lane 的安全执行权。
    #[cfg(feature = "partition")]
    pub const PARTITION_UNHEALTHY: &str = "partition_unhealthy";
    /// gRPC listener 在运行期失去 serve 所有权。
    #[cfg(feature = "grpc")]
    pub const GRPC_LISTENER_UNAVAILABLE: &str = "grpc_listener_unavailable";
    /// gRPC listener 仍持有 socket，但连续 accept 失败已超过受管摘流阈值。
    #[cfg(feature = "grpc")]
    pub const GRPC_ACCEPT_STALLED: &str = "grpc_accept_stalled";
    /// gRPC TLS server identity 已进入配置的到期告警窗口。
    #[cfg(feature = "grpc")]
    pub const GRPC_TLS_CERTIFICATE_EXPIRING: &str = "grpc_tls_certificate_expiring";
    /// gRPC TLS server identity 已到期，listener 不得继续接收新流量。
    #[cfg(feature = "grpc")]
    pub const GRPC_TLS_CERTIFICATE_EXPIRED: &str = "grpc_tls_certificate_expired";
    /// 依赖可服务但发生可恢复降级。
    #[cfg(any(
        feature = "redis",
        feature = "cache",
        feature = "nacos-config",
        feature = "config-watch",
        feature = "object-store",
        feature = "web",
        feature = "ws",
        feature = "telemetry",
        feature = "ws-client",
        feature = "hystrix"
    ))]
    pub const DEGRADED: &str = "degraded";
}

/// 单个依赖对流量裁决的贡献状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DependencyState {
    /// 尚无观测。关键依赖按 NotReady 处理,非关键依赖按 Degraded 处理。
    Unknown,
    /// 最近成功且未过 stale 窗。
    Ready,
    /// 仍可服务但发生可恢复降级;`/readyz` 仍返回 200。
    Degraded,
    /// 不应接收新流量;关键依赖会使 `/readyz` 返回 503。
    NotReady,
}

/// 一个依赖如何影响全局就绪的策略。
///
/// `failure_threshold` / `recovery_threshold` 必须大于零;`stale_after` 为 `Some(d)` 时 `d`
/// 应大于 monitor 间隔(否则依赖会被误判 stale),`None` 表示不做 stale 检查(适用于一次性
/// 发布、不由 monitor 周期刷新的兼容贡献项)。
#[derive(Debug, Clone, Copy)]
pub struct ReadinessPolicy {
    /// 该依赖是否参与"是否摘流"的关键裁决。`true`=关键(失败则 503),`false`=非关键(失败仅 Degraded)。
    pub affects_ready: bool,
    /// 连续多少次"不就绪"观测后才发布 NotReady。
    pub failure_threshold: u32,
    /// 连续多少次"就绪"观测后才发布 Ready。
    pub recovery_threshold: u32,
    /// 超过该时长未观测则判 stale;`None` 表示不检查。
    pub stale_after: Option<Duration>,
}

impl ReadinessPolicy {
    /// 业务作用：关键、立即生效、不检查 stale 的策略。
    ///
    /// 复现旧布尔契约:贡献项初始 `Unknown`(关键 → 未就绪),一次就绪观测即 Ready、一次
    /// 不就绪观测即 NotReady,不因未周期刷新而 stale。兼容入口 `set_ready` 使用此策略。
    ///
    /// # 返回
    ///
    /// `affects_ready=true`、两个阈值均为 1、`stale_after=None` 的策略。
    pub const fn critical_immediate() -> Self {
        Self {
            affects_ready: true,
            failure_threshold: 1,
            recovery_threshold: 1,
            stale_after: None,
        }
    }
}

/// 注册失败的结构化原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterError {
    /// 名称去除首尾空白后为空。
    EmptyName,
    /// 同名贡献项已存在。
    Duplicate,
    /// 注册表已封口,运行期不再接受新贡献项。
    Sealed,
    /// 策略非法(阈值为零)。
    InvalidPolicy,
}

/// `/readyz` 与管理端读取的单个依赖只读快照。不含计数器或时间戳等内部状态。
#[derive(Debug, Clone)]
pub struct DependencySnapshot {
    /// 贡献项所属组件。
    pub component: Arc<str>,
    /// 启动期注册的稳定名称,不含配置秘密。
    pub name: Arc<str>,
    /// 计入 stale 后的有效贡献状态。
    pub state: DependencyState,
    /// contributor 发布的不含秘密的稳定原因码。
    pub reason: &'static str,
    /// 是否参与关键裁决。
    pub affects_ready: bool,
}

/// 全体依赖聚合出的就绪快照。
#[derive(Debug, Clone)]
pub struct ReadinessSnapshot {
    /// 是否可接收流量(`/readyz` 200 vs 503)。无关键依赖处于 NotReady/未就绪时为 true。
    pub ready: bool,
    /// 是否存在降级(仍可服务但有可恢复问题)。
    pub degraded: bool,
    /// 按名称有序的各依赖只读快照。
    pub entries: Arc<[DependencySnapshot]>,
}

/// 单个贡献项的可变状态,置于 per-entry 短临界区之后。
#[derive(Debug)]
struct EntryState {
    component: Arc<str>,
    policy: ReadinessPolicy,
    published: DependencyState,
    reason: &'static str,
    success_streak: u32,
    fail_streak: u32,
    last_observed: Option<Instant>,
    valid_until: Option<Instant>,
}

impl EntryState {
    /// 业务作用：创建尚无观测的贡献项状态，并冻结组件身份与阈值策略。
    ///
    /// 参数说明：`component` 是组件身份，`policy` 决定依赖失败如何影响整体就绪。
    ///
    /// 返回：尚未取得观测或租约证据的 Unknown 状态。
    fn new(component: Arc<str>, policy: ReadinessPolicy) -> Self {
        Self {
            component,
            policy,
            published: DependencyState::Unknown,
            reason: reason::UNOBSERVED,
            success_streak: 0,
            fail_streak: 0,
            last_observed: None,
            valid_until: None,
        }
    }

    /// 业务作用：按连续观测阈值推进依赖状态，并替换上轮观测证据。
    ///
    /// 参数说明：`raw` 是原始状态，`observed_reason` 是静态原因码，`now` 是本次观测的单调时刻。
    ///
    /// 返回：更新连续观测计数与发布状态；清除上轮期限，租约发布者在同一临界区设置本轮期限。
    fn advance(&mut self, raw: DependencyState, observed_reason: &'static str, now: Instant) {
        self.last_observed = Some(now);
        self.valid_until = None;
        match raw {
            DependencyState::Ready => {
                self.success_streak = self.success_streak.saturating_add(1);
                self.fail_streak = 0;
                if self.success_streak >= self.policy.recovery_threshold {
                    self.published = DependencyState::Ready;
                    self.reason = observed_reason;
                }
            }
            DependencyState::NotReady => {
                self.fail_streak = self.fail_streak.saturating_add(1);
                self.success_streak = 0;
                if self.fail_streak >= self.policy.failure_threshold {
                    self.published = DependencyState::NotReady;
                    self.reason = observed_reason;
                }
            }
            // Degraded 是显式的可恢复降级信号,不走阈值抖动过滤,立即发布并将两个连击清零。
            DependencyState::Degraded => {
                self.success_streak = 0;
                self.fail_streak = 0;
                self.published = DependencyState::Degraded;
                self.reason = observed_reason;
            }
            // 观测方不应主动上报 Unknown;若发生则视为一次"未就绪"计入失败连击但不改原因语义。
            DependencyState::Unknown => {
                self.fail_streak = self.fail_streak.saturating_add(1);
                self.success_streak = 0;
                if self.fail_streak >= self.policy.failure_threshold {
                    self.published = DependencyState::NotReady;
                    self.reason = observed_reason;
                }
            }
        }
    }

    /// 业务作用：在读取依赖状态时复验观测新鲜度与租约，防止阻塞的发布任务延长就绪权威。
    ///
    /// 参数说明：`now` 是本次读取的单调时刻。
    ///
    /// 返回：证据有效时返回已发布状态；证据过期时按策略返回 NotReady 或 Degraded 及失效原因。
    fn effective(&self, now: Instant) -> (DependencyState, &'static str) {
        let stale = matches!(
            (self.policy.stale_after, self.last_observed),
            (Some(after), Some(at)) if now.saturating_duration_since(at) >= after
        );
        // 租约型依赖按绝对单调期限失效；即使续租任务阻塞，读取就绪状态也不能延长权威。
        if stale || self.valid_until.is_some_and(|deadline| now >= deadline) {
            let state = if self.policy.affects_ready {
                DependencyState::NotReady
            } else {
                DependencyState::Degraded
            };
            (state, reason::WATCH_STALE)
        } else {
            (self.published, self.reason)
        }
    }
}

/// 一个运行依赖对 Application 就绪结论的贡献句柄。
///
/// 组件只持有自己的句柄并 `observe` 自身观测,不能枚举或修改其他组件的贡献项;动态健康
/// 因此不需要把具体组件类型反向写进 Application 核心。
#[derive(Clone, Debug)]
pub struct ReadinessContributor {
    entry: Arc<Mutex<EntryState>>,
}

impl ReadinessContributor {
    /// 业务作用：在统一接流点复验已登记证据的新鲜度。
    /// 参数说明：无。
    /// 返回：当前证据仍为 Ready 时为 true，过期证据不会延长接流权威。
    #[cfg(any(feature = "redis", feature = "ws-client", feature = "hystrix"))]
    pub(crate) fn ready_now(&self) -> bool {
        self.entry.lock().unwrap().effective(Instant::now()).0 == DependencyState::Ready
    }
    /// 业务作用：发布有租约期限的就绪证据，期限到达后由读取端独立摘流。
    ///
    /// 参数说明：`deadline` 是整批依赖最早失效的单调时刻。
    ///
    /// 返回：原子发布 Ready 与期限；已到期的证据立即按 NotReady 处理。
    #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
    pub(crate) fn observe_ready_until(&self, deadline: Instant) {
        let mut entry = self
            .entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        // 已过期的证据不能触发恢复阈值或短暂开放路由。
        if deadline <= now {
            entry.advance(DependencyState::NotReady, reason::NOT_READY, now);
        } else {
            entry.advance(DependencyState::Ready, reason::HEALTHY, now);
            entry.valid_until = Some(deadline);
        }
    }

    /// 业务作用：发布一次原始观测,由策略阈值决定是否改变已发布状态。
    ///
    /// # 参数
    ///
    /// - `state`:本次观测的原始状态(通常 `Ready`/`Degraded`/`NotReady`)。
    /// - `reason`:静态原因码（见内部 `reason` 目录），禁止动态错误文本。
    /// - `now`:当前单调时刻,用于 stale 判定;由调用方传入以便确定性推进。
    pub fn observe(&self, state: DependencyState, reason: &'static str, now: Instant) {
        self.entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .advance(state, reason, now);
    }

    /// 业务作用：发布一次降级/失败裁决，但不把它计作 stale freshness。
    ///
    /// 仅用于“last-good 仍可服务，但 freshness 必须从最后一次成功刷新起算”的 owner（当前为远程
    /// JWKS）。这样刷新失败可以立刻显示 Degraded，同时 `stale_after` 仍会在最后成功时间到期后升级
    /// NotReady；普通健康 monitor 应继续使用 [`observe`](Self::observe)，因为它们的失败探测本身也是
    /// 一次有效观测。
    #[cfg(feature = "web")]
    pub fn observe_without_refreshing_freshness(
        &self,
        state: DependencyState,
        reason: &'static str,
        now: Instant,
    ) {
        let mut entry = self
            .entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let last_observed = entry.last_observed;
        entry.advance(state, reason, now);
        entry.last_observed = last_observed;
    }

    /// 业务作用：旧布尔契约兼容入口:`true` → 一次 `Ready` 观测,`false` → 一次 `NotReady` 观测。
    ///
    /// 供既有组件(kafka)继续使用;新组件应直接调用 [`observe`](Self::observe)。
    ///
    /// # 参数
    ///
    /// - `ready`:组件按当前运行状态计算出的布尔结论。
    #[cfg(feature = "kafka")]
    pub fn set_ready(&self, ready: bool) {
        let (state, why) = if ready {
            (DependencyState::Ready, reason::HEALTHY)
        } else {
            (DependencyState::NotReady, reason::NOT_READY)
        };
        self.observe(state, why, Instant::now());
    }
}

/// 汇总全部运行依赖动态就绪状态的通用注册表。
///
/// 没有贡献项时聚合结果 `ready=true`,保证未声明动态依赖的既有应用语义不变。组件在
/// Start/UserHook 登记，hosted initializer 可在 Initialization 登记；`seal` 之后返回
/// [`RegisterError::Sealed`]。运行期只读
/// 各 entry 的已发布状态并聚合,不做网络 I/O。
pub struct ReadinessRegistry {
    entries: RwLock<BTreeMap<Arc<str>, Arc<Mutex<EntryState>>>>,
    sealed: AtomicBool,
}

impl ReadinessRegistry {
    /// 业务作用：创建没有动态贡献项的注册表。
    ///
    /// # 返回
    ///
    /// 聚合 `ready=true` 的空注册表,直到组件注册关键贡献项。
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(BTreeMap::new()),
            sealed: AtomicBool::new(false),
        }
    }

    /// 业务作用：注册一个带稳定组件归属的贡献项。
    pub fn register_component(
        &self,
        component: impl Into<Arc<str>>,
        name: impl Into<Arc<str>>,
        policy: ReadinessPolicy,
    ) -> Result<ReadinessContributor, RegisterError> {
        if policy.failure_threshold == 0 || policy.recovery_threshold == 0 {
            return Err(RegisterError::InvalidPolicy);
        }
        if self.sealed.load(Ordering::Acquire) {
            return Err(RegisterError::Sealed);
        }
        let name = name.into();
        if name.trim().is_empty() {
            return Err(RegisterError::EmptyName);
        }
        let mut entries = self
            .entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 二次检查:封口与写锁之间可能发生封口。
        if self.sealed.load(Ordering::Acquire) {
            return Err(RegisterError::Sealed);
        }
        if entries.contains_key(&name) {
            return Err(RegisterError::Duplicate);
        }
        let component = component.into();
        if component.trim().is_empty() {
            return Err(RegisterError::EmptyName);
        }
        let entry = Arc::new(Mutex::new(EntryState::new(component, policy)));
        entries.insert(name, Arc::clone(&entry));
        Ok(ReadinessContributor { entry })
    }

    /// 业务作用：封口注册表:此后 `register` 返回 [`RegisterError::Sealed`]。
    ///
    /// 由 Application 在 Initialization 结束的 Seal 边界调用，防止运行期无界新增贡献项名称。
    pub fn seal(&self) {
        self.sealed.store(true, Ordering::Release);
    }

    /// 业务作用：计入 stale 后聚合全体依赖,得出对流量的最终裁决。
    ///
    /// # 参数
    ///
    /// - `now`:当前单调时刻,用于 stale 判定。
    ///
    /// # 返回
    ///
    /// [`ReadinessSnapshot`]:`ready` 表示无关键依赖处于未就绪;`degraded` 表示存在可恢复降级;
    /// `entries` 为按名称有序的各依赖有效状态。O(贡献项数量),无网络 I/O。
    pub fn snapshot(&self, now: Instant) -> ReadinessSnapshot {
        let entries = self
            .entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut snapshots: Vec<DependencySnapshot> = Vec::with_capacity(entries.len());
        let mut ready = true;
        let mut degraded = false;
        for (name, entry) in entries.iter() {
            let (component, affects_ready, state, reason) = {
                let guard = entry
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let (state, reason) = guard.effective(now);
                (
                    Arc::clone(&guard.component),
                    guard.policy.affects_ready,
                    state,
                    reason,
                )
            };
            match state {
                DependencyState::Ready => {}
                DependencyState::Degraded => degraded = true,
                DependencyState::NotReady | DependencyState::Unknown => {
                    if affects_ready {
                        ready = false;
                    } else {
                        degraded = true;
                    }
                }
            }
            snapshots.push(DependencySnapshot {
                component,
                name: Arc::clone(name),
                state,
                reason,
                affects_ready,
            });
        }
        ReadinessSnapshot {
            ready,
            degraded,
            entries: Arc::from(snapshots),
        }
    }

    /// 业务作用：旧布尔契约兼容入口:聚合是否可接收流量(等价 `snapshot(Instant::now()).ready`)。
    ///
    /// # 返回
    ///
    /// 无贡献项或无关键依赖处于未就绪时返回 true;否则 false。
    pub fn all_ready(&self) -> bool {
        self.snapshot(Instant::now()).ready
    }
}

impl Default for ReadinessRegistry {
    /// 业务作用：创建尚未封口且不含贡献项的注册表。
    fn default() -> Self {
        Self::new()
    }
}
