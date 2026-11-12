//! 运行依赖的动态就绪聚合。
//!
//! 数据源、缓存、消息消费与安全运行时等依赖注册贡献项，并通过 `observe` 更新观测。
//! 聚合只读取内存状态，不执行网络探测；Application 的生命周期限制由调用方另外合并。
//!
//! 观测在单项锁内推进失败与恢复阈值；快照持有注册表读锁并依次读取各项，不能视为跨依赖
//! 同时采样。未知或未就绪的关键依赖阻止接流，非关键依赖只使聚合结果降级。
//! 新鲜度窗口和租约均使用单调时间，读取时复验期限，避免后台发布停止后一直保持就绪。
//!
//! 原因码使用 `&'static str`，调用方仍须保证不含秘密；类型本身不执行脱敏。

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
    #[cfg(any(
        feature = "db",
        feature = "db-pgsql",
        feature = "db-sqlite",
        feature = "nacos-discovery"
    ))]
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
        feature = "db-sqlite",
        feature = "redis",
        feature = "cache",
        feature = "partition",
        feature = "grpc",
        feature = "nacos-config",
        feature = "nacos-discovery",
        feature = "discovery-runtime",
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
    /// 业务作用：为无需周期刷新、每次观测立即生效的关键依赖创建就绪策略。
    ///
    /// 参数说明：无。
    ///
    /// 返回：`affects_ready=true`、两个阈值均为 1、`stale_after=None` 的策略。
    /// 新注册的贡献项仍为 Unknown，须收到有效观测后才能参与就绪判定。
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
    /// 注册时固定的组件身份，用于按组件聚合诊断。
    component: Arc<str>,
    /// 注册时固定的阈值、关键性与新鲜度约束。
    policy: ReadinessPolicy,
    /// 最近达到阈值或被显式降级的状态，读取时还需复验期限。
    published: DependencyState,
    /// 与已发布状态一起更新的静态原因码。
    reason: &'static str,
    /// 连续 Ready 观测次数，失败或降级会清零。
    success_streak: u32,
    /// 连续 NotReady 或 Unknown 观测次数，成功或降级会清零。
    fail_streak: u32,
    /// 新鲜度起点；尚无观测时为空，保留旧数据的失败裁决不推进该时刻。
    last_observed: Option<Instant>,
    /// 租约贡献的绝对单调期限，普通观测会清除此限制。
    valid_until: Option<Instant>,
}

impl EntryState {
    /// 业务作用：创建尚无观测的贡献项状态，并冻结组件身份与阈值策略。
    ///
    /// 参数说明：
    /// - `component`：`component` 是组件身份。
    /// - `policy`：`policy` 决定依赖失败如何影响整体就绪。
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
    /// 参数说明：
    /// - `raw`：`raw` 是原始状态。
    /// - `observed_reason`：`observed_reason` 是静态原因码。
    /// - `now`：`now` 是本次观测的单调时刻。
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
    /// 参数说明：
    /// - `now`：`now` 是本次读取的单调时刻。
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
    /// 仅指向本贡献项的共享状态，克隆句柄不会新增注册项。
    entry: Arc<Mutex<EntryState>>,
}

impl ReadinessContributor {
    /// 业务作用：在统一接流点复验已登记证据的新鲜度。
    ///
    /// 参数说明：无。
    ///
    /// 返回：当前证据仍为 Ready 时为 true，过期证据不会延长接流权威。
    #[cfg(any(feature = "redis", feature = "ws-client", feature = "hystrix"))]
    pub(crate) fn ready_now(&self) -> bool {
        self.entry.lock().unwrap().effective(Instant::now()).0 == DependencyState::Ready
    }
    /// 业务作用：发布有租约期限的就绪证据，期限到达后由读取端独立摘流。
    ///
    /// 参数说明：
    /// - `deadline`：本次租约证据最早失效的单调时刻；若聚合多份证据，应取其中最早期限。
    ///
    /// 返回：在同一临界区推进观测并保存期限；未来期限按恢复阈值推进 Ready，已到期证据计作失败。
    /// 无论是否达到阈值，读取到已到期的期限时，关键依赖立即为 NotReady，非关键依赖为 Degraded。
    /// 此方法只记录证据，不取得或续租外部控制权。
    #[cfg(any(
        feature = "saga",
        feature = "saga-pgsql",
        feature = "discovery-runtime",
        feature = "config-watch"
    ))]
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
        }
        // advance 会清除上一轮期限；本轮即使已过期也必须保存，避免失败阈值保留 Ready 后失去失效边界。
        entry.valid_until = Some(deadline);
    }

    /// 业务作用：以本次探测更新连续成功或失败计数，由注册策略决定是否发布新的依赖状态。
    ///
    /// 参数说明：
    /// - `state`：原始观测；Ready 与 NotReady 按各自阈值发布，Degraded 立即发布，Unknown 计作失败。
    /// - `reason`：不含凭据或地址等动态内容的静态原因码，仅在发布状态时替换原原因。
    /// - `now`：本次探测的单调时刻，作为后续新鲜度检查的起点；调用方须按观测顺序传入。
    ///
    /// 返回：在贡献项锁内推进观测并刷新新鲜度，同时清除先前的租约期限；未达到阈值时保留已发布状态。
    /// 此方法记录探测结果，不确认业务接流，也不续租任何外部资源。
    pub fn observe(&self, state: DependencyState, reason: &'static str, now: Instant) {
        self.entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .advance(state, reason, now);
    }

    /// 业务作用：更新依赖状态而保留原新鲜度起点，使刷新失败不能延长上一份可用数据的有效窗口。
    ///
    /// 参数说明：
    /// - `state`：本次裁决，通常为 Degraded 或 NotReady；仍遵守注册时的连续观测阈值。
    /// - `reason`：不含动态错误内容的静态原因码。
    /// - `now`：本次裁决的单调时刻，参与状态推进，但不会成为新的新鲜度起点。
    ///
    /// 返回：在同一临界区推进计数与发布状态并恢复原观测时刻；先前的租约期限仍被清除。
    /// 适用于 JWKS 等保留上一份可用数据的刷新流程；调用前须已有有效观测并配置新鲜度窗口，
    /// 否则此方法不会建立自动过期边界。普通健康探测使用 [`observe`](Self::observe)。
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

    /// 业务作用：把组件的布尔健康结论转换为一次带静态原因码的就绪观测。
    ///
    /// 参数说明：
    /// - `ready`：true 记录 Ready 与 healthy，false 记录 NotReady 与 not_ready。
    ///
    /// 返回：使用当前单调时刻刷新观测；状态何时发布取决于贡献项注册时的阈值，
    /// 此入口不更换策略，也不直接修改 Application 的生命周期状态。
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
    /// 按稳定名称排序的贡献项；注册表锁保护目录，各项锁保护状态与期限。
    entries: RwLock<BTreeMap<Arc<str>, Arc<Mutex<EntryState>>>>,
    /// 初始化结束后的单向注册门禁，不影响已有贡献项更新观测。
    sealed: AtomicBool,
}

impl ReadinessRegistry {
    /// 业务作用：创建没有动态贡献项的注册表。
    ///
    /// 参数说明：无。
    ///
    /// 返回：尚未封口的空注册表；此时快照 ready=true、degraded=false，仅表示没有依赖阻止接流，
    /// 不代表 Application 已完成启动或业务监听已开放。
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(BTreeMap::new()),
            sealed: AtomicBool::new(false),
        }
    }

    /// 业务作用：注册一个带稳定组件归属的贡献项。
    ///
    /// 参数说明：
    /// - `component`：供诊断聚合使用的组件身份，去除首尾空白后不得为空，存储时保留原值。
    /// - `name`：注册表内全局唯一的稳定名称，按完整原值判重；不得为空白或含凭据等敏感内容。
    /// - `policy`：冻结的关键性、观测阈值与新鲜度窗口；两个阈值必须大于零。
    ///
    /// 返回：成功时插入 Unknown 贡献项并返回独立观测句柄；非法阈值、空白身份或名称、
    /// 名称重复、观察到注册表已封口时返回对应错误，已有贡献项不会被替换。
    /// 此处不验证组件是否已加载，也不校验新鲜度窗口与探测间隔的关系。
    pub fn register_component(
        &self,
        component: impl Into<Arc<str>>,
        name: impl Into<Arc<str>>,
        policy: ReadinessPolicy,
    ) -> Result<ReadinessContributor, RegisterError> {
        if policy.failure_threshold == 0 || policy.recovery_threshold == 0 {
            // 零阈值会使连续观测策略失去意义，不允许构造这样的贡献项。
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
        // 等待目录写锁期间可能已封口，必须在插入前再次检查注册权限。
        if self.sealed.load(Ordering::Acquire) {
            return Err(RegisterError::Sealed);
        }
        if entries.contains_key(&name) {
            // 稳定名称必须唯一，不能替换已有句柄所绑定的状态。
            return Err(RegisterError::Duplicate);
        }
        let component = component.into();
        if component.trim().is_empty() {
            return Err(RegisterError::EmptyName);
        }
        let entry = Arc::new(Mutex::new(EntryState::new(component, policy)));
        entries.insert(name, Arc::clone(&entry));
        Ok(ReadinessContributor {
            entry,
        })
    }

    /// 业务作用：在初始化结束时关闭贡献项注册入口，避免运行期间持续增加诊断名称。
    ///
    /// 参数说明：无。
    ///
    /// 返回：发布不可撤销的封口标志；后续注册观察到该标志时返回 [`RegisterError::Sealed`]。
    /// 已在目录写锁内通过封口复验的注册仍可完成，本方法不等待这些在途注册；快照会等待目录锁，
    /// 因此不会读到部分插入。已有贡献项仍可更新状态，封口本身不代表全部依赖已就绪。
    pub fn seal(&self) {
        self.sealed.store(true, Ordering::Release);
    }

    /// 业务作用：复验每项观测的新鲜度与租约期限，汇总依赖对接流的限制。
    ///
    /// 参数说明：
    /// - `now`：本次聚合共用的单调时刻，等于或晚于新鲜度边界、租约期限时视为过期。
    ///
    /// 返回：按名称排序的有效状态；关键依赖为 Unknown 或 NotReady 时 ready=false，
    /// 任一依赖为 Degraded 或非关键依赖未就绪时 degraded=true。空注册表 ready=true。
    /// 读取持有注册表读锁并依次取得贡献项锁，各项不构成同一瞬间的原子快照；不执行网络探测，
    /// 不改变已发布状态，也不检查 Application 生命周期。
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

    /// 业务作用：在当前单调时刻聚合依赖是否允许接流，供只需要布尔结论的调用方使用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：无贡献项或所有关键依赖的有效状态均为 Ready 或 Degraded 时为 true，
    /// 否则为 false；不检查 Application 生命周期，也不保留本次判断之后的接流权威。
    pub fn all_ready(&self) -> bool {
        self.snapshot(Instant::now()).ready
    }
}

impl Default for ReadinessRegistry {
    /// 业务作用：创建尚未封口且不含贡献项的注册表。
    ///
    /// 参数说明：无。
    ///
    /// 返回：与 [`Self::new`] 相同的空注册表；依赖聚合允许接流，但不承诺应用已启动。
    fn default() -> Self {
        Self::new()
    }
}
