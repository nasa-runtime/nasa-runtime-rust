//! route 级授权中间件:把 [`naauthz`] 策略决策接到 Web 请求路径。
//!
//! 从请求扩展读取 [`Principal`](由上游 authentication 层写入),对 `METHOD /path` route 查
//! [`PolicyRegistry`] 决策:Permit 放行;Deny → 403 [`ApiProblem`](不回显主体敏感数据)。装在
//! authentication 之后、decrypt/handler 之前。

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use naauthz::{AuthzDecision, Principal};

use crate::problem::ApiProblem;

/// 业务/Auth 层发布的进程级授权策略注册表(热更新保 last-good)。
pub type SharedPolicyRegistry = Arc<naauthz::PolicyRegistry>;
/// 业务注入的对象级授权 provider。
pub type SharedObjectAuthorizer = Arc<dyn naauthz::ObjectAuthorizer>;

/// Web Ready 冻结的授权层状态。
#[derive(Clone)]
pub struct AuthorizationLayerState {
    registry: Option<SharedPolicyRegistry>,
    object_authorizer: Option<SharedObjectAuthorizer>,
    object_timeout: std::time::Duration,
    unmatched: naauthz::UnmatchedRoutePolicy,
    /// 未命中缺省的豁免 route 集合:声明公开(auth_required=false)的业务路由与框架探针路由。
    /// 豁免只作用于"未命中任何策略"的分支——显式写了策略的 route 永远按策略裁决。
    unmatched_exempt: std::sync::Arc<std::collections::HashSet<String>>,
}

/// Observe 模式下"若翻转为 Deny 将被拒绝"的累计命中数(进程级、低成本、可测可查)。
static UNMATCHED_OBSERVED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Deny 模式下因未命中策略被拒绝的累计请求数。
static UNMATCHED_DENIED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Observe 告警已输出过的 route 集合:灰度清点需要的是"哪些 route 待补策略"的枚举,每个 route
/// 首次命中告警一次即可;逐请求告警会在灰度期(多数路由尚未写策略)刷爆日志。规模证据由计数承担。
static OBSERVE_LOGGED_ROUTES: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashSet<String>>,
> = std::sync::OnceLock::new();
/// Observe 告警枚举的容量护栏。route_id 是路由模板,正常低基数;护栏防的是异常形态
/// (如动态拼出的海量模板)把枚举集合变成无界内存。到达上限后不再新增告警,计数不受影响。
const OBSERVE_LOG_ROUTE_CAP: usize = 512;

/// 业务作用：裁决一次 Observe 命中是否需要输出告警日志——每个 route 只在首次命中时告警。
///
/// 参数说明：
/// - `route_id`: 命中的路由模板标识。
///
/// 返回：该 route 首次命中且枚举集合未达容量护栏时为 `true`；重复命中或已达护栏为 `false`。
fn observe_should_log(route_id: &str) -> bool {
    let mut logged = OBSERVE_LOGGED_ROUTES
        .get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if logged.contains(route_id) || logged.len() >= OBSERVE_LOG_ROUTE_CAP {
        return false;
    }
    logged.insert(route_id.to_owned());
    true
}

/// Ready 期覆盖对账写入的"已命中策略的有效 route 数";供指标源投影为 gauge。
static AUTHZ_ROUTES_COVERED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Ready 期覆盖对账写入的"有鉴权要求但未命中任何策略的 route 数";deny 缺省下该值恒为 0
/// (非零直接阻断 Ready),permit/observe 下它就是翻转前必须清零的漏配面。
static AUTHZ_ROUTES_UNCOVERED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// 业务作用：由启动期覆盖对账写入覆盖账目，使漏配面进入指标出口而不只在启动日志里一闪而过。
///
/// 参数说明：
/// - `covered`: 命中策略的有效 route 数。
/// - `uncovered`: 有鉴权要求但未命中任何策略的 route 数(截断展示前的完整数量)。
///
/// 返回：无返回值；以原子写入发布两项进程级覆盖账目，供统一指标源读取。
pub(crate) fn record_coverage_audit(covered: u64, uncovered: u64) {
    use std::sync::atomic::Ordering::Relaxed;
    AUTHZ_ROUTES_COVERED.store(covered, Relaxed);
    AUTHZ_ROUTES_UNCOVERED.store(uncovered, Relaxed);
}

static AUTHZ_UNMATCHED_OBSERVED_DESCRIPTOR: nametrics_core::MetricDescriptor =
    nametrics_core::MetricDescriptor {
        name: "napp_authz_unmatched_observed_total",
        help: "Observe 缺省下未命中任何授权策略且若翻转为 deny 将被拒绝的请求累计数。",
        unit: "",
        kind: nametrics_core::MetricKind::Counter,
        label_names: &[],
        histogram_bounds: &[],
    };
static AUTHZ_UNMATCHED_DENIED_DESCRIPTOR: nametrics_core::MetricDescriptor =
    nametrics_core::MetricDescriptor {
        name: "napp_authz_unmatched_denied_total",
        help: "Deny 缺省下因未命中任何授权策略被拒绝的请求累计数。",
        unit: "",
        kind: nametrics_core::MetricKind::Counter,
        label_names: &[],
        histogram_bounds: &[],
    };
static AUTHZ_ROUTES_COVERED_DESCRIPTOR: nametrics_core::MetricDescriptor =
    nametrics_core::MetricDescriptor {
        name: "napp_authz_routes_covered",
        help: "启动期覆盖对账中命中授权策略的有效 route 数。",
        unit: "",
        kind: nametrics_core::MetricKind::Gauge,
        label_names: &[],
        histogram_bounds: &[],
    };
static AUTHZ_ROUTES_UNCOVERED_DESCRIPTOR: nametrics_core::MetricDescriptor =
    nametrics_core::MetricDescriptor {
        name: "napp_authz_routes_uncovered",
        help: "启动期覆盖对账中有鉴权要求但未命中任何授权策略的 route 数(deny 缺省下恒为 0)。",
        unit: "",
        kind: nametrics_core::MetricKind::Gauge,
        label_names: &[],
        histogram_bounds: &[],
    };
static AUTHZ_METRIC_DESCRIPTORS: [&nametrics_core::MetricDescriptor; 4] = [
    &AUTHZ_UNMATCHED_OBSERVED_DESCRIPTOR,
    &AUTHZ_UNMATCHED_DENIED_DESCRIPTOR,
    &AUTHZ_ROUTES_COVERED_DESCRIPTOR,
    &AUTHZ_ROUTES_UNCOVERED_DESCRIPTOR,
];
/// 授权治理指标的最坏序列数:四个无 label family 各一条,启动期一次性预留。
pub(crate) const AUTHZ_METRIC_SERIES: usize = AUTHZ_METRIC_DESCRIPTORS.len();

/// 授权治理观测源:把未命中缺省计数与启动期覆盖账目投影为固定低基数序列。
///
/// 值取自进程级原子(与 [`unmatched_observed_total`] 等读取函数同源),仅在装配了授权层的
/// 应用中注册——未启用授权时不虚增"漏配为零"的误导序列。
pub(crate) struct AuthzMetricsSource {
    registry: Option<SharedPolicyRegistry>,
}

impl AuthzMetricsSource {
    /// 业务作用：绑定授权注册表，使覆盖 gauge 随每次成功 policy reload 更新。
    ///
    /// 参数说明：
    /// - registry：已安装覆盖合同的 route 策略注册表；仅对象授权场景可为空。
    ///
    /// 返回：固定四个指标族的只读兼容源。
    pub(crate) fn new(registry: Option<SharedPolicyRegistry>) -> Self {
        Self { registry }
    }
}

impl nametrics_core::LegacyMetricsSource for AuthzMetricsSource {
    /// 业务作用：返回授权治理指标的固定 family 目录，供启动期冲突与容量审计。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：两 counter(unmatched observe/deny)加两 gauge(覆盖账目)。
    fn descriptors(&self) -> &'static [&'static nametrics_core::MetricDescriptor] {
        &AUTHZ_METRIC_DESCRIPTORS
    }

    /// 业务作用：把授权治理的当前事实投影为恒定四条样本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无 label 的四条样本；未发生任何未命中事件时保持全零。
    fn snapshot(&self) -> Option<Vec<nametrics_core::MetricSample>> {
        use std::sync::atomic::Ordering::Relaxed;
        let coverage = self
            .registry
            .as_ref()
            .and_then(|registry| registry.coverage_audit());
        let covered = coverage
            .map(|audit| audit.covered)
            .unwrap_or_else(|| AUTHZ_ROUTES_COVERED.load(Relaxed));
        let uncovered = coverage
            .map(|audit| audit.uncovered)
            .unwrap_or_else(|| AUTHZ_ROUTES_UNCOVERED.load(Relaxed));
        Some(vec![
            nametrics_core::MetricSample {
                name: AUTHZ_UNMATCHED_OBSERVED_DESCRIPTOR.name,
                labels: Vec::new(),
                value: nametrics_core::MetricValue::Counter(UNMATCHED_OBSERVED.load(Relaxed)),
            },
            nametrics_core::MetricSample {
                name: AUTHZ_UNMATCHED_DENIED_DESCRIPTOR.name,
                labels: Vec::new(),
                value: nametrics_core::MetricValue::Counter(UNMATCHED_DENIED.load(Relaxed)),
            },
            nametrics_core::MetricSample {
                name: AUTHZ_ROUTES_COVERED_DESCRIPTOR.name,
                labels: Vec::new(),
                value: nametrics_core::MetricValue::Gauge(covered as f64),
            },
            nametrics_core::MetricSample {
                name: AUTHZ_ROUTES_UNCOVERED_DESCRIPTOR.name,
                labels: Vec::new(),
                value: nametrics_core::MetricValue::Gauge(uncovered as f64),
            },
        ])
    }

    /// 业务作用：结构化快照已覆盖全部序列，文本渲染统一由 hub 完成，本源不自渲染。
    ///
    /// 参数说明：
    /// - `_output`: 未使用的文本缓冲区。
    ///
    /// 返回：无。
    fn render_prometheus(&self, _output: &mut String) {}
}

/// 业务作用：读取 Observe 模式的累计命中数，供漏配面清点与运行核对。
///
/// 参数说明: 无。
///
/// 返回：进程启动以来的累计计数。
pub fn unmatched_observed_total() -> u64 {
    UNMATCHED_OBSERVED.load(std::sync::atomic::Ordering::Relaxed)
}

/// 业务作用：读取 Deny 模式的累计拒绝数，供运行验证与告警核对。
///
/// 参数说明: 无。
///
/// 返回：进程启动以来的累计计数。
pub fn unmatched_denied_total() -> u64 {
    UNMATCHED_DENIED.load(std::sync::atomic::Ordering::Relaxed)
}

impl AuthorizationLayerState {
    /// 业务作用：构造 route + object 授权统一边界；未命中缺省按兼容语义放行，
    /// 由 [`Self::with_unmatched_policy`] 分级收紧。
    ///
    /// # 参数
    ///
    /// - `registry`：route 策略注册表；`None` 表示不启用 route 授权。
    /// - `object_authorizer`：对象级授权 provider；`None` 表示不启用对象授权。
    /// - `object_timeout`：对象授权调用预算，超出生命周期上限时被收敛到上限。
    ///
    /// # 返回
    ///
    /// 未命中缺省为 `Permit` 的授权层状态。
    pub fn new(
        registry: Option<SharedPolicyRegistry>,
        object_authorizer: Option<SharedObjectAuthorizer>,
        object_timeout: std::time::Duration,
    ) -> Self {
        Self {
            registry,
            object_authorizer,
            // 公开低层构造器不能绕过应用 UserHook 的上限，防 Duration::MAX 进入 Tokio timeout。
            object_timeout: object_timeout.min(crate::runner::MAX_LIFECYCLE_TIMEOUT),
            unmatched: naauthz::UnmatchedRoutePolicy::Permit,
            unmatched_exempt: std::sync::Arc::new(std::collections::HashSet::new()),
        }
    }

    /// 业务作用：设置 route 未命中任何策略时的缺省裁决，用于从兼容放行分级收紧到拒绝。
    ///
    /// 参数说明：
    /// - `policy`: 三态缺省；`Observe` 放行但留证据，`Deny` 直接 403。
    ///
    /// 返回：更新后的状态。
    pub fn with_unmatched_policy(mut self, policy: naauthz::UnmatchedRoutePolicy) -> Self {
        self.unmatched = policy;
        self
    }

    /// 业务作用：冻结未命中缺省的豁免 route 集合——声明公开的业务路由与框架探针路由不受
    /// Observe/Deny 影响。
    ///
    /// 豁免必须与启动期覆盖对账的口径一致：对账把 auth_required=false 视为显式豁免,运行期若不
    /// 同步豁免,Deny 会拒绝对账宣称"无需策略"的公开路由,探针路由更会被 403 打死存活检查。
    ///
    /// 参数说明：
    /// - `exempt`: 完整 route_id(`METHOD {context_path}{path}`)集合,Web Ready 期一次构建。
    ///
    /// 返回：更新后的状态。
    pub fn with_unmatched_exemptions(
        mut self,
        exempt: std::sync::Arc<std::collections::HashSet<String>>,
    ) -> Self {
        self.unmatched_exempt = exempt;
        self
    }
}

/// 业务作用：授权中间件。命中策略的 route 按策略裁决;未命中策略的 route 按三态缺省裁决,
/// 其中声明公开的路由、框架探针路由与未命中真实路由的请求(router 兜底 404)不受缺省收紧影响。
///
/// 主体来自请求扩展 `Principal`——未认证/无主体时视为空 scope 集,受保护 route 将被拒。
///
/// route_id 优先取路由**模板**([`axum::extract::MatchedPath`],如 `GET /users/{id}`)；含动态段的
/// 路由必须按模板写策略。没有模板表示 router 未命中真实路由，此时保留 404，不把任意原始 path
/// 当成可配置 route，也不让扫描流量污染 Observe 证据。
///
/// 参数说明：
/// - `state`：授权 registry、启动期缺省、公开 route 豁免与对象授权配置。
/// - `request`：携带已认证主体和 Axum 路由模板的入站请求。
/// - `next`：授权通过后继续执行的下游服务。
///
/// 返回：放行请求返回下游响应；拒绝时返回不泄露主体和策略细节的 403。
pub async fn authorize(
    State(state): State<AuthorizationLayerState>,
    mut request: Request,
    next: Next,
) -> Response {
    let matched_template = request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|matched| matched.as_str().to_owned());
    // 是否命中真实路由:未命中(无 MatchedPath)的请求没有可写策略的路由身份,由 router 兜底 404;
    // 对它们套用未命中三态只会把 404 变 403,并让 Observe 的枚举被扫描器的任意 path 灌爆。
    let routed = matched_template.is_some();
    let path = matched_template.unwrap_or_else(|| request.uri().path().to_owned());
    let route_id = format!("{} {path}", request.method());
    let principal = request
        .extensions()
        .get::<Principal>()
        .cloned()
        .unwrap_or_default();

    let (registry_snapshot, coverage_installed) = state
        .registry
        .as_ref()
        .map(|registry| registry.request_snapshot())
        .unwrap_or_else(|| {
            (
                naauthz::PolicyDecisionSnapshot::new(
                    Arc::new(naauthz::PolicySet::default()),
                    state.unmatched,
                    0,
                ),
                false,
            )
        });
    let route_snapshot = if coverage_installed {
        registry_snapshot
    } else {
        // 覆盖合同安装前 registry 尚不知道 Web 的启动配置；只替换未命中缺省并复用同代策略，
        // 同时冻结公开 route 豁免，避免 Web 与 handler 对同一请求推导出相反结果。
        registry_snapshot
            .with_unmatched(state.unmatched)
            .with_unmatched_exemptions(Arc::clone(&state.unmatched_exempt))
    };
    let effective_unmatched = route_snapshot.unmatched();
    let unmatched_exempt = route_snapshot.is_unmatched_exempt(&route_id);
    let security = naauthz::RequestSecurityContext::new(
        principal,
        route_snapshot,
        state.object_authorizer,
        state.object_timeout,
    );
    let protected = security.policy_set().is_protected(&route_id);
    // 未命中任何策略的 route 按三态缺省裁决：兼容语义是放行（decide 内部即如此），Observe 在放行的
    // 同时留下"若翻转即拒绝"的证据供清点漏配,Deny 把缺省翻转为 fail-closed——与对象授权的
    // 失败语义对齐。已命中策略的 route 不受该缺省影响;未命中真实路由的请求交给 router 兜底 404;
    // 声明公开的路由与框架探针在豁免集合内,与启动期覆盖对账"公开即显式豁免"的口径一致。
    let decision = if !routed {
        AuthzDecision::Permit
    } else {
        if !protected && !unmatched_exempt {
            match effective_unmatched {
                naauthz::UnmatchedRoutePolicy::Permit => {}
                naauthz::UnmatchedRoutePolicy::Observe => {
                    UNMATCHED_OBSERVED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // route_id 为路由模板,每 route 首次命中告警一次(供灰度期清点待补策略的 route),
                    // 命中规模由计数承担,避免灰度期逐请求告警刷爆日志。
                    if observe_should_log(&route_id) {
                        tracing::warn!(
                            route_id = %route_id,
                            "authz unmatched-route observe: 该 route 未命中任何授权策略,当前放行;缺省翻转为 deny 后将被拒绝"
                        );
                    }
                }
                naauthz::UnmatchedRoutePolicy::Deny => {
                    UNMATCHED_DENIED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
        security.decide_route(&route_id)
    };
    request.extensions_mut().insert(security);
    match decision {
        AuthzDecision::Permit => next.run(request).await,
        AuthzDecision::Deny(_) => ApiProblem::new(
            "about:blank",
            "Forbidden",
            StatusCode::FORBIDDEN,
            "forbidden",
        )
        .with_detail("the authenticated principal is not permitted to access this resource")
        .into_response(),
    }
}
