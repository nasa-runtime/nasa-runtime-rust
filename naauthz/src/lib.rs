//! NASA route 级授权核心。
//!
//! # 完整裁决快照
//!
//! authentication 确认“你是谁”，authz 判定“已验证主体能否访问稳定 route 或业务对象”。
//! [`PolicyRegistry`] 把 [`PolicySet`]、[`UnmatchedRoutePolicy`]、公开 route 豁免与 generation 作为
//! 同代状态校验后原子发布；请求通过 [`PolicyDecisionSnapshot`] 冻结该状态，Web、registry 便捷入口
//! 与 handler 不会在同一请求中读取不同代裁决或不同公开性事实。候选发布失败时保留 last-good。
//!
//! 显式 route 策略按 All/Any scope 裁决；未命中时由快照中的三态缺省决定。对象授权 provider
//! 缺失、拒绝、错误或超时均 fail-closed。本 crate 不验签 token、不推断对象归属，也不依赖 `napp`；
//! 策略来源、稳定 route ID、公开 route 覆盖和已验证 [`Principal`] 由接入层提供。

#![forbid(unsafe_code)]

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;

/// 对象授权 helper 的最长单次等待；构造器保持非 fallible，极端输入在边界收敛。
const MAX_OBJECT_AUTHORIZATION_TIMEOUT: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// 需要 scope 的满足方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequireMode {
    /// 必须具备**全部** required scope。
    All,
    /// 具备**任一** required scope 即可。
    Any,
}

/// 一条 route 的授权要求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutePolicy {
    /// 编译期稳定 route 标识。
    pub route_id: String,
    /// 该 route 要求的 scope。
    pub required_scopes: BTreeSet<String>,
    /// All / Any。
    pub mode: RequireMode,
}

/// 已认证主体。
///
/// `subject`/`client_id`/`tenant` 都来自**已经验签并完成 claims 校验**的 access token。除授权外，
/// 请求治理层还会用这些稳定身份字段构造幂等命名空间，不能在 authentication → authorization
/// 的转换过程中丢弃。
#[derive(Debug, Clone, Default)]
pub struct Principal {
    /// OAuth subject (`sub`)。
    pub subject: Option<String>,
    /// OAuth client id (`client_id`)；client-credentials token 可能只有该字段。
    pub client_id: Option<String>,
    /// 可选租户标识。
    pub tenant: Option<String>,
    /// 已授予 scope。
    pub scopes: BTreeSet<String>,
}

impl Principal {
    /// 业务作用：用 scope 列表构造。
    pub fn with_scopes<I, S>(scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            scopes: scopes.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }

    /// 业务作用：返回可用于安全命名空间的认证身份：优先 `sub`，否则 `client_id`。
    pub fn authenticated_identity(&self) -> Option<&str> {
        self.subject
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                self.client_id
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
            })
    }
}

/// 授权裁决。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthzDecision {
    /// 放行(route 无策略,或主体满足要求)。
    Permit,
    /// 拒绝,附缺失/不满足原因(不含主体敏感数据)。
    Deny(DenyReason),
}

/// 拒绝原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DenyReason {
    /// 缺少全部要求 scope 中的某些(All 模式)。
    MissingRequiredScopes(BTreeSet<String>),
    /// 一个都不满足(Any 模式)。
    NoMatchingScope,
    /// route 未命中显式策略，且同代缺省要求 fail-closed。
    UnmatchedRoute,
}

/// route 未命中任何策略时的缺省裁决。
///
/// 兼容缺省是放行（authz 不适用即不拦截），与对象授权的 fail-closed 形成不一致；漏配策略的
/// 受保护 route 会静默放行。三态允许分级收紧：`Observe` 保持放行但对每次"若翻转即拒绝"的
/// 命中留下可观测证据，供存量业务在真实流量下清点漏配面；灰度完成后切 `Deny`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UnmatchedRoutePolicy {
    /// 未命中策略即放行(兼容缺省)。
    #[default]
    Permit,
    /// 放行但记录"若为 Deny 将被拒绝"的证据，用于翻转前清点漏配。
    Observe,
    /// 未命中策略即拒绝。接入层必须为确属公开的入口提供显式豁免通道(如按"声明公开"的路由
    /// 元数据豁免),并保证健康探针等基础入口不被本缺省切断;其余未覆盖 route 一律 fail-closed。
    Deny,
}

impl UnmatchedRoutePolicy {
    /// 业务作用：把配置文本解析为未命中缺省，供 YAML/UserHook 共用同一稳定词表。
    ///
    /// 参数说明：
    /// - `value`: 小写策略文本，允许 `permit`、`observe`、`deny`。
    ///
    /// 返回：合法文本返回对应策略；未知文本返回 `None`，由调用方给出定位明确的配置错误。
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "permit" => Some(Self::Permit),
            "observe" => Some(Self::Observe),
            "deny" => Some(Self::Deny),
            _ => None,
        }
    }

    /// 业务作用：返回策略的稳定小写文本，用于日志与低基数指标标签。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`permit` / `observe` / `deny`。
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Permit => "permit",
            Self::Observe => "observe",
            Self::Deny => "deny",
        }
    }
}

/// policy 校验或覆盖合同错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyError {
    /// 某策略 route_id 为空。
    EmptyRouteId,
    /// 某策略未声明任何 required scope(无意义)。
    EmptyRequiredScopes(String),
    /// 出现重复 route_id。
    DuplicateRoute(String),
    /// 候选策略指向当前应用不存在的 route。
    DanglingRoutes(Vec<String>),
    /// fail-closed 缺省下仍有要求授权但没有策略的 route。
    UncoveredRoutes(Vec<String>),
    /// 同一注册表重复安装应用路由合同。
    CoverageAlreadyInstalled,
}

impl std::fmt::Display for PolicyError {
    /// 业务作用：输出不含主体、scope 内容或请求数据的稳定策略错误摘要。
    ///
    /// 参数说明：
    /// - formatter：标准格式化目标。
    ///
    /// 返回：错误摘要写入成功时返回成功，否则返回格式化错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyRouteId => formatter.write_str("policy route_id cannot be empty"),
            Self::EmptyRequiredScopes(_) => {
                formatter.write_str("policy required_scopes cannot be empty")
            }
            Self::DuplicateRoute(_) => formatter.write_str("policy route_id is duplicated"),
            Self::DanglingRoutes(routes) => write!(
                formatter,
                "{} policy route(s) do not exist in the installed route contract",
                routes.len()
            ),
            Self::UncoveredRoutes(routes) => write!(
                formatter,
                "{} protected route(s) are uncovered under deny policy",
                routes.len()
            ),
            Self::CoverageAlreadyInstalled => {
                formatter.write_str("policy coverage contract is already installed")
            }
        }
    }
}

impl std::error::Error for PolicyError {}

/// 一组 route 授权策略(可校验、可决策)。
#[derive(Debug, Clone, Default)]
pub struct PolicySet {
    policies: HashMap<String, RoutePolicy>,
}

impl PolicySet {
    /// 业务作用：从策略列表构造完整策略集合，并在发布前拒绝空 route、空 scope 与重复 route。
    ///
    /// 参数说明：
    /// - `policies`：候选 route 策略全集；本函数取得所有权并建立稳定 route 索引。
    ///
    /// 返回：全部策略通过结构校验时返回可用于完整快照的 `PolicySet`；否则返回精确
    /// `PolicyError`，不会发布部分策略。
    pub fn build(policies: Vec<RoutePolicy>) -> Result<Self, PolicyError> {
        let mut map = HashMap::with_capacity(policies.len());
        for policy in policies {
            if policy.route_id.trim().is_empty() {
                return Err(PolicyError::EmptyRouteId);
            }
            if policy.required_scopes.is_empty() {
                return Err(PolicyError::EmptyRequiredScopes(policy.route_id.clone()));
            }
            if map.contains_key(&policy.route_id) {
                return Err(PolicyError::DuplicateRoute(policy.route_id.clone()));
            }
            map.insert(policy.route_id.clone(), policy);
        }
        Ok(Self { policies: map })
    }

    /// 业务作用：只对显式存在的 route policy 执行 scope 裁决，不处理未命中缺省。
    ///
    /// 参数说明：
    /// - `route_id`：待匹配的稳定 route 标识。
    /// - `principal`：已验签主体。
    ///
    /// 返回：route 命中策略时返回 scope 裁决；未命中时返回 `None`，调用方必须交由完整快照应用
    /// `UnmatchedRoutePolicy`，不得把空值自行解释为放行。
    pub fn decide_explicit_policy(
        &self,
        route_id: &str,
        principal: &Principal,
    ) -> Option<AuthzDecision> {
        let policy = self.policies.get(route_id)?;
        Some(match policy.mode {
            RequireMode::All => {
                let missing: BTreeSet<String> = policy
                    .required_scopes
                    .difference(&principal.scopes)
                    .cloned()
                    .collect();
                if missing.is_empty() {
                    AuthzDecision::Permit
                } else {
                    AuthzDecision::Deny(DenyReason::MissingRequiredScopes(missing))
                }
            }
            RequireMode::Any => {
                if policy
                    .required_scopes
                    .iter()
                    .any(|scope| principal.scopes.contains(scope))
                {
                    AuthzDecision::Permit
                } else {
                    AuthzDecision::Deny(DenyReason::NoMatchingScope)
                }
            }
        })
    }

    /// 业务作用：策略条数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前完整策略集合中的稳定 route 数量。
    pub fn len(&self) -> usize {
        self.policies.len()
    }

    /// 业务作用：是否无策略。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：策略集合不包含任何显式 route 时为 `true`。
    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }

    /// 业务作用：遍历全部策略 route_id，供启动期把策略与有效路由表对账(悬空策略阻止 Ready)。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无序的 route_id 迭代器。
    pub fn route_ids(&self) -> impl Iterator<Item = &str> {
        self.policies.keys().map(String::as_str)
    }

    /// 业务作用：判断完整 route_id 是否受当前快照保护。
    ///
    /// 参数说明：
    /// - `route_id`：接入层提供的稳定 route 模板标识。
    ///
    /// 返回：存在显式策略时为 `true`；未命中时为 `false`，调用方仍须应用同代未命中缺省。
    pub fn is_protected(&self, route_id: &str) -> bool {
        self.policies.contains_key(route_id)
    }
}

/// policy、未命中缺省与公开 route 豁免的同代快照。
struct PolicySnapshot {
    /// 已完成结构和覆盖校验的 route 策略。
    policies: Arc<PolicySet>,
    /// 与策略同代发布的未命中裁决。
    unmatched: UnmatchedRoutePolicy,
    /// 只在未命中显式策略时放行的 route；来源是同一覆盖合同中的非保护 route。
    unmatched_exempt: Arc<HashSet<String>>,
    /// 与策略及未命中裁决同代的覆盖账目；接入层安装路由合同前为空。
    coverage_audit: Option<PolicyCoverageAudit>,
    /// 启动期冻结的有效路由合同；与账目同帧发布，供后续候选发布前复验。
    coverage: Option<Arc<PolicyCoverage>>,
}

/// 一次请求冻结的完整 route 授权快照；策略、未命中缺省、公开豁免与 generation 来自同一发布代次。
#[derive(Clone)]
pub struct PolicyDecisionSnapshot {
    policies: Arc<PolicySet>,
    unmatched: UnmatchedRoutePolicy,
    unmatched_exempt: Arc<HashSet<String>>,
    generation: u64,
}

impl PolicyDecisionSnapshot {
    /// 业务作用：构造同代 route 授权快照，供没有 registry 的接入层使用稳定缺省。
    ///
    /// 参数说明：
    /// - `policies`：已完成结构校验的策略集合。
    /// - `unmatched`：route 未命中策略时的三态缺省。
    /// - `generation`：策略与缺省共同对应的发布代次。
    ///
    /// 返回：冻结且可跨异步边界共享的完整裁决快照。
    pub fn new(policies: Arc<PolicySet>, unmatched: UnmatchedRoutePolicy, generation: u64) -> Self {
        Self {
            policies,
            unmatched,
            unmatched_exempt: Arc::new(HashSet::new()),
            generation,
        }
    }

    /// 业务作用：在保留同代策略与 generation 的前提下替换接入层未命中缺省。
    ///
    /// 参数说明：
    /// - `unmatched`：接入层在覆盖合同安装前使用的稳定三态缺省。
    ///
    /// 返回：复用原策略 Arc 的完整快照；不会复制策略集合或改变 generation。
    pub fn with_unmatched(mut self, unmatched: UnmatchedRoutePolicy) -> Self {
        self.unmatched = unmatched;
        self
    }

    /// 业务作用：把接入层冻结的公开 route 与健康探针并入当前完整裁决快照。
    ///
    /// 参数说明：
    /// - `exempt`：仅在没有显式策略时豁免未命中缺省的稳定 route 集合。
    ///
    /// 返回：保留策略、缺省和 generation，仅替换同请求豁免事实的完整快照。
    pub fn with_unmatched_exemptions(mut self, exempt: Arc<HashSet<String>>) -> Self {
        self.unmatched_exempt = exempt;
        self
    }

    /// 业务作用：对 route 执行完整裁决，显式策略与未命中缺省不会走不同入口。
    ///
    /// 参数说明：
    /// - `route_id`：待裁决的稳定 route 标识。
    /// - `principal`：已验签主体。
    ///
    /// 返回：命中策略时返回 scope 裁决；没有显式策略且 route 在豁免集合中时放行；其余未命中
    /// route 按 Permit/Observe/Deny 返回稳定结果。
    pub fn decide(&self, route_id: &str, principal: &Principal) -> AuthzDecision {
        if let Some(decision) = self.policies.decide_explicit_policy(route_id, principal) {
            return decision;
        }
        if self.unmatched_exempt.contains(route_id) {
            return AuthzDecision::Permit;
        }
        match self.unmatched {
            UnmatchedRoutePolicy::Permit | UnmatchedRoutePolicy::Observe => AuthzDecision::Permit,
            UnmatchedRoutePolicy::Deny => AuthzDecision::Deny(DenyReason::UnmatchedRoute),
        }
    }

    /// 业务作用：判断 route 是否由当前快照明确豁免未命中缺省，供接入层避免产生错误观测。
    ///
    /// 参数说明：
    /// - `route_id`：待核对的稳定 route 标识。
    ///
    /// 返回：没有显式策略且 route 属于同代豁免集合时为 `true`；显式策略始终返回 `false`。
    pub fn is_unmatched_exempt(&self, route_id: &str) -> bool {
        !self.policies.is_protected(route_id) && self.unmatched_exempt.contains(route_id)
    }

    /// 业务作用：返回快照内的策略集合，供接入层判断是否需要记录 Observe 证据。
    ///
    /// 参数说明：无。
    ///
    /// 返回：与本快照未命中缺省和 generation 同代的只读策略集合。
    pub fn policies(&self) -> &PolicySet {
        &self.policies
    }

    /// 业务作用：返回快照内的未命中缺省，供接入层执行 Observe 观测副作用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：与策略集合和 generation 同代的三态缺省。
    pub fn unmatched(&self) -> UnmatchedRoutePolicy {
        self.unmatched
    }

    /// 业务作用：返回完整裁决快照的发布代次。
    ///
    /// 参数说明：无。
    ///
    /// 返回：策略与未命中缺省共同对应的 generation。
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// 启动期安装的有效路由覆盖合同；后续 reload 必须在发布前复验。
#[derive(PartialEq, Eq)]
struct PolicyCoverage {
    /// 应用可被策略引用的全部稳定 route ID。
    routes: BTreeSet<String>,
    /// 声明要求授权、在 Deny 下必须被策略覆盖的 route ID。
    protected_routes: BTreeSet<String>,
}

/// 一次授权覆盖复验的低基数账目。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyCoverageAudit {
    /// 要求授权且命中策略的 route 数。
    pub covered: u64,
    /// 要求授权但没有策略的 route 数。
    pub uncovered: u64,
}

impl PolicyCoverage {
    /// 业务作用：在候选发布前复验悬空策略与 fail-closed 覆盖面。
    ///
    /// 参数说明：
    /// - policies：已完成结构校验的候选策略。
    /// - unmatched：与候选同代发布的未命中裁决。
    ///
    /// 返回：满足当前路由合同时返回覆盖账目；否则返回完整、稳定排序的违规 route 集合。
    fn audit(
        &self,
        policies: &PolicySet,
        unmatched: UnmatchedRoutePolicy,
    ) -> Result<PolicyCoverageAudit, PolicyError> {
        let mut dangling = policies
            .route_ids()
            .filter(|route| !self.routes.contains(*route))
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if !dangling.is_empty() {
            dangling.sort_unstable();
            return Err(PolicyError::DanglingRoutes(dangling));
        }
        let mut uncovered = self
            .protected_routes
            .iter()
            .filter(|route| !policies.is_protected(route))
            .cloned()
            .collect::<Vec<_>>();
        uncovered.sort_unstable();
        if unmatched == UnmatchedRoutePolicy::Deny && !uncovered.is_empty() {
            return Err(PolicyError::UncoveredRoutes(uncovered));
        }
        let covered = self
            .protected_routes
            .iter()
            .filter(|route| policies.is_protected(route))
            .count();
        Ok(PolicyCoverageAudit {
            covered: u64::try_from(covered).unwrap_or(u64::MAX),
            uncovered: u64::try_from(uncovered.len()).unwrap_or(u64::MAX),
        })
    }
}

/// route 授权 policy 运行时注册表：策略、未命中缺省与 generation 原子发布，失败保留 last-good。
pub struct PolicyRegistry {
    current: ArcSwap<PolicySnapshot>,
    generation: AtomicU64,
    snapshot_gate: std::sync::RwLock<()>,
}

/// 对象级授权 provider 的稳定裁决。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectDecision {
    /// 允许当前主体对目标对象执行动作。
    Permit,
    /// 拒绝；业务响应不得暴露 owner、对象存在性或 provider 细节。
    Deny,
}

/// provider 内部失败的无细节标记；诊断应由 provider 自己写入脱敏日志。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectProviderError;

/// 传给对象授权 provider 的只读请求。
#[derive(Debug, Clone)]
pub struct ObjectAuthorizationRequest {
    /// 已验签身份。
    pub principal: Principal,
    /// 动作，如 `order:cancel`。
    pub action: String,
    /// 低基数对象类型，如 `order`。
    pub object_type: String,
    /// 对象标识；不得进入日志、指标或错误正文。
    pub object_id: String,
    /// route 授权层捕获的同一 policy generation。
    pub policy_generation: u64,
}

/// 对象级授权 provider。实现可以查询 DB owner/ACL 或远程 PDP。
#[async_trait::async_trait]
pub trait ObjectAuthorizer: Send + Sync {
    /// 业务作用：依据已冻结主体、动作、对象和策略代次裁决对象级访问；provider 不得泄露对象细节。
    ///
    /// 参数说明：
    /// - `request`：同一请求安全上下文构造的只读对象授权请求。
    ///
    /// 返回：明确确认授权时返回 `Permit`，业务拒绝返回 `Deny`；provider 内部失败只返回无细节错误，
    /// 调用方必须 fail-closed。
    async fn authorize(
        &self,
        request: &ObjectAuthorizationRequest,
    ) -> Result<ObjectDecision, ObjectProviderError>;
}

/// service helper 的 fail-closed 结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectAuthorizationError {
    /// provider 明确拒绝。
    Denied,
    /// 未配置对象授权 provider。
    ProviderUnavailable,
    /// provider 调用失败。
    ProviderFailed,
    /// provider 超过固定预算。
    TimedOut,
}

/// 单请求安全快照。route 层只加载一次 registry generation；对象 helper 不再读取全局 registry。
#[derive(Clone)]
pub struct RequestSecurityContext {
    principal: Principal,
    route_snapshot: PolicyDecisionSnapshot,
    object_authorizer: Option<Arc<dyn ObjectAuthorizer>>,
    object_timeout: Duration,
}

impl RequestSecurityContext {
    /// 业务作用：由 Web 授权边界创建同代请求快照。
    ///
    /// 参数说明：
    /// - `principal`：本请求已经验签的主体。
    /// - `route_snapshot`：策略、未命中缺省与 generation 的完整同代快照。
    /// - `object_authorizer`：可选对象级授权 provider。
    /// - `object_timeout`：对象级授权调用预算，超过硬上限时自动收敛。
    ///
    /// 返回：跨异步边界保持同一授权代次的请求安全上下文。
    pub fn new(
        principal: Principal,
        route_snapshot: PolicyDecisionSnapshot,
        object_authorizer: Option<Arc<dyn ObjectAuthorizer>>,
        object_timeout: Duration,
    ) -> Self {
        Self {
            principal,
            route_snapshot,
            object_authorizer,
            object_timeout: object_timeout.min(MAX_OBJECT_AUTHORIZATION_TIMEOUT),
        }
    }

    /// 业务作用：已验签主体。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：请求边界冻结的只读 Principal；其身份字段与本上下文的策略快照同请求使用。
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// 业务作用：本请求冻结的 policy generation。
    ///
    /// 参数说明：无。
    ///
    /// 返回：路由策略与未命中缺省共同对应的发布代次。
    pub fn policy_generation(&self) -> u64 {
        self.route_snapshot.generation()
    }

    /// 业务作用：用本请求冻结的完整 route 快照做决策，包含未命中三态缺省。
    ///
    /// 参数说明：
    /// - `route_id`：待裁决的稳定 route 标识。
    ///
    /// 返回：显式策略或同代未命中缺省共同决定的最终授权结果。
    pub fn decide_route(&self, route_id: &str) -> AuthzDecision {
        self.route_snapshot.decide(route_id, &self.principal)
    }

    /// 业务作用：暴露本请求冻结的策略快照，供授权边界在同代快照上执行覆盖判断与未命中缺省裁决。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：请求创建时冻结的策略集合；与 `decide_route` 使用同一份快照，不存在代次错配。
    pub fn policy_set(&self) -> &PolicySet {
        self.route_snapshot.policies()
    }

    /// 业务作用：对业务对象执行 fail-closed 授权；错误和超时均不降级为 permit。
    ///
    /// 参数说明：
    /// - `action`：当前主体请求执行的稳定动作。
    /// - `object_type`：低基数业务对象类型。
    /// - `object_id`：受授权决策约束的对象标识。
    ///
    /// 返回：provider 明确允许时成功；拒绝、未配置、调用失败或超时均返回可区分的封闭错误。
    pub async fn authorize_object(
        &self,
        action: impl Into<String>,
        object_type: impl Into<String>,
        object_id: impl Into<String>,
    ) -> Result<(), ObjectAuthorizationError> {
        let provider = self
            .object_authorizer
            .as_ref()
            .ok_or(ObjectAuthorizationError::ProviderUnavailable)?;
        let request = ObjectAuthorizationRequest {
            principal: self.principal.clone(),
            action: action.into(),
            object_type: object_type.into(),
            object_id: object_id.into(),
            policy_generation: self.route_snapshot.generation(),
        };
        match tokio::time::timeout(self.object_timeout, provider.authorize(&request)).await {
            Ok(Ok(ObjectDecision::Permit)) => Ok(()),
            Ok(Ok(ObjectDecision::Deny)) => Err(ObjectAuthorizationError::Denied),
            Ok(Err(_)) => Err(ObjectAuthorizationError::ProviderFailed),
            Err(_) => Err(ObjectAuthorizationError::TimedOut),
        }
    }
}

impl PolicyRegistry {
    /// 业务作用：用初始 policy set 建注册表,generation=1。
    ///
    /// 参数说明：
    /// - `initial`：已完成结构校验的初始路由策略集。
    ///
    /// 返回：未命中缺省为 Permit、尚未安装路由覆盖合同的第一代注册表。
    pub fn new(initial: PolicySet) -> Self {
        Self {
            current: ArcSwap::from_pointee(PolicySnapshot {
                policies: Arc::new(initial),
                unmatched: UnmatchedRoutePolicy::Permit,
                unmatched_exempt: Arc::new(HashSet::new()),
                coverage_audit: None,
                coverage: None,
            }),
            generation: AtomicU64::new(1),
            snapshot_gate: std::sync::RwLock::new(()),
        }
    }

    /// 业务作用：从策略列表校验并建注册表。
    ///
    /// 参数说明：
    /// - `policies`：第一代候选 route 策略全集。
    ///
    /// 返回：结构校验通过时返回 generation 为 1、未命中缺省为 Permit 的注册表；否则返回精确
    /// `PolicyError`，不创建可发布的部分状态。
    pub fn from_policies(policies: Vec<RoutePolicy>) -> Result<Self, PolicyError> {
        Ok(Self::new(PolicySet::build(policies)?))
    }

    /// 业务作用：热更新:**先校验候选**(此处由调用方以 [`PolicySet::build`] 保证)通过则原子发布并 generation++;
    /// 失败保留 last-good、generation 不变。此签名收 `Result<PolicySet>` 以显式表达"校验失败即保 last-good"。
    ///
    /// 参数说明：
    /// - `candidate`：已构建的候选策略，结构失败由 Err 原样传入。
    ///
    /// 返回：覆盖合同满足时原子发布并返回新代次；任一校验失败时保留 last-good 与原代次。
    pub fn reload(&self, candidate: Result<PolicySet, PolicyError>) -> Result<u64, PolicyError> {
        let policy_set = candidate?;
        let _gate = self
            .snapshot_gate
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = self.current.load_full();
        self.publish_locked(policy_set, current.unmatched)?;
        Ok(self.generation.fetch_add(1, Ordering::AcqRel) + 1)
    }

    /// 业务作用：把候选策略与未命中缺省作为同一授权 generation 校验并发布。
    ///
    /// 参数说明：
    /// - candidate：已完成结构构建的候选策略。
    /// - unmatched：与候选同时生效的三态未命中裁决。
    ///
    /// 返回：结构和覆盖均通过时返回新 generation；失败时策略、缺省与 generation 全部不变。
    pub fn reload_with_unmatched(
        &self,
        candidate: Result<PolicySet, PolicyError>,
        unmatched: UnmatchedRoutePolicy,
    ) -> Result<u64, PolicyError> {
        let policy_set = candidate?;
        let _gate = self
            .snapshot_gate
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.publish_locked(policy_set, unmatched)?;
        Ok(self.generation.fetch_add(1, Ordering::AcqRel) + 1)
    }

    /// 业务作用：在 Web Ready 期安装冻结路由合同，并把初始未命中缺省并入当前 generation。
    ///
    /// 参数说明：
    /// - routes：静态与显式动态合同组成的全部有效 route ID。
    /// - protected_routes：声明要求授权、Deny 下必须被策略覆盖的 route ID。
    /// - unmatched：YAML 与 UserHook 归并后的初始未命中裁决。
    ///
    /// 返回：当前策略满足合同时返回覆盖账目并启用后续 reload 复验；失败时不安装合同、不改变快照。
    pub fn install_coverage_contract(
        &self,
        routes: impl IntoIterator<Item = String>,
        protected_routes: impl IntoIterator<Item = String>,
        unmatched: UnmatchedRoutePolicy,
    ) -> Result<PolicyCoverageAudit, PolicyError> {
        let contract = PolicyCoverage {
            routes: routes.into_iter().collect(),
            protected_routes: protected_routes.into_iter().collect(),
        };
        let _gate = self
            .snapshot_gate
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = self.current.load_full();
        let audit = contract.audit(&current.policies, unmatched)?;
        let unmatched_exempt = Arc::new(
            contract
                .routes
                .difference(&contract.protected_routes)
                .cloned()
                .collect(),
        );
        if current
            .coverage
            .as_deref()
            .is_some_and(|installed| installed == &contract)
            && current.unmatched == unmatched
        {
            return Ok(audit);
        }
        if current.coverage.is_some() {
            return Err(PolicyError::CoverageAlreadyInstalled);
        }
        // 当前 generation 尚未对请求开放，可在同一快照锁内合入启动配置而不虚增代次。
        self.current.store(Arc::new(PolicySnapshot {
            policies: Arc::clone(&current.policies),
            unmatched,
            unmatched_exempt,
            coverage_audit: Some(audit),
            coverage: Some(Arc::new(contract)),
        }));
        Ok(audit)
    }

    /// 业务作用：在已持有快照写门禁时完成覆盖复验与整帧发布。
    ///
    /// 参数说明：
    /// - policy_set：已完成结构构建的候选策略。
    /// - unmatched：与候选同代的未命中裁决。
    ///
    /// 返回：覆盖合同满足时替换完整快照；失败时不产生部分发布。
    fn publish_locked(
        &self,
        policy_set: PolicySet,
        unmatched: UnmatchedRoutePolicy,
    ) -> Result<(), PolicyError> {
        let current = self.current.load_full();
        let audit = if let Some(coverage) = current.coverage.as_deref() {
            Some(coverage.audit(&policy_set, unmatched)?)
        } else {
            None
        };
        self.current.store(Arc::new(PolicySnapshot {
            policies: Arc::new(policy_set),
            unmatched,
            unmatched_exempt: Arc::clone(&current.unmatched_exempt),
            coverage_audit: audit,
            coverage: current.coverage.clone(),
        }));
        Ok(())
    }

    /// 业务作用：当前 policy set(原子快照)。
    ///
    /// 参数说明：无。
    ///
    /// 返回：当前发布快照的只读策略集；不包含未命中缺省。
    pub fn current(&self) -> Arc<PolicySet> {
        Arc::clone(&self.current.load_full().policies)
    }

    /// 业务作用：当前 generation(每次成功 reload +1)。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：最后一次成功发布的单调代次；需要同时裁决策略时必须改用完整请求快照。
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// 业务作用：原子读取 policy set 与 generation，供一次请求冻结同代安全快照。
    ///
    /// 参数说明：无。
    ///
    /// 返回：同一读锁下取得的策略集与发布代次；完整路由裁决应使用 `request_snapshot`。
    pub fn snapshot(&self) -> (Arc<PolicySet>, u64) {
        let _gate = self
            .snapshot_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let snapshot = self.current.load_full();
        (
            Arc::clone(&snapshot.policies),
            self.generation.load(Ordering::Acquire),
        )
    }

    /// 业务作用：原子读取策略、未命中缺省与 generation，供请求冻结同代授权裁决。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：三项均来自同一快照门禁，不会把新策略与旧缺省交叉组合。
    pub fn snapshot_with_unmatched(&self) -> (Arc<PolicySet>, UnmatchedRoutePolicy, u64) {
        let _gate = self
            .snapshot_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let snapshot = self.current.load_full();
        (
            Arc::clone(&snapshot.policies),
            snapshot.unmatched,
            self.generation.load(Ordering::Acquire),
        )
    }

    /// 业务作用：判断接入层是否已安装冻结路由覆盖合同。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已安装时为真；未安装表示低层调用方仍由其授权层状态决定未命中裁决。
    pub fn coverage_contract_installed(&self) -> bool {
        self.current.load().coverage.is_some()
    }

    /// 业务作用：一次性读取请求裁决所需的策略、未命中缺省、公开豁免、覆盖状态与 generation。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：完整裁决快照与覆盖安装状态来自同一读取门禁，不会交叉组合代次或公开性事实。
    pub fn request_snapshot(&self) -> (PolicyDecisionSnapshot, bool) {
        let _gate = self
            .snapshot_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let snapshot = self.current.load_full();
        (
            PolicyDecisionSnapshot::new(
                Arc::clone(&snapshot.policies),
                snapshot.unmatched,
                self.generation.load(Ordering::Acquire),
            )
            .with_unmatched_exemptions(Arc::clone(&snapshot.unmatched_exempt)),
            snapshot.coverage.is_some(),
        )
    }

    /// 业务作用：读取最近一次成功发布快照对应的覆盖账目。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：接入层安装覆盖合同后返回已覆盖与未覆盖数量；安装前返回空。
    pub fn coverage_audit(&self) -> Option<PolicyCoverageAudit> {
        self.current.load().coverage_audit
    }

    /// 业务作用：使用当前同代完整快照执行 route 裁决，包含未命中三态缺省。
    ///
    /// 参数说明：
    /// - `route_id`：待裁决的稳定 route 标识。
    /// - `principal`：已验签主体。
    ///
    /// 返回：显式策略或未命中缺省共同决定的最终授权结果。
    pub fn decide(&self, route_id: &str, principal: &Principal) -> AuthzDecision {
        let (snapshot, _) = self.request_snapshot();
        snapshot.decide(route_id, principal)
    }
}
