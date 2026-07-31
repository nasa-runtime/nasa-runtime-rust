//! NASA route 级授权核心。
//!
//! 与 authentication 分离:auth 确认"你是谁",authz 判定"你能否访问这条 route"。策略以 scope 集合
//! 表达每条 route 的要求([`RoutePolicy`]),对已认证主体([`Principal`])的 scope 做 All/Any 判定。
//!
//! [`PolicyRegistry`] 用 `ArcSwap` 提供**校验后原子发布 + 失败保留 last-good**,generation 随
//! 发布单调推进——真实系统里其 generation 应与完整安全快照一起可见。
//!
//! 本 crate **不依赖 `napp`**;策略来源(远端 provider / 配置)由上层适配。

#![forbid(unsafe_code)]

use std::collections::{BTreeSet, HashMap};
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
    /// 用 scope 列表构造。
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

    /// 返回可用于安全命名空间的认证身份：优先 `sub`，否则 `client_id`。
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
}

/// policy 校验错误(结构问题)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyError {
    /// 某策略 route_id 为空。
    EmptyRouteId,
    /// 某策略未声明任何 required scope(无意义)。
    EmptyRequiredScopes(String),
    /// 出现重复 route_id。
    DuplicateRoute(String),
}

/// 一组 route 授权策略(可校验、可决策)。
#[derive(Debug, Clone, Default)]
pub struct PolicySet {
    policies: HashMap<String, RoutePolicy>,
}

impl PolicySet {
    /// 从策略列表构造并**校验**:route_id 非空、required_scopes 非空、无重复 route。
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

    /// 对某 route + 主体决策:无策略的 route 放行(authz 不适用),有策略则按 mode 判 scope。
    pub fn decide(&self, route_id: &str, principal: &Principal) -> AuthzDecision {
        let Some(policy) = self.policies.get(route_id) else {
            return AuthzDecision::Permit; // 未受 authz 保护的 route
        };
        match policy.mode {
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
        }
    }

    /// 策略条数。
    pub fn len(&self) -> usize {
        self.policies.len()
    }

    /// 是否无策略。
    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }

    /// 判断完整 route_id 是否受当前快照保护。
    pub fn is_protected(&self, route_id: &str) -> bool {
        self.policies.contains_key(route_id)
    }
}

/// route 授权 policy 运行时注册表:校验后原子发布 + 失败保留 last-good。
pub struct PolicyRegistry {
    current: ArcSwap<PolicySet>,
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
    /// 返回 permit/deny；内部错误只返回无细节标记。
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
    policy_set: Arc<PolicySet>,
    policy_generation: u64,
    object_authorizer: Option<Arc<dyn ObjectAuthorizer>>,
    object_timeout: Duration,
}

impl RequestSecurityContext {
    /// 由 Web 授权边界创建同代请求快照。
    pub fn new(
        principal: Principal,
        policy_set: Arc<PolicySet>,
        policy_generation: u64,
        object_authorizer: Option<Arc<dyn ObjectAuthorizer>>,
        object_timeout: Duration,
    ) -> Self {
        Self {
            principal,
            policy_set,
            policy_generation,
            object_authorizer,
            object_timeout: object_timeout.min(MAX_OBJECT_AUTHORIZATION_TIMEOUT),
        }
    }

    /// 已验签主体。
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// 本请求冻结的 policy generation。
    pub fn policy_generation(&self) -> u64 {
        self.policy_generation
    }

    /// 用本请求冻结的 route policy 快照做决策。
    pub fn decide_route(&self, route_id: &str) -> AuthzDecision {
        self.policy_set.decide(route_id, &self.principal)
    }

    /// 对业务对象执行 fail-closed 授权；错误和超时均不降级为 permit。
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
            policy_generation: self.policy_generation,
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
    /// 用初始 policy set 建注册表,generation=1。
    pub fn new(initial: PolicySet) -> Self {
        Self {
            current: ArcSwap::from_pointee(initial),
            generation: AtomicU64::new(1),
            snapshot_gate: std::sync::RwLock::new(()),
        }
    }

    /// 从策略列表校验并建注册表。
    pub fn from_policies(policies: Vec<RoutePolicy>) -> Result<Self, PolicyError> {
        Ok(Self::new(PolicySet::build(policies)?))
    }

    /// 热更新:**先校验候选**(此处由调用方以 [`PolicySet::build`] 保证)通过则原子发布并 generation++;
    /// 失败保留 last-good、generation 不变。此签名收 `Result<PolicySet>` 以显式表达"校验失败即保 last-good"。
    pub fn reload(&self, candidate: Result<PolicySet, PolicyError>) -> Result<u64, PolicyError> {
        let policy_set = candidate?; // 校验失败:直接返回,current/generation 不变
        let _gate = self
            .snapshot_gate
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.current.store(Arc::new(policy_set));
        Ok(self.generation.fetch_add(1, Ordering::AcqRel) + 1)
    }

    /// 当前 policy set(原子快照)。
    pub fn current(&self) -> Arc<PolicySet> {
        self.current.load_full()
    }

    /// 当前 generation(每次成功 reload +1)。
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// 原子读取 policy set 与 generation，供一次请求冻结同代安全快照。
    pub fn snapshot(&self) -> (Arc<PolicySet>, u64) {
        let _gate = self
            .snapshot_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            self.current.load_full(),
            self.generation.load(Ordering::Acquire),
        )
    }

    /// 便捷:用当前 policy set 决策。
    pub fn decide(&self, route_id: &str, principal: &Principal) -> AuthzDecision {
        self.current().decide(route_id, principal)
    }
}
