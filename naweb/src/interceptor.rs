//! Mapping 路由拦截器的声明、作用域合并和确定性执行计划。

use std::collections::{HashMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
#[cfg(feature = "auth")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
#[cfg(feature = "auth")]
use std::time::Instant;

use axum::extract::{FromRequestParts, Request, State};
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use axum::routing::MethodRouter;

use crate::{MappingBuildError, MappingRuntime, RoutePolicy};

/// 拦截器相对安全端点流水线的固定位置。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InterceptorStage {
    /// 位于整个端点最外层，看到原始请求和最终响应。
    Edge,
    /// 位于身份上下文门禁之外、请求解密之前。
    Auth,
    /// 位于请求解密之后、响应加密之前。
    Plaintext,
}

impl InterceptorStage {
    /// 业务作用：返回排序使用的固定阶段序号。
    const fn rank(self) -> u8 {
        match self {
            Self::Edge => 0,
            Self::Auth => 1,
            Self::Plaintext => 2,
        }
    }
}

/// `#[interceptor]` 生成的不可变声明信息。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InterceptorDescriptor {
    /// 稳定拦截器 ID；同一路由的 effective plan 内必须唯一，推荐应用内保持唯一。
    pub id: &'static str,
    /// 固定执行阶段。
    pub stage: InterceptorStage,
    /// 同阶段无依赖关系时使用的稳定顺序，数值越小越先执行。
    pub order: i32,
    /// 必须排在这些 ID 之前。
    pub before: &'static [&'static str],
    /// 必须排在这些 ID 之后。
    pub after: &'static [&'static str],
    /// 完整业务函数身份，仅用于诊断和审计。
    pub handler: &'static str,
    /// 源码文件，仅用于无敏感信息的启动诊断。
    pub source_file: &'static str,
    /// 源码行号。
    pub source_line: u32,
    /// 该 auth 拦截器是否调用快照中的通用 `AuthRuntime`。
    ///
    /// 只有 `kind = "auth"` 可以开启。声明后 provider/condition 会纳入启动审计和
    /// last-good 热更新审计，不会因 endpoint 已挂 auth interceptor 而被跳过。
    pub auth_runtime: bool,
}

/// 同名 marker 实现的静态 definition 合同。
pub trait InterceptorDefinition {
    /// 宏从声明参数和函数位置生成的完整描述符。
    const DESCRIPTOR: InterceptorDescriptor;
}

/// 当前 binding 在 effective plan 中来自哪个声明范围。
///
/// 该值由 mapping 编排器生成，不能由请求 Header 伪造；业务可用于低基数日志，但不应把
/// 动态 URL 或凭证拼进指标标签。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InterceptorScope {
    /// 通过 [`MappingPlan::global`] 注册，覆盖全部符合安全策略的 mapping 路由。
    Global,
    /// 通过 [`MappingPlan::scope`] 注册；值是启动期声明的静态路径前缀。
    Router(Arc<str>),
    /// 通过路由属性 `interceptors(...)` 直接绑定到单个端点。
    Endpoint,
}

/// 当前拦截器调用可读取的路由与运行时上下文。
#[derive(Clone)]
pub struct InterceptorContext {
    policy: RoutePolicy,
    interceptor: InterceptorDescriptor,
    scope: InterceptorScope,
    effective_order: usize,
    runtime: Arc<MappingRuntime>,
}

impl InterceptorContext {
    /// 业务作用：返回编译期稳定路由 ID。
    pub fn route_id(&self) -> &'static str {
        self.policy.route_id
    }

    /// 业务作用：返回编译期 HTTP 方法。
    pub fn method(&self) -> &'static str {
        self.policy.method
    }

    /// 业务作用：返回不含动态参数值的路径模板。
    pub fn path_template(&self) -> &'static str {
        self.policy.path_template
    }

    /// 业务作用：返回当前调用对应的拦截器 ID。
    pub fn interceptor_id(&self) -> &'static str {
        self.interceptor.id
    }

    /// 业务作用：返回当前调用的固定阶段。
    pub fn stage(&self) -> InterceptorStage {
        self.interceptor.stage
    }

    /// 业务作用：返回当前路由完整的只读静态安全合同。
    ///
    /// 这里不包含密钥、Token 或运行期配置值，可安全用于分支选择和低基数审计。
    pub fn route_policy(&self) -> RoutePolicy {
        self.policy
    }

    /// 业务作用：返回当前 binding 的有效声明范围。
    pub fn scope(&self) -> &InterceptorScope {
        &self.scope
    }

    /// 业务作用：返回当前 stage 内、按入站方向计算的零基有效顺序。
    pub fn effective_order(&self) -> usize {
        self.effective_order
    }

    /// 业务作用：返回本次读取时的 MappingRuntime 代次。
    pub fn runtime_generation(&self) -> u64 {
        self.runtime.snapshot().generation
    }

    /// 业务作用：在 auth 阶段执行当前快照的 provider/condition 并把结果交给 endpoint gate。
    ///
    /// 这是公共的“运行时认证胶水”：应用仍自己实现 Token Header、Redis 会话、
    /// 禁用位和 address-read provider，mapping 只负责 auth-before-decrypt 顺序、错误
    /// profile、短路响应和 [`crate::auth::AuthContext`] 传递。
    ///
    /// # 参数
    ///
    /// - `request`: 尚未解密或消费 body 的原始请求。
    /// - `next`: endpoint AuthContext gate 与后续安全流水线。
    ///
    /// # 返回
    ///
    /// 认证成功/可选匿名时继续，缺失、无效、后端故障和不合法阶段均受控短路。
    #[cfg(feature = "auth")]
    pub async fn run_auth_runtime(&self, mut request: Request, next: Next) -> Response {
        if self.interceptor.stage != InterceptorStage::Auth || !self.interceptor.auth_runtime {
            let error = crate::auth::AuthError::new(
                crate::auth::AuthErrorKind::Policy,
                "interceptor-auth-runtime-contract",
            );
            return crate::endpoint::auth_error_response(&error, self.policy.error_profile);
        }

        // provider 与 endpoint 必须固定读取同一代快照；否则配置中心恰好热更新时，
        // 旧代身份可能错配新代密钥环。私有 extension 不能由 Header 伪造。
        let snapshot = request
            .extensions()
            .get::<AuthRuntimeRequestSnapshot>()
            .map(AuthRuntimeRequestSnapshot::snapshot)
            .unwrap_or_else(|| self.runtime.snapshot());
        request
            .extensions_mut()
            .insert(AuthRuntimeRequestSnapshot(snapshot.clone()));
        let Some(runtime) = snapshot.auth.as_ref() else {
            let error = crate::auth::AuthError::new(
                crate::auth::AuthErrorKind::Policy,
                "auth-runtime-missing",
            );
            return crate::endpoint::auth_error_response(&error, self.policy.error_profile);
        };
        let input = crate::auth::AuthInput {
            method: self.policy.method,
            route_id: self.policy.route_id,
            path: request.uri().path(),
            headers: request.headers(),
            extensions: request.extensions(),
        };
        match runtime.evaluate(&self.policy, input).await {
            Ok(crate::auth::AuthDecision::Authenticated(context)) => {
                request.extensions_mut().insert(context);
                next.run(request).await
            }
            Ok(crate::auth::AuthDecision::Anonymous) => {
                if self.policy.auth == crate::AuthRequirement::Required {
                    // 只有已审计 AuthCondition 能让 required 在本次请求上得到 Anonymous；
                    // endpoint 仅信任这个私有标记，不把匿名伪造成 AuthContext。
                    request.extensions_mut().insert(AuthRuntimeAnonymous {
                        route_id: self.policy.route_id,
                        generation: snapshot.generation,
                    });
                }
                next.run(request).await
            }
            Ok(crate::auth::AuthDecision::ShortCircuit(short_circuit)) => {
                if short_circuit.crypto == crate::auth::ShortCircuitCrypto::ApplyRouteResponsePolicy
                    && !matches!(
                        self.policy.crypto_response,
                        crate::CryptoRequirement::Disabled
                    )
                {
                    // auth 在 request decrypt 之前，此时尚未建立可信响应加密上下文。
                    let error = crate::auth::AuthError::new(
                        crate::auth::AuthErrorKind::Policy,
                        "auth-short-circuit-crypto-unavailable",
                    );
                    return crate::endpoint::auth_error_response(&error, self.policy.error_profile);
                }
                crate::endpoint::ensure_no_store(short_circuit.response)
            }
            Err(error) => crate::endpoint::auth_error_response(&error, self.policy.error_profile),
        }
    }
}

/// auth runtime 与 endpoint composer 共享的请求固定快照。
#[cfg(feature = "auth")]
#[derive(Clone)]
pub(crate) struct AuthRuntimeRequestSnapshot(Arc<crate::MappingRuntimeSnapshot>);

#[cfg(feature = "auth")]
impl AuthRuntimeRequestSnapshot {
    /// 业务作用：返回本请求已固定的不可变快照。
    pub(crate) fn snapshot(&self) -> Arc<crate::MappingRuntimeSnapshot> {
        self.0.clone()
    }
}

/// 已审计 AuthCondition 把 required 路由收窄为当前请求可匿名的私有凭证。
#[cfg(feature = "auth")]
#[derive(Clone, Copy)]
pub(crate) struct AuthRuntimeAnonymous {
    route_id: &'static str,
    generation: u64,
}

#[cfg(feature = "auth")]
impl AuthRuntimeAnonymous {
    /// 业务作用：标记只能用于生成它的路由和运行时代次。
    pub(crate) fn matches(self, route_id: &str, generation: u64) -> bool {
        self.route_id == route_id && self.generation == generation
    }
}

impl<S> FromRequestParts<S> for InterceptorContext
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    /// 业务作用：从请求扩展读取框架预先写入的上下文；缺失表示层序或装配错误并返回 500。
    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts.extensions.get::<Self>().cloned().ok_or((
            StatusCode::INTERNAL_SERVER_ERROR,
            "interceptor context is unavailable",
        ))
    }
}

type Mount<S> = Arc<dyn Fn(MethodRouter<S>, &S) -> MethodRouter<S> + Send + Sync + 'static>;

/// 一个已经单态化到 Router 根状态的拦截器绑定。
pub struct InterceptorBinding<S> {
    descriptor: InterceptorDescriptor,
    mount: Mount<S>,
    route_selector: Option<Arc<RouteSelector>>,
}

/// 启动期静态 route selector 及其一次求值缓存。
///
/// `try_register_all` 会先审计全部路由、再逐条构造 MethodRouter；缓存保证即使业务闭包错误地
/// 使用内部可变状态，同一路由在两个阶段也只能得到同一个决定，不会出现“审计选中、挂载跳过”。
struct RouteSelector {
    evaluate: Arc<dyn Fn(RoutePolicy) -> bool + Send + Sync + 'static>,
    decisions: Mutex<HashMap<&'static str, bool>>,
}

impl RouteSelector {
    /// 业务作用：对静态路由最多求值一次，并把 panic 收敛成监听前构建错误。
    fn matches(&self, policy: RoutePolicy) -> Result<bool, MappingBuildError> {
        let mut decisions = self
            .decisions
            .lock()
            .map_err(|_| MappingBuildError::new("interceptor route selector 缓存不可用"))?;
        if let Some(decision) = decisions.get(policy.route_id) {
            return Ok(*decision);
        }
        let decision = match catch_unwind(AssertUnwindSafe(|| (self.evaluate)(policy))) {
            Ok(decision) => decision,
            Err(payload) => {
                // panic payload 是业务可构造的 `Any`；除禁止格式化外也不能直接 Drop，否则恶意
                // payload 的析构再次 panic 会越过 MappingBuildError，破坏监听前的可失败启动合同。
                std::mem::forget(payload);
                return Err(MappingBuildError::new(format!(
                    "路由 {} 的 interceptor route selector 执行失败",
                    policy.route_id
                )));
            }
        };
        decisions.insert(policy.route_id, decision);
        Ok(decision)
    }
}

impl<S> Clone for InterceptorBinding<S> {
    /// 业务作用：克隆只读 descriptor、挂载闭包与 selector 缓存，共享同一静态求值结果。
    fn clone(&self) -> Self {
        Self {
            descriptor: self.descriptor,
            mount: self.mount.clone(),
            route_selector: self.route_selector.clone(),
        }
    }
}

impl<S> InterceptorBinding<S>
where
    S: Clone + Send + Sync + 'static,
{
    /// 业务作用：从 definition 和 Axum layer 单态化函数建立高级 binding。
    ///
    /// `#[interceptor]` 生成的 `binding`/`binding_with` 最终也走这里；需要自定义 Tower
    /// Layer、流式 Body 或专用 backpressure 的开源使用者可以手写 mount，但仍必须提供静态
    /// descriptor，并接受同一 scope 排序、auth-before-decrypt、启动审计和 panic 收敛。
    pub fn new<F>(descriptor: InterceptorDescriptor, mount: F) -> Self
    where
        F: Fn(MethodRouter<S>, &S) -> MethodRouter<S> + Send + Sync + 'static,
    {
        Self {
            descriptor,
            mount: Arc::new(mount),
            route_selector: None,
        }
    }

    /// 业务作用：添加只读取静态 [`RoutePolicy`] 的启动期 selector。
    ///
    /// selector 对每条静态路由最多执行一次，决定会被审计与挂载阶段共享；它不能读取请求数据，
    /// 也不能用于让 public 路由执行 auth-stage binding。需要按 Header 等请求数据判断时，应在
    /// 业务拦截器内部做 fail-closed 判断，后续动态 condition API 也必须进入同一阶段模型。
    pub fn when_route<F>(mut self, selector: F) -> Self
    where
        F: Fn(RoutePolicy) -> bool + Send + Sync + 'static,
    {
        self.route_selector = Some(Arc::new(RouteSelector {
            evaluate: Arc::new(selector),
            decisions: Mutex::new(HashMap::new()),
        }));
        self
    }

    /// 业务作用：返回不可变描述符。
    pub fn descriptor(&self) -> InterceptorDescriptor {
        self.descriptor
    }

    /// 业务作用：判断当前 binding 是否参与指定路由的 effective plan。
    fn matches_route(&self, policy: RoutePolicy) -> Result<bool, MappingBuildError> {
        self.route_selector
            .as_ref()
            .map_or(Ok(true), |selector| selector.matches(policy))
    }

    /// 业务作用：把业务 layer 和其调用上下文一起挂到当前 MethodRouter。
    pub(crate) fn apply(
        &self,
        route: MethodRouter<S>,
        state: &S,
        policy: RoutePolicy,
        runtime: Arc<MappingRuntime>,
        scope: InterceptorScope,
        effective_order: usize,
    ) -> Result<MethodRouter<S>, MappingBuildError> {
        // 高级 binding 可以装入任意业务 Layer；构造 panic 只转换成稳定 ID 的启动错误，
        // payload 不格式化，避免把 State 内的地址或 secret 带入诊断。
        let route = match catch_unwind(AssertUnwindSafe(|| (self.mount)(route, state))) {
            Ok(route) => route,
            Err(payload) => {
                // 与 selector 一样，不格式化也不析构不可信 payload；启动错误只保留静态路由与 ID。
                std::mem::forget(payload);
                return Err(MappingBuildError::new(format!(
                    "路由 {} 的 interceptor {} layer 构造失败",
                    policy.route_id, self.descriptor.id
                )));
            }
        };
        let context = InterceptorContext {
            policy,
            interceptor: self.descriptor,
            scope,
            effective_order,
            runtime,
        };
        Ok(route.layer(axum::middleware::from_fn_with_state(
            context,
            publish_interceptor_context,
        )))
    }
}

/// 业务作用：把当前调用的类型化上下文写入 request extensions 后进入业务拦截器。
async fn publish_interceptor_context(
    State(context): State<InterceptorContext>,
    mut request: Request,
    next: Next,
) -> Response {
    #[cfg(feature = "auth")]
    let auth_metric = if context.interceptor.stage == InterceptorStage::Auth {
        // 多个 auth interceptor 共享同一个 marker。只有创建 marker 的最外层 binding 负责
        // 补记“业务在 endpoint gate 之前短路”的一次路由指标，避免 N 个 auth 层重复计数。
        if let Some(existing) = request.extensions().get::<AuthInterceptorMetricMarker>() {
            Some((existing.clone(), false, Instant::now()))
        } else {
            let marker =
                AuthInterceptorMetricMarker::for_route(context.runtime.clone(), context.policy);
            request.extensions_mut().insert(marker.clone());
            Some((marker, true, Instant::now()))
        }
    } else {
        None
    };
    request.extensions_mut().insert(context);
    let response = next.run(request).await;
    #[cfg(feature = "auth")]
    if let Some((marker, owns_metric, started)) = auth_metric {
        if owns_metric && !marker.reached_gate() {
            let status = response.status();
            let runtime = marker.runtime();
            let metrics = runtime.metrics().route(marker.policy());
            let auth_outcome = if status.is_success() || status.is_redirection() {
                crate::AuthMetricOutcome::ShortCircuit
            } else {
                crate::AuthMetricOutcome::Rejected
            };
            metrics.record_auth(auth_outcome, started.elapsed());
            metrics.record_request(
                crate::SecurityMetricOutcome::from_status(status),
                started.elapsed(),
            );
        }
    }
    response
}

/// auth binding 与 endpoint gate 之间共享的低成本观测标记。
///
/// 标记不保存 Header、身份或动态路径；它只回答“请求是否真正进入框架 gate”，用于区分业务
/// interceptor 的提前短路与 endpoint 已经正常记账的结果。
#[cfg(feature = "auth")]
#[derive(Clone)]
pub(crate) struct AuthInterceptorMetricMarker {
    reached_gate: Arc<AtomicBool>,
    runtime: Arc<MappingRuntime>,
    policy: RoutePolicy,
}

#[cfg(feature = "auth")]
impl AuthInterceptorMetricMarker {
    /// 业务作用：使用当前静态路由和共享运行时建立正式标记。
    fn for_route(runtime: Arc<MappingRuntime>, policy: RoutePolicy) -> Self {
        Self {
            reached_gate: Arc::new(AtomicBool::new(false)),
            runtime,
            policy,
        }
    }

    /// 业务作用：endpoint composer 进入 AuthContext gate 时调用，阻止外层重复记录。
    pub(crate) fn mark_reached_gate(&self) {
        self.reached_gate.store(true, Ordering::Release);
    }

    /// 业务作用：判断 endpoint 是否已经接管后续身份和总请求指标。
    fn reached_gate(&self) -> bool {
        self.reached_gate.load(Ordering::Acquire)
    }

    /// 业务作用：返回只读共享运行时。
    fn runtime(&self) -> &Arc<MappingRuntime> {
        &self.runtime
    }

    /// 业务作用：返回编译期静态路由合同。
    fn policy(&self) -> RoutePolicy {
        self.policy
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BindingOrigin {
    Global,
    Scope,
    Endpoint,
}

/// 一项尚未按依赖排序的拦截器及其来源范围、前缀和稳定插入序。
#[derive(Clone)]
struct PlannedBinding<S> {
    binding: InterceptorBinding<S>,
    origin: BindingOrigin,
    path_prefix: Option<Arc<str>>,
    insertion: usize,
}

/// 应用启动期构造、Ready 时封口的拦截器计划。
pub struct MappingPlan<S> {
    bindings: Vec<PlannedBinding<S>>,
    runtime: Option<Arc<MappingRuntime>>,
}

impl<S> Default for MappingPlan<S> {
    /// 业务作用：创建不含 binding 且尚未绑定运行时的开放计划。
    fn default() -> Self {
        Self {
            bindings: Vec::new(),
            runtime: None,
        }
    }
}

impl<S> MappingPlan<S>
where
    S: Clone + Send + Sync + 'static,
{
    /// 业务作用：创建空计划；未显式指定运行时时使用 `MappingRuntime::empty()`。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：指定应用已经完成构建的共享 MappingRuntime。
    pub fn with_runtime(mut self, runtime: Arc<MappingRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// 业务作用：注册覆盖全部 mapping 业务端点的 binding。
    pub fn global(mut self, binding: InterceptorBinding<S>) -> Self {
        let insertion = self.bindings.len();
        self.bindings.push(PlannedBinding {
            binding,
            origin: BindingOrigin::Global,
            path_prefix: None,
            insertion,
        });
        self
    }

    /// 业务作用：注册仅覆盖指定路径边界的 Router-scope binding。
    pub fn scope(
        mut self,
        path_prefix: impl Into<Arc<str>>,
        binding: InterceptorBinding<S>,
    ) -> Result<Self, MappingBuildError> {
        let path_prefix = path_prefix.into();
        if !valid_scope_prefix(&path_prefix) {
            return Err(MappingBuildError::new(
                "interceptor scope 必须是以 / 开头且不以 / 结尾的静态路径前缀",
            ));
        }
        let insertion = self.bindings.len();
        self.bindings.push(PlannedBinding {
            binding,
            origin: BindingOrigin::Scope,
            path_prefix: Some(path_prefix),
            insertion,
        });
        Ok(self)
    }

    /// 业务作用：返回计划指定的运行时；没有时建立空运行时。
    pub fn runtime_or_default(&self) -> Arc<MappingRuntime> {
        self.runtime
            .clone()
            .unwrap_or_else(|| Arc::new(MappingRuntime::empty()))
    }

    /// 业务作用：校验显式注入的 runtime 与装配调用使用的是同一个共享实例。
    ///
    /// napp 会从本计划取得 runtime 再交给业务 crate 的单态化工厂；非 napp 应用若同时传入
    /// 两个不同实例会在监听前失败，避免审计、InterceptorContext 与 endpoint 使用不同快照。
    #[doc(hidden)]
    pub fn validate_runtime(&self, runtime: &Arc<MappingRuntime>) -> Result<(), MappingBuildError> {
        if self
            .runtime
            .as_ref()
            .is_some_and(|configured| !Arc::ptr_eq(configured, runtime))
        {
            return Err(MappingBuildError::new(
                "MappingPlan 与 try_register_all 必须使用同一个 MappingRuntime 实例",
            ));
        }
        Ok(())
    }

    /// 业务作用：为单条路由合并应用 binding 与端点 definition，并完成拓扑排序。
    #[doc(hidden)]
    pub fn effective(
        &self,
        policy: RoutePolicy,
        endpoint_bindings: Vec<InterceptorBinding<S>>,
    ) -> Result<EffectiveInterceptors<S>, MappingBuildError> {
        let mut candidates = Vec::new();
        for entry in &self.bindings {
            if scope_matches(entry.path_prefix.as_deref(), policy.path_template)
                && entry.binding.matches_route(policy)?
            {
                candidates.push(entry.clone());
            }
        }
        let base = candidates.len();
        candidates.extend(
            endpoint_bindings
                .into_iter()
                .enumerate()
                .map(|(index, binding)| PlannedBinding {
                    binding,
                    origin: BindingOrigin::Endpoint,
                    path_prefix: None,
                    insertion: base + index,
                }),
        );
        effective_from_candidates(policy, candidates)
    }

    /// 业务作用：只用静态描述符计算审计摘要，必须与 `effective` 得到同一身份阶段结论。
    #[doc(hidden)]
    pub fn audit_route(
        &self,
        policy: RoutePolicy,
        endpoint_descriptors: &[InterceptorDescriptor],
    ) -> Result<InterceptorRouteAudit, MappingBuildError> {
        let mut candidates = Vec::new();
        for entry in &self.bindings {
            if scope_matches(entry.path_prefix.as_deref(), policy.path_template)
                && entry.binding.matches_route(policy)?
            {
                candidates.push(DescriptorCandidate {
                    descriptor: entry.binding.descriptor,
                    origin: entry.origin,
                    path_prefix: entry.path_prefix.clone(),
                    insertion: entry.insertion,
                });
            }
        }
        let base = candidates.len();
        candidates.extend(
            endpoint_descriptors
                .iter()
                .enumerate()
                .map(|(index, descriptor)| DescriptorCandidate {
                    descriptor: *descriptor,
                    origin: BindingOrigin::Endpoint,
                    path_prefix: None,
                    insertion: base + index,
                }),
        );
        let sorted = sort_descriptors(policy, candidates)?;
        let auth_descriptors = sorted
            .iter()
            .filter(|candidate| candidate.descriptor.stage == InterceptorStage::Auth)
            .map(|candidate| candidate.descriptor)
            .collect::<Vec<_>>();
        let has_auth = !auth_descriptors.is_empty();
        let requires_auth_runtime = auth_descriptors
            .iter()
            .any(|descriptor| descriptor.auth_runtime);
        let auth_runtime_count = auth_descriptors
            .iter()
            .filter(|descriptor| descriptor.auth_runtime)
            .count();
        if auth_runtime_count > 1 {
            return Err(MappingBuildError::new(format!(
                "路由 {} 只能挂载一个 auth_runtime 拦截器",
                policy.route_id
            )));
        }
        let all_auth_use_runtime = auth_descriptors
            .iter()
            .all(|descriptor| descriptor.auth_runtime);
        if has_auth
            && (policy.auth_provider.is_some() || policy.auth_condition.is_some())
            && !all_auth_use_runtime
        {
            return Err(MappingBuildError::new(format!(
                "路由 {} 只有显式 auth_runtime 拦截器才能消费 AuthProvider/AuthCondition",
                policy.route_id
            )));
        }
        Ok(InterceptorRouteAudit {
            has_auth,
            requires_auth_runtime,
        })
    }
}

/// 单条路由参与 MappingRuntime 审计的拦截器摘要。
#[derive(Debug, Clone, Copy)]
pub struct InterceptorRouteAudit {
    has_auth: bool,
    requires_auth_runtime: bool,
}

impl InterceptorRouteAudit {
    /// 业务作用：返回当前 effective plan 是否包含 auth-stage 拦截器。
    pub fn has_auth(self) -> bool {
        self.has_auth
    }

    /// 业务作用：返回 effective auth plan 是否依赖快照中的 AuthRuntime。
    pub fn requires_auth_runtime(self) -> bool {
        self.requires_auth_runtime
    }
}

/// 已按入站顺序分段的单路由执行计划。
#[doc(hidden)]
pub struct EffectiveInterceptors<S> {
    edge: Vec<EffectiveBinding<S>>,
    auth: Vec<EffectiveBinding<S>>,
    plaintext: Vec<EffectiveBinding<S>>,
}

/// 已补齐作用域与 stage 内有效顺序的单路由 binding。
struct EffectiveBinding<S> {
    binding: InterceptorBinding<S>,
    scope: InterceptorScope,
    effective_order: usize,
}

impl<S> EffectiveInterceptors<S>
where
    S: Clone + Send + Sync + 'static,
{
    /// 业务作用：是否存在会在 AuthContext gate 前执行的认证拦截器。
    pub fn has_auth(&self) -> bool {
        !self.auth.is_empty()
    }

    /// 业务作用：按 Tower 套层需要的逆序挂载某一阶段。
    fn apply_group(
        bindings: &[EffectiveBinding<S>],
        mut route: MethodRouter<S>,
        state: &S,
        policy: RoutePolicy,
        runtime: Arc<MappingRuntime>,
    ) -> Result<MethodRouter<S>, MappingBuildError> {
        for binding in bindings.iter().rev() {
            route = binding.binding.apply(
                route,
                state,
                policy,
                runtime.clone(),
                binding.scope.clone(),
                binding.effective_order,
            )?;
        }
        Ok(route)
    }

    /// 业务作用：挂载位于 request decrypt 之后、response encrypt 之前的明文阶段。
    pub fn apply_plaintext(
        &self,
        route: MethodRouter<S>,
        state: &S,
        policy: RoutePolicy,
        runtime: Arc<MappingRuntime>,
    ) -> Result<MethodRouter<S>, MappingBuildError> {
        Self::apply_group(&self.plaintext, route, state, policy, runtime)
    }

    /// 业务作用：挂载位于 AuthContext gate 与 request decrypt 之前的身份阶段。
    pub fn apply_auth(
        &self,
        route: MethodRouter<S>,
        state: &S,
        policy: RoutePolicy,
        runtime: Arc<MappingRuntime>,
    ) -> Result<MethodRouter<S>, MappingBuildError> {
        Self::apply_group(&self.auth, route, state, policy, runtime)
    }

    /// 业务作用：挂载能观察原始请求和最终响应的最外层 edge 阶段。
    pub fn apply_edge(
        &self,
        route: MethodRouter<S>,
        state: &S,
        policy: RoutePolicy,
        runtime: Arc<MappingRuntime>,
    ) -> Result<MethodRouter<S>, MappingBuildError> {
        Self::apply_group(&self.edge, route, state, policy, runtime)
    }
}

/// 业务作用：审计候选 descriptor、稳定排序并按安全 stage 生成最终拦截器执行计划。
fn effective_from_candidates<S>(
    policy: RoutePolicy,
    candidates: Vec<PlannedBinding<S>>,
) -> Result<EffectiveInterceptors<S>, MappingBuildError>
where
    S: Clone + Send + Sync + 'static,
{
    let descriptors = candidates
        .iter()
        .map(|candidate| DescriptorCandidate {
            descriptor: candidate.binding.descriptor,
            origin: candidate.origin,
            path_prefix: candidate.path_prefix.clone(),
            insertion: candidate.insertion,
        })
        .collect::<Vec<_>>();
    let sorted = sort_descriptors(policy, descriptors)?;
    let mut by_id = candidates
        .into_iter()
        .map(|candidate| (candidate.binding.descriptor.id, candidate))
        .collect::<HashMap<_, _>>();
    let mut result = EffectiveInterceptors {
        edge: Vec::new(),
        auth: Vec::new(),
        plaintext: Vec::new(),
    };
    let mut stage_orders = HashMap::<InterceptorStage, usize>::new();
    for candidate in sorted {
        let planned = by_id
            .remove(candidate.descriptor.id)
            .ok_or_else(|| MappingBuildError::new("interceptor effective plan 不一致"))?;
        let effective_order = stage_orders.entry(candidate.descriptor.stage).or_default();
        let scope = scope_from_planned(&planned)?;
        let binding = EffectiveBinding {
            binding: planned.binding,
            scope,
            effective_order: *effective_order,
        };
        *effective_order += 1;
        match candidate.descriptor.stage {
            InterceptorStage::Edge => result.edge.push(binding),
            InterceptorStage::Auth => result.auth.push(binding),
            InterceptorStage::Plaintext => result.plaintext.push(binding),
        }
    }
    Ok(result)
}

/// 参与排序审计的轻量 descriptor 投影，不携带 Tower 挂载闭包。
#[derive(Clone)]
struct DescriptorCandidate {
    descriptor: InterceptorDescriptor,
    origin: BindingOrigin,
    path_prefix: Option<Arc<str>>,
    insertion: usize,
}

/// 业务作用：校验重复、stage/scope 依赖并执行稳定拓扑排序，循环依赖在监听前失败。
fn sort_descriptors(
    policy: RoutePolicy,
    mut candidates: Vec<DescriptorCandidate>,
) -> Result<Vec<DescriptorCandidate>, MappingBuildError> {
    // 重复是静态装配错误，必须先于 public 路由的 auth 排除检查。否则同一个 auth interceptor
    // 同时自动和手动装配，在恰好只有 public 路由时会被静默吞掉，违背 fail-fast 合同。
    let mut ids = HashMap::with_capacity(candidates.len());
    for (index, candidate) in candidates.iter().enumerate() {
        if ids.insert(candidate.descriptor.id, index).is_some() {
            return Err(MappingBuildError::new(format!(
                "路由 {} 的 interceptor ID {} 重复",
                policy.route_id, candidate.descriptor.id
            )));
        }
    }

    if matches!(
        policy.auth,
        crate::AuthRequirement::Public | crate::AuthRequirement::Unspecified
    ) {
        if candidates.iter().any(|candidate| {
            candidate.origin == BindingOrigin::Endpoint
                && candidate.descriptor.stage == InterceptorStage::Auth
        }) {
            return Err(MappingBuildError::new(format!(
                "路由 {} 未声明 required/optional auth，禁止显式绑定 auth interceptor",
                policy.route_id,
            )));
        }
        candidates.retain(|candidate| candidate.descriptor.stage != InterceptorStage::Auth);
    }

    // public/未声明 auth 会移除非 endpoint auth；依赖图必须基于过滤后的下标重新建立。
    ids.clear();
    for (index, candidate) in candidates.iter().enumerate() {
        ids.insert(candidate.descriptor.id, index);
    }

    let mut outgoing = vec![HashSet::<usize>::new(); candidates.len()];
    let mut indegree = vec![0_usize; candidates.len()];
    for (index, candidate) in candidates.iter().enumerate() {
        for target in candidate.descriptor.before {
            add_dependency(
                policy,
                &candidates,
                &ids,
                index,
                target,
                true,
                &mut outgoing,
                &mut indegree,
            )?;
        }
        for target in candidate.descriptor.after {
            add_dependency(
                policy,
                &candidates,
                &ids,
                index,
                target,
                false,
                &mut outgoing,
                &mut indegree,
            )?;
        }
    }

    let mut sorted = Vec::with_capacity(candidates.len());
    let mut emitted = vec![false; candidates.len()];
    while sorted.len() < candidates.len() {
        let next = (0..candidates.len())
            .filter(|index| !emitted[*index] && indegree[*index] == 0)
            .min_by_key(|index| candidate_sort_key(&candidates[*index]));
        let Some(index) = next else {
            return Err(MappingBuildError::new(format!(
                "路由 {} 的 interceptor before/after 存在循环依赖",
                policy.route_id
            )));
        };
        emitted[index] = true;
        sorted.push(candidates[index].clone());
        for target in outgoing[index].iter().copied() {
            indegree[target] = indegree[target].saturating_sub(1);
        }
    }
    Ok(sorted)
}

#[allow(clippy::too_many_arguments)]
/// 业务作用：为 before/after 约束加入同 stage、同 scope 的有向边，并维护目标入度。
fn add_dependency(
    policy: RoutePolicy,
    candidates: &[DescriptorCandidate],
    ids: &HashMap<&'static str, usize>,
    source: usize,
    target_id: &'static str,
    source_before_target: bool,
    outgoing: &mut [HashSet<usize>],
    indegree: &mut [usize],
) -> Result<(), MappingBuildError> {
    let target = *ids.get(target_id).ok_or_else(|| {
        MappingBuildError::new(format!(
            "路由 {} 的 interceptor {} 依赖未绑定的 {}",
            policy.route_id, candidates[source].descriptor.id, target_id
        ))
    })?;
    if candidates[source].descriptor.stage != candidates[target].descriptor.stage {
        return Err(MappingBuildError::new(format!(
            "路由 {} 的 interceptor 依赖不能跨 stage",
            policy.route_id
        )));
    }
    if scope_sort_key(&candidates[source]) != scope_sort_key(&candidates[target]) {
        return Err(MappingBuildError::new(format!(
            "路由 {} 的 interceptor 依赖不能跨 global/scope/endpoint 边界",
            policy.route_id
        )));
    }
    let (from, to) = if source_before_target {
        (source, target)
    } else {
        (target, source)
    };
    if outgoing[from].insert(to) {
        indegree[to] += 1;
    }
    Ok(())
}

/// 业务作用：把内部计划来源转换为业务只读上下文，不暴露排序实现细节。
fn scope_from_planned<S>(
    planned: &PlannedBinding<S>,
) -> Result<InterceptorScope, MappingBuildError> {
    match planned.origin {
        BindingOrigin::Global => Ok(InterceptorScope::Global),
        BindingOrigin::Scope => planned
            .path_prefix
            .clone()
            .map(InterceptorScope::Router)
            .ok_or_else(|| MappingBuildError::new("interceptor scope 计划缺少静态路径前缀")),
        BindingOrigin::Endpoint => Ok(InterceptorScope::Endpoint),
    }
}

/// 业务作用：返回作用域层级排序键：global → 外层 scope → 内层 scope → endpoint。
fn scope_sort_key(candidate: &DescriptorCandidate) -> (u8, usize, &str) {
    match candidate.origin {
        BindingOrigin::Global => (0, 0, ""),
        BindingOrigin::Scope => {
            let prefix = candidate.path_prefix.as_deref().unwrap_or("/");
            (
                1,
                prefix.split('/').filter(|part| !part.is_empty()).count(),
                prefix,
            )
        }
        BindingOrigin::Endpoint => (2, usize::MAX, ""),
    }
}

/// 业务作用：稳定排序先保护固定安全 stage，再保护声明范围，最后才允许业务 order 调整同范围顺序。
fn candidate_sort_key(candidate: &DescriptorCandidate) -> (u8, u8, usize, &str, i32, usize) {
    let (scope_rank, depth, prefix) = scope_sort_key(candidate);
    (
        candidate.descriptor.stage.rank(),
        scope_rank,
        depth,
        prefix,
        candidate.descriptor.order,
        candidate.insertion,
    )
}

/// 业务作用：校验 scope 前缀为无通配符、无参数、无重复分隔符的绝对静态路径。
fn valid_scope_prefix(prefix: &str) -> bool {
    prefix.starts_with('/')
        && (prefix == "/" || !prefix.ends_with('/'))
        && !prefix.contains("//")
        && !prefix.chars().any(|character| {
            character.is_control() || matches!(character, '{' | '}' | '*' | '?' | '#')
        })
}

/// 业务作用：按完整路径段边界判断路由是否落在 scope 内，避免 `/api` 误匹配 `/api2`。
fn scope_matches(prefix: Option<&str>, path: &str) -> bool {
    match prefix {
        None | Some("/") => true,
        Some(prefix) => {
            path == prefix
                || path
                    .strip_prefix(prefix)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        }
    }
}
