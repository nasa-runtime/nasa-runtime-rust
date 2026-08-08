//! 路由安全运行时、不可变快照与启动审计。

use std::collections::HashSet;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use sha2::{Digest, Sha256};

use crate::policy::{
    AuthRequirement, CryptoRequirement, ReplayRequirement, RoutePolicy, RouteSecurityPlan,
};

/// 启动审计或快照替换失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingBuildError {
    message: String,
}

impl MappingBuildError {
    /// 业务作用：建立不包含敏感配置值的启动错误。
    ///
    /// # 参数
    ///
    /// - `message`: 面向维护者的安全诊断文本，不得包含密钥、token、明文或完整密文。
    ///
    /// # 返回
    ///
    /// 返回可由应用启动入口上抛的构建错误。
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for MappingBuildError {
    /// 业务作用：把安全诊断写入标准格式化器。
    ///
    /// # 参数
    ///
    /// - `formatter`: 错误链提供的输出目标。
    ///
    /// # 返回
    ///
    /// 返回标准格式化结果。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MappingBuildError {}

/// 一次完整构建后发布给请求读取的不可变运行时快照。
#[derive(Clone)]
pub struct MappingRuntimeSnapshot {
    /// 单调递增的配置代次，用于识别集群漂移。
    pub generation: u64,
    /// 是否要求所有路由显式声明身份策略。
    pub require_explicit_auth: bool,
    /// 快照完成构建的时间，不参与协议计算。
    pub created_at: SystemTime,
    /// 可选身份运行时；只在 auth feature 下存在。
    #[cfg(feature = "auth")]
    pub auth: Option<Arc<crate::auth::AuthRuntime>>,
    /// 可选加密运行时；只在 crypto feature 下存在。
    #[cfg(feature = "crypto")]
    pub crypto: Option<Arc<crate::crypto::CryptoRuntime>>,
}

impl fmt::Debug for MappingRuntimeSnapshot {
    /// 业务作用：输出不包含身份缓存、密钥或 provider 内部状态的快照摘要。
    ///
    /// # 参数
    ///
    /// - `formatter`: 调试输出目标。
    ///
    /// # 返回
    ///
    /// 返回标准格式化结果。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MappingRuntimeSnapshot")
            .field("generation", &self.generation)
            .field("require_explicit_auth", &self.require_explicit_auth)
            .field("created_at", &self.created_at)
            .finish_non_exhaustive()
    }
}

/// 支持 last-good 原子替换的端点安全运行时。
#[derive(Clone)]
pub struct MappingRuntime {
    current: Arc<ArcSwap<MappingRuntimeSnapshot>>,
    /// 串行化“检查代次并发布”这一复合操作，避免并发热更新用较旧快照覆盖较新快照。
    replacement_lock: Arc<Mutex<()>>,
    /// 首次成功启动审计后冻结的完整路由合同。热更新必须对同一合同重新审计，不能只验证
    /// generation 和 key 身份后直接发布。
    route_contract: Arc<Mutex<Option<RouteContract>>>,
    /// 最近一次热刷新失败的 Unix epoch 毫秒；零表示当前没有未恢复的刷新错误。
    last_reload_failure_epoch_ms: Arc<AtomicU64>,
    /// 跨快照持续累积且只使用静态路由 label 的安全指标注册表。
    #[cfg(any(feature = "auth", feature = "crypto"))]
    metrics: Arc<crate::SecurityMetrics>,
    /// 最近一次存在的密码运行时锚点；即使中间快照临时关闭全部加密路由，也保留进程级 key
    /// 调用次数和 retired 历史，防止通过“移除再添加”重置安全状态。
    #[cfg(feature = "crypto")]
    key_usage_anchor: Arc<Mutex<Option<Arc<crate::crypto::CryptoRuntime>>>>,
}

impl MappingRuntime {
    /// 业务作用：构造只允许无安全策略路由的空运行时。
    ///
    /// # 返回
    ///
    /// 返回 generation 为零且不要求显式身份策略的运行时。
    pub fn empty() -> Self {
        Self::from_snapshot(MappingRuntimeSnapshot {
            generation: 0,
            require_explicit_auth: false,
            created_at: SystemTime::now(),
            #[cfg(feature = "auth")]
            auth: None,
            #[cfg(feature = "crypto")]
            crypto: None,
        })
    }

    /// 业务作用：从已经完整校验的快照创建运行时。
    ///
    /// # 参数
    ///
    /// - `snapshot`: 启动构建器生成的不可变完整快照。
    ///
    /// # 返回
    ///
    /// 返回可克隆并注入多个 Router 的运行时句柄。
    pub fn from_snapshot(snapshot: MappingRuntimeSnapshot) -> Self {
        #[cfg(any(feature = "auth", feature = "crypto"))]
        let metrics = Arc::new(crate::SecurityMetrics::new(snapshot.generation));
        #[cfg(feature = "crypto")]
        let key_usage_anchor = Arc::new(Mutex::new(snapshot.crypto.clone()));
        Self {
            current: Arc::new(ArcSwap::from_pointee(snapshot)),
            replacement_lock: Arc::new(Mutex::new(())),
            route_contract: Arc::new(Mutex::new(None)),
            last_reload_failure_epoch_ms: Arc::new(AtomicU64::new(0)),
            #[cfg(any(feature = "auth", feature = "crypto"))]
            metrics,
            #[cfg(feature = "crypto")]
            key_usage_anchor,
        }
    }

    /// 业务作用：获取当前请求应固定持有的完整快照。
    ///
    /// # 返回
    ///
    /// 返回当前 `Arc`；调用方必须在一次请求内持续持有它，避免跨代读取。
    pub fn snapshot(&self) -> Arc<MappingRuntimeSnapshot> {
        self.current.load_full()
    }

    /// 业务作用：原子发布一个更高代次的完整 last-good 快照。
    ///
    /// # 参数
    ///
    /// - `snapshot`: 已完成配置合并和组件自检的新快照；本方法会针对启动时冻结的完整路由
    ///   合同再次执行 route audit。
    ///
    /// # 返回
    ///
    /// 替换成功返回空值；旧请求继续持有旧 `Arc`。
    ///
    /// # 错误
    ///
    /// 尚未完成启动路由审计、候选快照不能服务既有路由、新代次没有严格递增或发布锁异常时
    /// 拒绝替换，完整保留 last-good。
    pub fn replace(&self, snapshot: MappingRuntimeSnapshot) -> Result<(), MappingBuildError> {
        // 代次比较与 ArcSwap 写入必须属于同一个临界区。若只依赖原子 store，两个并发更新都可能
        // 读到旧代次，随后由较小代次后写覆盖较大代次，破坏 last-good 的单调发布语义。
        let _replacement_guard = self
            .replacement_lock
            .lock()
            .map_err(|_| MappingBuildError::new("安全运行时发布锁不可用"))?;
        let current = self.current.load();
        if snapshot.generation <= current.generation {
            return Err(MappingBuildError::new(format!(
                "安全运行时 generation 必须递增：current={} next={}",
                current.generation, snapshot.generation
            )));
        }
        let route_contract = self
            .route_contract
            .lock()
            .map_err(|_| MappingBuildError::new("路由合同锁不可用"))?
            .clone()
            .ok_or_else(|| {
                MappingBuildError::new("安全运行时尚未完成启动路由审计，拒绝发布热更新")
            })?;
        // 审计必须位于 ArcSwap 发布和 key 使用状态迁移之前。这样候选快照即使缺 provider、
        // key ring 或 replay guard，也不会改变 last-good 或进程级 key 调用次数。
        self.audit_snapshot(&snapshot, &route_contract.routes)?;
        #[cfg(feature = "crypto")]
        let mut key_usage_anchor = self
            .key_usage_anchor
            .lock()
            .map_err(|_| MappingBuildError::new("密钥使用锚点锁不可用"))?;
        #[cfg(feature = "crypto")]
        if let (Some(next_crypto), Some(previous_crypto)) =
            (snapshot.crypto.as_ref(), key_usage_anchor.as_ref())
        {
            // 密钥次数和 retired 状态属于进程生命周期安全状态，不能随配置快照重建。绑定发生在
            // ArcSwap 发布之前；任何身份冲突都会保留完整的 last-good 快照。
            next_crypto.adopt_key_usage_registry(previous_crypto)?;
        }
        #[cfg(feature = "crypto")]
        let next_crypto_anchor = snapshot.crypto.clone();
        #[cfg(any(feature = "auth", feature = "crypto"))]
        let next_generation = snapshot.generation;
        self.current.store(Arc::new(snapshot));
        #[cfg(feature = "crypto")]
        if next_crypto_anchor.is_some() {
            *key_usage_anchor = next_crypto_anchor;
        }
        // generation gauge 只能在 ArcSwap 已发布后推进，避免抓取端短暂看到尚不可读取的新代次。
        #[cfg(any(feature = "auth", feature = "crypto"))]
        self.metrics.set_generation(next_generation);
        #[cfg(any(feature = "auth", feature = "crypto"))]
        self.metrics.record_reload(true);
        self.clear_reload_failure();
        Ok(())
    }

    /// 业务作用：记录一次未发布、已保留 last-good 的热刷新失败。
    ///
    /// # 语义
    ///
    /// 本方法只记录低敏感时间状态，不接收错误文本，因此不会把 secret 来源或后端详情带入健康
    /// 输出。调用方应在配置解析、组件构建或路由审计失败时调用。
    pub fn record_reload_failure(&self) {
        self.last_reload_failure_epoch_ms
            .store(current_epoch_millis(), Ordering::Release);
        #[cfg(any(feature = "auth", feature = "crypto"))]
        self.metrics.record_reload(false);
    }

    /// 业务作用：清除最近一次热刷新失败标志。
    ///
    /// # 语义
    ///
    /// 只有新快照已经原子发布后才应调用；[`Self::replace`] 成功时会自动执行。
    pub fn clear_reload_failure(&self) {
        self.last_reload_failure_epoch_ms
            .store(0, Ordering::Release);
    }

    /// 业务作用：返回不含路由、密钥和后端详情的健康摘要。
    ///
    /// # 返回
    ///
    /// 返回当前代次、配置年龄和最近未恢复刷新失败的年龄，供管理端 health body 与指标使用。
    pub fn health(&self) -> MappingRuntimeHealth {
        let snapshot = self.snapshot();
        let now = SystemTime::now();
        let failure_epoch_ms = self.last_reload_failure_epoch_ms.load(Ordering::Acquire);
        MappingRuntimeHealth {
            generation: snapshot.generation,
            snapshot_age: now.duration_since(snapshot.created_at).unwrap_or_default(),
            last_reload_failure_age: (failure_epoch_ms != 0).then(|| {
                Duration::from_millis(current_epoch_millis().saturating_sub(failure_epoch_ms))
            }),
        }
    }

    /// 业务作用：返回跨热更新持续存在的安全指标注册表。
    ///
    /// # 返回
    ///
    /// 返回共享句柄；注册表只接受编译期 route policy，不接受 raw path、token、kid 或明文。
    #[cfg(any(feature = "auth", feature = "crypto"))]
    pub fn metrics(&self) -> Arc<crate::SecurityMetrics> {
        self.metrics.clone()
    }

    /// 业务作用：在端口监听前审计完整路由集合。
    ///
    /// # 参数
    ///
    /// - `routes`: linkme 收集并由调用方转成的静态路由策略切片。
    ///
    /// # 返回
    ///
    /// 全部路由可安全服务时返回稳定审计结果。
    ///
    /// # 错误
    ///
    /// 重复路由、未声明身份、缺少 feature/runtime/provider/key/replay 或危险组合都会拒绝启动。
    pub fn audit_routes(&self, routes: &[RoutePolicy]) -> Result<RouteAudit, MappingBuildError> {
        let plans = routes
            .iter()
            .copied()
            .map(|policy| RouteSecurityPlan {
                policy,
                auth_interceptor: false,
                auth_runtime: false,
            })
            .collect::<Vec<_>>();
        self.audit_route_plans(&plans)
    }

    /// 业务作用：审计已经合并 effective interceptor plan 的完整路由合同。
    ///
    /// auth-stage interceptor 可以替代旧 AuthRuntime provider，但请求期仍必须通过
    /// AuthContext gate。本方法只确认不可变挂载合同，不信任任何请求输入。
    pub fn audit_route_plans(
        &self,
        routes: &[RouteSecurityPlan],
    ) -> Result<RouteAudit, MappingBuildError> {
        let snapshot = self.snapshot();
        let audit = self.audit_snapshot(&snapshot, routes)?;
        self.bind_route_contract(routes, &audit)?;
        Ok(audit)
    }

    /// 业务作用：在同一个固定快照上执行路由审计与远程依赖就绪探测。
    ///
    /// # 参数
    ///
    /// - `routes`: 当前二进制收集的完整静态路由集合。
    ///
    /// # 返回
    ///
    /// route/key/provider 审计和 required replay 后端探测均成功时返回同一快照的审计结果；失败
    /// 时调用方应返回 not-ready，但继续保留 last-good 供在途请求完成。
    pub async fn readiness(&self, routes: &[RoutePolicy]) -> Result<RouteAudit, MappingBuildError> {
        let plans = routes
            .iter()
            .copied()
            .map(|policy| RouteSecurityPlan {
                policy,
                auth_interceptor: false,
                auth_runtime: false,
            })
            .collect::<Vec<_>>();
        let snapshot = self.snapshot();
        let audit = self.audit_snapshot(&snapshot, &plans)?;
        self.bind_route_contract(&plans, &audit)?;
        #[cfg(feature = "crypto")]
        if let Some(crypto) = snapshot.crypto.as_ref() {
            crypto.readiness(routes).await?;
        }
        Ok(audit)
    }

    /// 业务作用：使用启动时已冻结的完整路由与 interceptor 合同执行 readiness。
    ///
    /// 普通 [`Self::readiness`] 适合尚未引入 interceptor plan 的旧应用；新式应用必须
    /// 保留 `auth_interceptor/auth_runtime` 位，否则管理端会用另一份合同误判健康。
    ///
    /// # 返回
    ///
    /// 当前 last-good 快照仍满足冻结合同且 required replay 后端可用时返回审计摘要。
    pub async fn readiness_bound(&self) -> Result<RouteAudit, MappingBuildError> {
        let contract = self
            .route_contract
            .lock()
            .map_err(|_| MappingBuildError::new("路由合同锁不可用"))?
            .clone()
            .ok_or_else(|| MappingBuildError::new("安全运行时尚未完成启动路由审计"))?;
        let snapshot = self.snapshot();
        let audit = self.audit_snapshot(&snapshot, &contract.routes)?;
        #[cfg(feature = "crypto")]
        if let Some(crypto) = snapshot.crypto.as_ref() {
            let policies = contract
                .routes
                .iter()
                .map(|route| route.policy)
                .collect::<Vec<_>>();
            crypto.readiness(&policies).await?;
        }
        Ok(audit)
    }

    /// 业务作用：查询启动期冻结合同中的某条路由是否启用了请求或响应加密。
    ///
    /// 该入口只暴露静态、非敏感的路由能力位，供外层治理层在**执行 handler 之前**拒绝无法安全
    /// 组合的能力（例如当前 HTTP 幂等中间件尚不能缓存 plaintext 后再逐请求重新加密）。路径必须是
    /// 宏登记的模板路径，不含 Application context path。
    pub fn route_has_crypto(
        &self,
        method: &str,
        path_template: &str,
    ) -> Result<bool, MappingBuildError> {
        let contract = self
            .route_contract
            .lock()
            .map_err(|_| MappingBuildError::new("路由合同锁不可用"))?;
        let contract = contract
            .as_ref()
            .ok_or_else(|| MappingBuildError::new("安全运行时尚未完成启动路由审计"))?;
        Ok(contract.routes.iter().any(|route| {
            route.policy.method == method
                && route.policy.path_template == path_template
                && route.policy.has_crypto()
        }))
    }

    /// 业务作用：冻结首次成功审计的静态路由集合，并拒绝同一运行时随后改用另一份合同。
    fn bind_route_contract(
        &self,
        routes: &[RouteSecurityPlan],
        audit: &RouteAudit,
    ) -> Result<(), MappingBuildError> {
        let routes = canonical_routes(routes);
        let mut contract = self
            .route_contract
            .lock()
            .map_err(|_| MappingBuildError::new("路由合同锁不可用"))?;
        if let Some(existing) = contract.as_ref() {
            if existing.routes.as_ref() != routes.as_slice() {
                return Err(MappingBuildError::new(format!(
                    "安全运行时路由合同已经冻结：current={} attempted={}",
                    existing.fingerprint, audit.fingerprint
                )));
            }
            return Ok(());
        }
        *contract = Some(RouteContract {
            routes: routes.into(),
            fingerprint: audit.fingerprint.clone(),
        });
        Ok(())
    }

    /// 业务作用：对调用方已经固定的快照执行纯内存路由审计。
    ///
    /// # 参数
    ///
    /// - `snapshot`: 一次 readiness 或启动审计期间不得替换的不可变快照。
    /// - `routes`: 当前二进制收集的完整静态路由集合。
    ///
    /// # 返回
    ///
    /// 返回该快照对应的稳定指纹、代次与高风险项；任何合同不完整都会 fail closed。
    fn audit_snapshot(
        &self,
        snapshot: &MappingRuntimeSnapshot,
        routes: &[RouteSecurityPlan],
    ) -> Result<RouteAudit, MappingBuildError> {
        let mut identities = HashSet::with_capacity(routes.len());
        let mut high_risk = Vec::new();

        for route_plan in routes {
            let route = &route_plan.policy;
            let identity = (route.method, route.path_template);
            if !identities.insert(identity) {
                return Err(MappingBuildError::new(format!(
                    "重复路由 {} {}",
                    route.method, route.path_template
                )));
            }
            if snapshot.require_explicit_auth && route.auth == AuthRequirement::Unspecified {
                return Err(MappingBuildError::new(format!(
                    "路由 {} 未显式声明 auth",
                    route.route_id
                )));
            }
            if route.auth == AuthRequirement::Unspecified {
                high_risk.push(format!("{}:implicit-auth-policy", route.route_id));
            }

            #[cfg(feature = "auth")]
            if !matches!(
                route.auth,
                AuthRequirement::Unspecified | AuthRequirement::Public
            ) && (!route_plan.auth_interceptor || route_plan.auth_runtime)
            {
                let runtime = snapshot.auth.as_ref().ok_or_else(|| {
                    MappingBuildError::new(format!(
                        "路由 {} 的 auth 流水线需要 AuthRuntime，但快照未注入",
                        route.route_id
                    ))
                })?;
                runtime.audit(route)?;
            }
            #[cfg(not(feature = "auth"))]
            if !matches!(
                route.auth,
                AuthRequirement::Unspecified | AuthRequirement::Public
            ) {
                return Err(MappingBuildError::new(format!(
                    "路由 {} 需要 web-auth feature",
                    route.route_id
                )));
            }

            if route.has_crypto() {
                #[cfg(feature = "crypto")]
                {
                    let runtime = snapshot.crypto.as_ref().ok_or_else(|| {
                        MappingBuildError::new(format!(
                            "路由 {} 声明 crypto 但未注入 CryptoRuntime",
                            route.route_id
                        ))
                    })?;
                    runtime.audit(route)?;
                }
                #[cfg(not(feature = "crypto"))]
                {
                    return Err(MappingBuildError::new(format!(
                        "路由 {} 需要 web-crypto feature",
                        route.route_id
                    )));
                }
            }

            if matches!(route.method, "POST" | "PUT" | "PATCH" | "DELETE")
                && route.crypto_protocol == Some("modern-v2")
                && route.replay == ReplayRequirement::Disabled
            {
                high_risk.push(format!("{}:modern-write-without-replay", route.route_id));
            }
            if route.crypto_protocol == Some("legacy-v1") {
                high_risk.push(format!("{}:legacy-protocol", route.route_id));
                if route.replay == ReplayRequirement::Required {
                    return Err(MappingBuildError::new(format!(
                        "路由 {} 的 legacy-v1 信封没有 rid，不能声明 required replay",
                        route.route_id
                    )));
                }
            }
            if route.crypto_condition == Some("legacy-disable-header") {
                high_risk.push(format!("{}:crypto-bypass", route.route_id));
            }
            if route.crypto_response == CryptoRequirement::Required
                && route.crypto_protocol == Some("modern-v2")
                && route.crypto_request == CryptoRequirement::Disabled
            {
                return Err(MappingBuildError::new(format!(
                    "路由 {} 的 modern-v2 响应必须绑定同一请求的安全上下文",
                    route.route_id
                )));
            }
        }

        #[cfg(any(feature = "auth", feature = "crypto"))]
        for route in routes {
            self.metrics.route(route.policy);
        }

        Ok(RouteAudit {
            fingerprint: compute_route_fingerprint(routes),
            route_count: routes.len(),
            high_risk,
            generation: snapshot.generation,
        })
    }
}

/// 首次成功审计后与运行时绑定的规范化静态路由合同。
#[derive(Clone)]
struct RouteContract {
    routes: Arc<[RouteSecurityPlan]>,
    fingerprint: String,
}

/// 管理端可公开的运行时健康摘要。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MappingRuntimeHealth {
    /// 当前 last-good 快照的单调代次。
    pub generation: u64,
    /// 从当前快照完成构建到本次读取经过的时间。
    pub snapshot_age: Duration,
    /// 最近一次未恢复热刷新失败到本次读取经过的时间；`None` 表示随后已有成功发布。
    pub last_reload_failure_age: Option<Duration>,
}

impl Default for MappingRuntime {
    /// 业务作用：建立兼容无安全路由的默认运行时。
    ///
    /// # 返回
    ///
    /// 返回与 [`MappingRuntime::empty`] 相同的值。
    fn default() -> Self {
        Self::empty()
    }
}

/// 启动审计的非敏感结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteAudit {
    /// 按路由排序后计算的 SHA-256 十六进制指纹。
    pub fingerprint: String,
    /// 参与审计的路由数量。
    pub route_count: usize,
    /// 需要迁移或人工接受的有限高风险项。
    pub high_risk: Vec<String>,
    /// 审计所使用的运行时配置代次。
    pub generation: u64,
}

/// 业务作用：按稳定字段计算路由集合指纹。
///
/// - `routes`: 已完成重复检查的静态路由集合；函数内部排序，不依赖链接器收集顺序。
///
/// 返回小写 SHA-256 十六进制文本，不包含任何运行时 secret。
fn compute_route_fingerprint(routes: &[RouteSecurityPlan]) -> String {
    let sorted = canonical_routes(routes);
    let mut digest = Sha256::new();
    for route in sorted {
        digest.update(format!("{route:?}\n").as_bytes());
    }
    format!("{:x}", digest.finalize())
}

/// 业务作用：返回与链接器收集顺序无关的规范化路由集合。
fn canonical_routes(routes: &[RouteSecurityPlan]) -> Vec<RouteSecurityPlan> {
    let mut sorted = routes.to_vec();
    sorted.sort_by_key(|route| {
        (
            route.policy.method,
            route.policy.path_template,
            route.policy.handler,
        )
    });
    sorted
}

/// 业务作用：返回适合原子健康状态记录的 Unix epoch 毫秒。
///
/// # 返回
///
/// 正常系统时钟返回非零毫秒；时钟早于 epoch 或超出 `u64` 时返回一，保留“存在失败”语义。
fn current_epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|value| u64::try_from(value.as_millis()).ok())
        .filter(|value| *value != 0)
        .unwrap_or(1)
}
