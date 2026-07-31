//! Web 端点密码运行时、双协议、密钥、重放与资源治理。

mod condition;
mod error;
mod executor;
mod keyring;
mod legacy_v1;
mod limits;
mod modern_v2;
mod protocol;
mod provider;
mod replay;

pub use condition::*;
pub use error::*;
pub use executor::*;
pub use keyring::*;
pub use legacy_v1::*;
pub use limits::*;
pub use modern_v2::*;
pub use protocol::*;
pub use provider::*;
pub use replay::*;

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;

use crate::{AuthRequirement, CryptoDirections, MappingBuildError, ReplayRequirement, RoutePolicy};

/// 单进程最多持续记忆的密钥身份数量，防止错误热更新用不断变化的 kid 消耗无界内存。
const MAX_KEY_USAGE_IDENTITIES: usize = 4_096;

/// 进程级密钥状态的公开身份，由租户域、路由密钥域和 kid 共同组成。
type KeyUsageIdentity = (Arc<str>, Arc<str>, Arc<str>);

/// 一个公开密钥身份在进程生命周期内持续保存的安全元数据。
struct KeyUsageRecord {
    /// 首次注册的算法，后续同一身份不得改变。
    algorithm: KeyAlgorithm,
    /// 首次注册的材料指纹，避免同一 kid 对应不同密钥而产生解密歧义。
    material_fingerprint: [u8; 32],
    /// 热更新前后与旧请求共同消费的原子调用次数。
    invocations: Arc<AtomicU64>,
    /// 历史发布过的最大调用上限；新快照只允许保持或提高，不能让旧快照按更高旧值继续消费。
    max_invocations: u64,
    /// 一旦进入 retired 就不可恢复，防止删除后重新添加绕过生命周期约束。
    retired: bool,
}

/// 跨不可变快照持续存在的密钥调用次数与生命周期注册表。
struct KeyUsageRegistry {
    /// key 是 tenant scope、key scope 与 kid，值不包含原始密钥材料。
    records: Mutex<HashMap<KeyUsageIdentity, KeyUsageRecord>>,
}

impl KeyUsageRegistry {
    /// 建立空的进程级密钥使用注册表。
    ///
    /// # 返回
    ///
    /// 返回尚未绑定任何 key ring 的有界注册表。
    fn new() -> Self {
        Self {
            records: Mutex::new(HashMap::new()),
        }
    }

    /// 原子校验并把一组新快照 key ring 绑定到持续计数器。
    ///
    /// # 参数
    ///
    /// - `rings`: 待启动或待发布快照中的完整有限 key ring 集合。
    ///
    /// # 返回
    ///
    /// 所有身份均兼容时返回空值；同一 kid 偷换算法或材料、恢复 retired 身份、耗尽新上限、
    /// 超过注册表容量或内部锁异常时拒绝整个快照。
    fn attach(
        &self,
        rings: &HashMap<(Arc<str>, Arc<str>), Arc<KeyRing>>,
    ) -> Result<(), MappingBuildError> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| MappingBuildError::new("密钥使用注册表锁不可用"))?;
        let mut additions = 0_usize;

        // 第一遍只校验，不修改记录和 entry。这样任意一项失败都不会留下半绑定的新快照。
        for ring in rings.values() {
            for entry in ring.usage_entries() {
                let identity = (
                    ring.tenant_scope.clone(),
                    ring.key_scope.clone(),
                    entry.kid.clone(),
                );
                if let Some(record) = records.get(&identity) {
                    if record.algorithm != entry.algorithm {
                        return Err(MappingBuildError::new("同一密钥身份在热更新中改变了算法"));
                    }
                    if record.retired && entry.state != KeyState::Retired {
                        return Err(MappingBuildError::new(
                            "已经 retired 的密钥身份不能重新启用",
                        ));
                    }
                    if entry.state != KeyState::Retired
                        && record.material_fingerprint != entry.material_fingerprint()
                    {
                        return Err(MappingBuildError::new("同一 kid 在热更新中替换了密钥材料"));
                    }
                    if entry.state != KeyState::Retired
                        && entry.max_invocations < record.max_invocations
                    {
                        return Err(MappingBuildError::new(
                            "热更新不能降低既有密钥身份的调用上限",
                        ));
                    }
                    if entry.state != KeyState::Retired
                        && record
                            .invocations
                            .load(std::sync::atomic::Ordering::Acquire)
                            >= entry.max_invocations
                    {
                        return Err(MappingBuildError::new("密钥累计调用次数已经达到新快照上限"));
                    }
                } else {
                    additions = additions.saturating_add(1);
                }
            }
        }
        if records.len().saturating_add(additions) > MAX_KEY_USAGE_IDENTITIES {
            return Err(MappingBuildError::new("密钥使用注册表身份数量超过上限"));
        }

        // 第二遍才提交。已有身份复用旧原子计数器；新身份登记当前 entry 的初始计数器。
        for ring in rings.values() {
            for entry in ring.usage_entries() {
                let identity = (
                    ring.tenant_scope.clone(),
                    ring.key_scope.clone(),
                    entry.kid.clone(),
                );
                if let Some(record) = records.get_mut(&identity) {
                    entry.attach_invocation_counter(record.invocations.clone());
                    record.max_invocations = record.max_invocations.max(entry.max_invocations);
                    if entry.state == KeyState::Retired {
                        record.retired = true;
                    }
                } else {
                    records.insert(
                        identity,
                        KeyUsageRecord {
                            algorithm: entry.algorithm,
                            material_fingerprint: entry.material_fingerprint(),
                            invocations: entry.invocation_counter(),
                            max_invocations: entry.max_invocations,
                            retired: entry.state == KeyState::Retired,
                        },
                    );
                }
            }
        }
        Ok(())
    }

    /// 返回当前持续记忆的公开密钥身份数量。
    ///
    /// # 返回
    ///
    /// 锁可用时返回记录数；锁异常时返回上限值，使调试摘要保持低敏感且不触发 panic。
    fn len(&self) -> usize {
        self.records
            .lock()
            .map(|records| records.len())
            .unwrap_or(MAX_KEY_USAGE_IDENTITIES)
    }
}

/// 构建不可变密码运行时所需的完整组件集合。
pub struct CryptoRuntimeConfig {
    /// 可由 route policy 显式选择的有限协议集合。
    pub protocols: Vec<Arc<dyn CryptoProtocol>>,
    /// 可由 route policy 显式选择的有限 provider 集合。
    pub providers: Vec<Arc<dyn WebCryptoProvider>>,
    /// 只读可信元数据 condition 集合。
    pub conditions: Vec<Arc<dyn CryptoCondition>>,
    /// 按已认证租户域和 route key scope 建立的有限密钥环。
    pub key_rings: Vec<Arc<KeyRing>>,
    /// 集群或单实例重放 guard 集合。
    pub replay_guards: Vec<Arc<dyn ReplayGuard>>,
    /// route 省略协议时使用的 ID；生产路由仍建议显式声明。
    pub default_protocol: Option<&'static str>,
    /// route 省略 provider 时使用的 ID。
    pub default_provider: Option<&'static str>,
    /// required replay route 使用的默认 guard ID。
    pub default_replay_guard: Option<&'static str>,
    /// public/匿名加密 route 使用的固定租户域。
    pub default_tenant_scope: Arc<str>,
    /// 单请求大小、JSON、时间窗与总 deadline 限制。
    pub limits: CryptoLimits,
    /// 全进程临时密码缓冲区预算。
    pub memory_budget: CryptoMemoryBudget,
    /// rid 原子占位保留时间。
    pub replay_ttl: Duration,
    /// 单次重放后端操作超时。
    pub replay_request_timeout: Duration,
}

/// 构建后不可变的协议、provider、condition、key ring 与重放注册表。
pub struct CryptoRuntime {
    protocols: HashMap<&'static str, Arc<dyn CryptoProtocol>>,
    providers: HashMap<&'static str, Arc<dyn WebCryptoProvider>>,
    conditions: HashMap<&'static str, Arc<dyn CryptoCondition>>,
    key_rings: HashMap<(Arc<str>, Arc<str>), Arc<KeyRing>>,
    replay_guards: HashMap<&'static str, Arc<dyn ReplayGuard>>,
    default_protocol: Option<&'static str>,
    default_provider: Option<&'static str>,
    default_replay_guard: Option<&'static str>,
    default_tenant_scope: Arc<str>,
    limits: Arc<CryptoLimits>,
    memory_budget: CryptoMemoryBudget,
    replay_ttl: Duration,
    replay_request_timeout: Duration,
    /// 通过 `ArcSwap` 允许待发布运行时原子接管当前进程的持续密钥使用状态。
    key_usage_registry: ArcSwap<KeyUsageRegistry>,
}

impl std::fmt::Debug for CryptoRuntime {
    /// 输出组件数量和公开限制，不输出密钥材料、旁路值或后端连接信息。
    ///
    /// # 参数
    ///
    /// - `formatter`: 调试输出目标。
    ///
    /// # 返回
    ///
    /// 返回脱敏格式化结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CryptoRuntime")
            .field("protocol_count", &self.protocols.len())
            .field("provider_count", &self.providers.len())
            .field("condition_count", &self.conditions.len())
            .field("key_ring_count", &self.key_rings.len())
            .field("replay_guard_count", &self.replay_guards.len())
            .field(
                "key_usage_identity_count",
                &self.key_usage_registry.load().len(),
            )
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl CryptoRuntime {
    /// 从经过 secret 合并的完整配置建立不可变密码运行时。
    ///
    /// # 参数
    ///
    /// - `config`: 应用启动期创建的全部协议、provider、condition、ring、guard 与资源限制。
    ///
    /// # 返回
    ///
    /// 组件 ID 唯一、默认项存在且 replay TTL 足够时返回运行时；任一步失败都拒绝发布快照。
    pub fn new(config: CryptoRuntimeConfig) -> Result<Self, MappingBuildError> {
        config.limits.validate()?;
        config.memory_budget.validate_limits(&config.limits)?;
        if config.default_tenant_scope.is_empty()
            || config.default_tenant_scope.len() > 128
            || config.replay_ttl.is_zero()
            || config.replay_request_timeout.is_zero()
            || config.replay_ttl > MAX_CRYPTO_RUNTIME_DURATION
            || config.replay_request_timeout > MAX_CRYPTO_RUNTIME_DURATION
        {
            return Err(MappingBuildError::new(
                "crypto tenant 或 replay 时间配置非法",
            ));
        }
        let minimum_replay_ttl = config
            .limits
            .clock_past_window
            .saturating_add(config.limits.clock_future_skew)
            .saturating_add(config.limits.max_request_duration);
        if config.replay_ttl < minimum_replay_ttl {
            return Err(MappingBuildError::new(
                "replay TTL 必须覆盖过去窗口、未来偏差与最长请求时间",
            ));
        }
        let protocols =
            collect_components(config.protocols, |value| value.id(), "crypto protocol")?;
        let providers =
            collect_components(config.providers, |value| value.id(), "crypto provider")?;
        let conditions =
            collect_components(config.conditions, |value| value.id(), "crypto condition")?;
        let replay_guards =
            collect_components(config.replay_guards, |value| value.id(), "replay guard")?;
        validate_default(
            &protocols,
            config.default_protocol,
            "default crypto protocol",
        )?;
        validate_default(
            &providers,
            config.default_provider,
            "default crypto provider",
        )?;
        validate_default(
            &replay_guards,
            config.default_replay_guard,
            "default replay guard",
        )?;
        let mut key_rings = HashMap::with_capacity(config.key_rings.len());
        for ring in config.key_rings {
            let identity = (ring.tenant_scope.clone(), ring.key_scope.clone());
            if key_rings.insert(identity, ring).is_some() {
                return Err(MappingBuildError::new("tenant/key scope 对应多个 key ring"));
            }
        }
        let key_usage_registry = Arc::new(KeyUsageRegistry::new());
        key_usage_registry.attach(&key_rings)?;
        Ok(Self {
            protocols,
            providers,
            conditions,
            key_rings,
            replay_guards,
            default_protocol: config.default_protocol,
            default_provider: config.default_provider,
            default_replay_guard: config.default_replay_guard,
            default_tenant_scope: config.default_tenant_scope,
            limits: Arc::new(config.limits),
            memory_budget: config.memory_budget,
            replay_ttl: config.replay_ttl,
            replay_request_timeout: config.replay_request_timeout,
            key_usage_registry: ArcSwap::new(key_usage_registry),
        })
    }

    /// 在快照发布前接管当前运行时的持续密钥调用次数与生命周期记录。
    ///
    /// # 参数
    ///
    /// - `previous`: 当前仍在服务旧请求的 last-good 密码运行时。
    ///
    /// # 返回
    ///
    /// 新快照全部密钥身份均兼容时返回空值并完成计数器绑定；失败时保持当前运行时不变，调用方
    /// 必须拒绝发布新快照。
    pub(crate) fn adopt_key_usage_registry(
        &self,
        previous: &Self,
    ) -> Result<(), MappingBuildError> {
        let registry = previous.key_usage_registry.load_full();
        registry.attach(&self.key_rings)?;
        self.key_usage_registry.store(registry);
        Ok(())
    }

    /// 在端口监听前检查一条加密路由的全部依赖和算法能力。
    ///
    /// # 参数
    ///
    /// - `route`: 过程宏生成的静态安全合同。
    ///
    /// # 返回
    ///
    /// 协议、provider、condition、key scope 与 replay 均可安全服务时返回空值，否则拒绝启动。
    pub fn audit(&self, route: &RoutePolicy) -> Result<(), MappingBuildError> {
        let protocol_id = route
            .crypto_protocol
            .or(self.default_protocol)
            .ok_or_else(|| {
                MappingBuildError::new(format!("路由 {} 缺少 crypto protocol", route.route_id))
            })?;
        let provider_id = route
            .crypto_provider
            .or(self.default_provider)
            .ok_or_else(|| {
                MappingBuildError::new(format!("路由 {} 缺少 crypto provider", route.route_id))
            })?;
        let _protocol = self.protocols.get(protocol_id).ok_or_else(|| {
            MappingBuildError::new(format!(
                "路由 {} 引用未注册 crypto protocol {}",
                route.route_id, protocol_id
            ))
        })?;
        let provider = self.providers.get(provider_id).ok_or_else(|| {
            MappingBuildError::new(format!(
                "路由 {} 引用未注册 crypto provider {}",
                route.route_id, provider_id
            ))
        })?;
        if let Some(condition_id) = route.crypto_condition {
            if !self.conditions.contains_key(condition_id) {
                return Err(MappingBuildError::new(format!(
                    "路由 {} 引用未注册 crypto condition {}",
                    route.route_id, condition_id
                )));
            }
            if condition_id == "legacy-disable-header" && protocol_id != "legacy-v1" {
                return Err(MappingBuildError::new(format!(
                    "路由 {} 只能在 legacy-v1 兼容路径使用 disable condition",
                    route.route_id
                )));
            }
        }
        let key_scope = route.crypto_key_scope.ok_or_else(|| {
            MappingBuildError::new(format!("路由 {} 缺少 crypto key scope", route.route_id))
        })?;
        let rings = self
            .key_rings
            .values()
            .filter(|ring| ring.key_scope.as_ref() == key_scope)
            .collect::<Vec<_>>();
        if rings.is_empty() {
            return Err(MappingBuildError::new(format!(
                "路由 {} 没有任何匹配 key ring",
                route.route_id
            )));
        }
        let may_use_default_tenant =
            route.auth != AuthRequirement::Required || route.auth_condition.is_some();
        if may_use_default_tenant
            && !rings
                .iter()
                .any(|ring| ring.tenant_scope == self.default_tenant_scope)
        {
            // 公开、可选认证或可被 condition 收窄为公开的路由可能没有 AuthContext，运行时会
            // 使用 default_tenant_scope。这里必须检查精确租户，而不能让其它租户的同 scope
            // 密钥环误导启动审计，否则服务会监听成功却让全部匿名加密请求在运行时失败。
            return Err(MappingBuildError::new(format!(
                "路由 {} 缺少默认 tenant 的匹配 key ring",
                route.route_id
            )));
        }
        let capabilities = provider.capabilities();
        if protocol_id == "modern-v2" {
            // modern-v2 响应复用请求建立的 rid 与 key 快照，没有独立响应上下文；只声明响应加密而
            // 不声明请求解密的路由会在运行期每个请求上 fail closed（endpoint composer 的
            // modern-response-without-request-context）。这里作为权威启动门禁拦下该组合，覆盖手工
            // 构造的 RoutePolicy，符合 不变量 12 与「审计通过才监听」。
            if route.crypto_response == crate::CryptoRequirement::Required
                && route.crypto_request != crate::CryptoRequirement::Required
            {
                return Err(MappingBuildError::new(format!(
                    "路由 {} 的 modern-v2 响应加密必须同时启用请求解密",
                    route.route_id
                )));
            }
            if !capabilities.modern_aead {
                return Err(MappingBuildError::new(format!(
                    "路由 {} 的 provider 不支持 modern-v2",
                    route.route_id
                )));
            }
            for ring in &rings {
                let active = ring.active_for_audit()?;
                if active.algorithm != KeyAlgorithm::A256Gcm {
                    return Err(MappingBuildError::new(format!(
                        "路由 {} 的 modern-v2 key ring 算法不是 A256GCM",
                        route.route_id
                    )));
                }
            }
        }
        if protocol_id == "legacy-v1" {
            for ring in rings {
                if ring
                    .usage_entries()
                    .filter(|entry| entry.state == KeyState::DecryptOnly)
                    .count()
                    > 1
                {
                    return Err(MappingBuildError::new(format!(
                        "路由 {} 的 legacy key ring 只能保留一个 decrypt-only key",
                        route.route_id
                    )));
                }
                let active = ring.active_for_audit()?;
                match active.algorithm {
                    KeyAlgorithm::LegacyAes if capabilities.legacy_aes => {}
                    KeyAlgorithm::LegacyRsa | KeyAlgorithm::LegacyRsaAes
                        if capabilities.legacy_private_operations => {}
                    _ => {
                        return Err(MappingBuildError::new(format!(
                            "路由 {} 的 legacy 算法能力被安全门禁拒绝",
                            route.route_id
                        )))
                    }
                }
            }
            if route.crypto_response == crate::CryptoRequirement::Required
                && route.response_contract != Some("base-response-v1")
            {
                return Err(MappingBuildError::new(format!(
                    "路由 {} 的 legacy 响应缺少 base-response-v1 合同",
                    route.route_id
                )));
            }
        }
        if route.replay == ReplayRequirement::Required {
            if protocol_id != "modern-v2" {
                return Err(MappingBuildError::new(format!(
                    "路由 {} 的协议没有可用于重放保护的 rid",
                    route.route_id
                )));
            }
            let guard_id = self.default_replay_guard.ok_or_else(|| {
                MappingBuildError::new(format!("路由 {} 缺少 replay guard", route.route_id))
            })?;
            if !self.replay_guards.contains_key(guard_id) {
                return Err(MappingBuildError::new(format!(
                    "路由 {} 的 replay guard 未注册",
                    route.route_id
                )));
            }
        }
        Ok(())
    }

    /// 返回当前运行时默认的匿名租户域。
    ///
    /// # 返回
    ///
    /// 返回 public 或匿名路由使用的固定域；认证路由应优先使用 [`crate::auth::AuthContext`] 中的 tenant。
    pub fn default_tenant_scope(&self) -> Arc<str> {
        self.default_tenant_scope.clone()
    }

    /// 返回固定快照的公开单请求限制。
    ///
    /// # 返回
    ///
    /// 返回共享限制值，调用方不得跨请求修改。
    pub fn limits(&self) -> Arc<CryptoLimits> {
        self.limits.clone()
    }

    /// 返回全进程共享的临时内存预算。
    ///
    /// # 返回
    ///
    /// 返回克隆句柄，所有请求仍竞争同一个加权 semaphore。
    pub fn memory_budget(&self) -> CryptoMemoryBudget {
        self.memory_budget.clone()
    }

    /// 解析 route 固定协议实现。
    ///
    /// # 参数
    ///
    /// - `route`: 已通过启动审计的静态路由合同。
    ///
    /// # 返回
    ///
    /// 返回共享协议；快照异常时 fail closed。
    pub fn protocol(&self, route: &RoutePolicy) -> Result<Arc<dyn CryptoProtocol>, CryptoError> {
        let id = route
            .crypto_protocol
            .or(self.default_protocol)
            .ok_or_else(|| {
                CryptoError::new(CryptoErrorKind::Policy, "crypto-protocol-unspecified")
            })?;
        self.protocols
            .get(id)
            .cloned()
            .ok_or_else(|| CryptoError::new(CryptoErrorKind::Policy, "crypto-protocol-missing"))
    }

    /// 解析 route 固定 provider 实现。
    ///
    /// # 参数
    ///
    /// - `route`: 已通过启动审计的静态路由合同。
    ///
    /// # 返回
    ///
    /// 返回共享 provider；不按请求 kid 做远程动态发现。
    pub fn provider(&self, route: &RoutePolicy) -> Result<Arc<dyn WebCryptoProvider>, CryptoError> {
        let id = route
            .crypto_provider
            .or(self.default_provider)
            .ok_or_else(|| {
                CryptoError::new(CryptoErrorKind::Policy, "crypto-provider-unspecified")
            })?;
        self.providers
            .get(id)
            .cloned()
            .ok_or_else(|| CryptoError::new(CryptoErrorKind::Policy, "crypto-provider-missing"))
    }

    /// 按已认证 tenant 和 route key scope 解析有限 key ring。
    ///
    /// # 参数
    ///
    /// - `tenant_scope`: auth context 或固定服务域提供的可信租户。
    /// - `route`: 已通过启动审计且声明 key scope 的路由合同。
    ///
    /// # 返回
    ///
    /// 精确匹配时返回 ring；不存在时不根据外部 kid 动态查找其它来源。
    pub fn key_ring(
        &self,
        tenant_scope: &str,
        route: &RoutePolicy,
    ) -> Result<Arc<KeyRing>, CryptoError> {
        let key_scope = route.crypto_key_scope.ok_or_else(|| {
            CryptoError::new(CryptoErrorKind::Policy, "crypto-key-scope-unspecified")
        })?;
        self.key_rings
            .get(&(Arc::from(tenant_scope), Arc::from(key_scope)))
            .cloned()
            .ok_or_else(|| CryptoError::new(CryptoErrorKind::Key, "crypto-key-ring-missing"))
    }

    /// 解析当前 route 使用的重放 guard。
    ///
    /// # 返回
    ///
    /// required route 返回共享 guard，关闭重放时返回 `None`；缺失配置 fail closed。
    pub fn replay_guard(
        &self,
        route: &RoutePolicy,
    ) -> Result<Option<Arc<dyn ReplayGuard>>, CryptoError> {
        if route.replay == ReplayRequirement::Disabled {
            return Ok(None);
        }
        let id = self
            .default_replay_guard
            .ok_or_else(|| CryptoError::new(CryptoErrorKind::Policy, "replay-guard-unspecified"))?;
        self.replay_guards
            .get(id)
            .cloned()
            .map(Some)
            .ok_or_else(|| CryptoError::new(CryptoErrorKind::Policy, "replay-guard-missing"))
    }

    /// 根据只读可信元数据收窄 route 声明的密码方向。
    ///
    /// # 参数
    ///
    /// - `route`: 已匹配路由的静态方向上限。
    /// - `input`: 不含 body 的请求视图。
    ///
    /// # 返回
    ///
    /// 返回声明方向的子集；condition 缺失、错误或尝试扩张都会 fail closed。
    pub async fn evaluate_condition(
        &self,
        route: &RoutePolicy,
        input: CryptoConditionInput<'_>,
    ) -> Result<CryptoDirections, CryptoError> {
        let declared = CryptoDirections::declared(route);
        let Some(id) = route.crypto_condition else {
            return Ok(declared);
        };
        let condition = self
            .conditions
            .get(id)
            .ok_or_else(|| CryptoError::new(CryptoErrorKind::Policy, "crypto-condition-missing"))?;
        let effective = condition.evaluate(input, declared).await?;
        if (effective.request && !declared.request) || (effective.response && !declared.response) {
            return Err(CryptoError::new(
                CryptoErrorKind::Policy,
                "crypto-condition-expanded",
            ));
        }
        Ok(effective)
    }

    /// 返回重放占位 TTL。
    ///
    /// # 返回
    ///
    /// 返回已经过启动关系校验的保留时间。
    pub const fn replay_ttl(&self) -> Duration {
        self.replay_ttl
    }

    /// 返回单次重放后端操作超时。
    ///
    /// # 返回
    ///
    /// 返回小于请求总 deadline 的后端调用预算。
    pub const fn replay_request_timeout(&self) -> Duration {
        self.replay_request_timeout
    }

    /// 探测当前快照中 required 路由依赖的共享重放后端。
    ///
    /// # 参数
    ///
    /// - `routes`: 与当前运行时快照一起完成启动审计的完整静态路由集合。
    ///
    /// # 返回
    ///
    /// 没有 required replay 路由或后端探测成功时返回空值；后端缺失、超时或不可用时返回不含
    /// 连接细节的构建错误，使 readiness 关闭流量但不销毁 last-good 快照。
    pub async fn readiness(&self, routes: &[RoutePolicy]) -> Result<(), MappingBuildError> {
        if !routes
            .iter()
            .any(|route| route.replay == ReplayRequirement::Required)
        {
            return Ok(());
        }
        let guard_id = self
            .default_replay_guard
            .ok_or_else(|| MappingBuildError::new("required replay guard 未配置"))?;
        let guard = self
            .replay_guards
            .get(guard_id)
            .ok_or_else(|| MappingBuildError::new("required replay guard 未注册"))?;
        tokio::time::timeout(self.replay_request_timeout, guard.readiness())
            .await
            .map_err(|_| MappingBuildError::new("required replay guard 就绪探测超时"))?
            .map_err(|_| MappingBuildError::new("required replay guard 当前不可用"))
    }
}

/// 把对象安全组件列表收敛为唯一 ID 注册表。
///
/// # 类型参数
///
/// - `T`: 可通过 trait object 共享的组件类型。
/// - `F`: 从组件引用读取静态 ID 的函数类型。
///
/// # 参数
///
/// - `values`: 应用启动期创建的有限组件列表。
/// - `id_of`: 不读取 secret 的静态 ID 提取函数。
/// - `kind`: 启动错误使用的组件类别。
///
/// # 返回
///
/// ID 合法且唯一时返回不可变 map，否则拒绝启动。
fn collect_components<T: ?Sized, F>(
    values: Vec<Arc<T>>,
    id_of: F,
    kind: &str,
) -> Result<HashMap<&'static str, Arc<T>>, MappingBuildError>
where
    F: Fn(&T) -> &'static str,
{
    let mut output = HashMap::with_capacity(values.len());
    for value in values {
        let id = id_of(value.as_ref());
        if !valid_component_id(id) || output.insert(id, value).is_some() {
            return Err(MappingBuildError::new(format!("{kind} ID 非法或重复")));
        }
    }
    Ok(output)
}

/// 校验可公开的静态组件 ID。
///
/// # 参数
///
/// - `id`: provider、protocol、condition 或 guard 的静态标识。
///
/// # 返回
///
/// 非空、至多 64 字节且只含安全 ASCII 字符时返回 `true`。
fn valid_component_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

/// 校验默认组件 ID 确实存在于对应注册表。
///
/// # 类型参数
///
/// - `T`: 注册表中的共享组件类型。
///
/// # 参数
///
/// - `map`: 已完成唯一性校验的组件注册表。
/// - `id`: 可选默认 ID。
/// - `kind`: 启动错误中的默认项类别。
///
/// # 返回
///
/// 未配置或已注册时返回空值；悬空默认 ID 会拒绝启动。
fn validate_default<T: ?Sized>(
    map: &HashMap<&'static str, Arc<T>>,
    id: Option<&'static str>,
    kind: &str,
) -> Result<(), MappingBuildError> {
    if id.is_some_and(|value| !map.contains_key(value)) {
        Err(MappingBuildError::new(format!("{kind} 未注册")))
    } else {
        Ok(())
    }
}
