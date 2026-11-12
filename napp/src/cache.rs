//! 两级缓存组件。
//!
//! `CacheComponent` 把 `cacheable` 的两级缓存(L1 moka + L2 Redis 三防 + 跨实例失效广播)做成配置驱动、
//! 由容器托管生命周期的能力:Start 按 `cache.*` 建 L2 后端与失效广播并经 [`CacheRuntimeGuard`] 收拢为
//! 单一拥有式句柄(它把 backend 装进进程级 `CacheRuntime`,`#[cached]`/`#[cache_invalidate]` 展开代码据此
//! 工作),同时压入 Stopping action;停机时排空并停止失效广播、join drainer/subscriber。
//!
//! 组件顺序固定为 `redis -> cache -> kafka/web`。cache **不把 auto 强制成 Service**
//! Start 与停机在 Batch 也执行,故管道两种模式都能建立并在退出前停机。
//!
//! L2 后端两种取法:`cache.redis_ref`(如 `default`)**复用 `redis` 组件的托管连接**
//! (经 nadis adapter [`RedisClientBackend`],不新开连接),或 `cache.redis_url` **自建** Redis Cluster 连接;二者
//! 互斥。`redis_ref` 依赖 `redis` 组件已声明并在 `redis -> cache` 序中先发布 typed handle(未声明/未就绪即报错)。
//! scene 策略在 Start 审计；宏入口由带 generation/owner 的可撤销 runtime 承载。旧 guard 只能撤销
//! 自己安装的一代，不能清空随后安装的新一代。

use std::sync::Arc;

use serde::Deserialize;

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, ShutdownAction, ShutdownContext,
    StartContext,
};

/// nadis adapter:把**受管** `RedisClient` 适配成 cacheable 的 [`cacheable::cache::CacheBackend`]。
///
/// 使 `redis_ref: default` 的 L2 命令(GET/PSETEX/DEL)直接走 `redis` 组件已建立的连接,而非新开一条集群连接
/// ——`CacheLayer` 的 single-flight/TTL/serde 不变,只换连接来源。仅在启用 `redis` 组件时可用。
#[cfg(feature = "redis")]
struct RedisClientBackend {
    /// `redis` 组件在 Start 发布、经资源容器借出的受管客户端句柄(clone 廉价)。
    client: Arc<nadis::RedisClient>,
}

#[cfg(feature = "redis")]
#[async_trait::async_trait]
impl cacheable::cache::CacheBackend for RedisClientBackend {
    /// 业务作用：经受管客户端执行只读 `PING` 健康探针。
    ///
    /// 参数说明：无。
    ///
    /// 返回：Redis 探针成功时为 Ok，连接或命令错误向上传播；不读取缓存值，也不证明广播订阅健康。
    async fn health_check(&self) -> anyhow::Result<()> {
        self.client.ping().await?;
        Ok(())
    }

    /// 业务作用：经受管客户端读一个 key(GET)。
    ///
    /// 参数说明：
    /// - `key`: 完整缓存 key。
    ///
    /// 返回：存在时返回原始字符串，缺失时为 None，Redis 或字符串解码错误向上传播；不反序列化业务对象或执行回源。
    async fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        Ok(self.client.get::<String>(key).await?)
    }

    /// 业务作用：经受管客户端以毫秒 TTL 写一个 key(PSETEX)。
    ///
    /// 参数说明：
    /// - `key`: 完整缓存 key。
    /// - `value`: 已序列化的 JSON 载荷。
    /// - `ttl_ms`: 过期毫秒数。
    ///
    /// 返回：Redis 写入成功时为 Ok，连接、命令或过期参数错误向上传播；不解析载荷，不同步失效其它实例的 L1。
    async fn set(&self, key: &str, value: &str, ttl_ms: u64) -> anyhow::Result<()> {
        self.client
            .set_ttl(key, value, std::time::Duration::from_millis(ttl_ms))
            .await?;
        Ok(())
    }

    /// 业务作用：经受管客户端删一个 key(DEL)。
    ///
    /// 参数说明：
    /// - `key`: 完整缓存 key。
    ///
    /// 返回：DEL 成功时为 Ok，目标已不存在也视为成功；Redis 错误传播给调用方，不在此广播 L1 失效。
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.client.del(&[key]).await?;
        Ok(())
    }
}

/// 正常缓存值基础 TTL 缺省秒数。
const DEFAULT_CACHE_TTL_SECS: u64 = 300;
/// 空结果哨兵 TTL 缺省秒数(防穿透)。
const DEFAULT_NULL_TTL_SECS: u64 = 30;
/// Redis PSETEX 使用有符号 64 位毫秒；正常 TTL 还需为最多 1000ms 抖动留空间。
const MAX_CACHE_TTL_SECS: u64 = ((i64::MAX as u64) - 1000) / 1000;
/// 缓存后端健康 monitor 的固定间隔。
const CACHE_MONITOR_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
/// 超过三个 monitor 周期没有新观测时，把缓存标记为 stale。
const CACHE_STALE_AFTER: std::time::Duration = std::time::Duration::from_secs(15);

impl Application {
    /// 业务作用：为显式持久失效记录提供标准受管策略入口，保持调用方事务上下文。
    ///
    /// 参数说明：
    /// - `sink`：在调用方事务内记录失效意图的业务策略；实际缓存失效与投递由该策略的后续流程承担。
    ///
    /// 返回：UserHook 仍开放、缓存已声明且启用、策略唯一时安装并托管 owner；配置或冲突错误不替换已有策略。
    /// 安装后若资源登记失败，由 Drop 关闭本次入口；成功登记不表示任何失效记录已执行。
    pub fn configure_cache_invalidation_sink(
        &self,
        sink: Arc<dyn cacheable::DurableInvalidationSink>,
    ) -> ApplicationResult<()> {
        self.ensure_user_hook_open("cache invalidation strategy")?;
        self.ensure_component_declared(
            ComponentId::Cache,
            ApplicationPhase::UserHook,
            "cache invalidation strategy",
        )?;
        if read_cache_config(self)?.mode == CacheMode::Disabled {
            return Err(cache_error(
                ApplicationPhase::UserHook,
                "cache invalidation strategy requires an enabled cache",
            ));
        }
        let owner = cacheable::DurableInvalidationOwner::install(sink).map_err(|error| {
            cache_error_src(
                ApplicationPhase::UserHook,
                "durable invalidation strategy conflicts with an existing owner",
                error,
            )
        })?;
        self.register_managed(DurableSinkResource(owner))
    }
}

/// 将持久失效策略的关闭与在途等待纳入宿主资源生命周期。
/// 字段 `0` 持有本组件独占的策略 owner，释放时关闭该代记录入口。
struct DurableSinkResource(cacheable::DurableInvalidationOwner);

impl crate::ManagedResource for DurableSinkResource {
    /// 业务作用：撤销持久意图准入并等待在途记录释放事务依赖。
    ///
    /// 参数说明：
    /// - `context`：携带宿主共享截止点，用于限制等待在途持久记录释放策略的时间。
    ///
    /// 返回：关闭新记录准入并等到所有在途调用释放后成功；超时返回 Stopping 错误，入口保持关闭，不回滚已记录的意图。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            tokio::time::timeout_at(context.deadline().into(), self.0.shutdown())
                .await
                .map_err(|_| {
                    cache_error(
                        ApplicationPhase::Stopping,
                        "durable invalidation calls did not drain",
                    )
                })?;
            Ok(())
        })
    }
}

/// 缓存组件负责读取的顶层配置根投影。
#[derive(Default, Deserialize)]
#[serde(default)]
struct CacheConfigRoot {
    /// 可选缓存配置段；声明组件却缺少该段时拒绝启动。
    cache: Option<CacheConfig>,
}

/// 缓存工作模式。
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CacheMode {
    /// 显式关闭:组件成为无副作用空操作(不建连、不装 backend、不起广播)。
    #[default]
    Disabled,
    /// 仅在本进程缓存并托管有界刷新，不建立 L2 或广播连接。
    Local,
    /// 两级缓存:L1 moka + L2 Redis 三防(+ 可选跨实例失效广播)。
    TwoLevel,
}

/// `cache` 配置段。`deny_unknown_fields`:拼写错误在建立任何连接前即被拒。
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheConfig {
    /// 工作模式;缺省 `disabled`(声明组件但显式不启用两级缓存)。
    #[serde(default)]
    mode: CacheMode,
    /// 复用**受管** Redis 实例的 qualifier(如 `default`);设置即经 nadis adapter 复用 `redis` 组件连接。
    /// 与 `redis_url` **互斥**:`two_level` 必须给其一。
    #[serde(default)]
    redis_ref: Option<String>,
    /// L2 Redis Cluster 连接串(逗号分隔多节点);未配 `redis_ref` 时 `two_level` 用它**自建**连接。
    #[serde(default)]
    redis_url: String,
    /// 正常缓存值基础 TTL 秒数;缺省 300。
    #[serde(default = "default_cache_ttl_secs")]
    cache_ttl_secs: u64,
    /// 空结果哨兵 TTL 秒数;缺省 30。
    #[serde(default = "default_null_ttl_secs")]
    null_ttl_secs: u64,
    /// 跨实例 L1 失效广播配置。
    #[serde(default)]
    invalidation: InvalidationConfig,
}

/// L1 失效广播(Redis pub/sub)配置。缺省 = 关闭 + 空连接串(仅本地 L1 失效)。
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct InvalidationConfig {
    /// 是否启用跨实例失效广播;缺省关(仅本地 L1 失效)。
    enabled: bool,
    /// 复用受管 Redis 实例的 qualifier；与 `redis_url` 互斥。
    redis_ref: Option<String>,
    /// 广播用 Redis 连接串(pub/sub,单节点即可)；未配 `redis_ref` 时使用。
    redis_url: String,
}

/// 业务作用：正常缓存值 TTL 缺省。
///
/// 参数说明：无。
///
/// 返回：300 秒的基础存活时间，供未配置该字段时构造缓存层；不改变已存在的 Redis key。
fn default_cache_ttl_secs() -> u64 {
    DEFAULT_CACHE_TTL_SECS
}

/// 业务作用：空哨兵 TTL 缺省。
///
/// 参数说明：无。
///
/// 返回：30 秒的空结果存活时间，用于短期抑制重复回源，不代表正常缓存值的 TTL。
fn default_null_ttl_secs() -> u64 {
    DEFAULT_NULL_TTL_SECS
}

impl CacheConfig {
    /// 业务作用：在缓存装配前检查具名资源语法、两级缓存唯一连接来源、TTL 与广播来源约束。
    ///
    /// 参数说明：
    ///
    /// - `phase`:本次校验所属生命周期阶段,用于错误归因。
    ///
    /// 返回：适用于当前模式的约束满足时成功，否则带指定阶段的配置错误；禁用和本地模式仍检查引用语法及广播来源，
    /// 但不检查两级 TTL，不解析 URL、不确认具名 Redis 已发布，也不执行 scene 审计。
    fn validate(&self, phase: ApplicationPhase) -> ApplicationResult<()> {
        let has_ref = self
            .redis_ref
            .as_deref()
            .is_some_and(|r| !r.trim().is_empty());
        let has_url = !self.redis_url.trim().is_empty();
        if let Some(reference) = self.redis_ref.as_deref() {
            validate_redis_reference(reference, "cache.redis_ref", phase)?;
        }
        if self.mode == CacheMode::TwoLevel {
            // 二者必须给其一,且互斥(同时给无法确定用托管还是自建)。
            if !has_ref && !has_url {
                return Err(cache_error(
                    phase,
                    "cache.mode is two_level but neither cache.redis_ref nor cache.redis_url is set",
                ));
            }
            if has_ref && has_url {
                return Err(cache_error(
                    phase,
                    "cache.redis_ref and cache.redis_url are mutually exclusive (choose managed reuse or a self-owned connection)",
                ));
            }
            if self.cache_ttl_secs == 0
                || self.null_ttl_secs == 0
                || self.cache_ttl_secs > MAX_CACHE_TTL_SECS
                || self.null_ttl_secs > MAX_CACHE_TTL_SECS
            {
                return Err(cache_error(
                    phase,
                    "cache TTL values must be positive and fit Redis millisecond expiration",
                ));
            }
        }
        let invalidation_has_ref = self
            .invalidation
            .redis_ref
            .as_deref()
            .is_some_and(|reference| !reference.trim().is_empty());
        let invalidation_has_url = !self.invalidation.redis_url.trim().is_empty();
        if let Some(reference) = self.invalidation.redis_ref.as_deref() {
            validate_redis_reference(reference, "cache.invalidation.redis_ref", phase)?;
        }
        if invalidation_has_ref && invalidation_has_url {
            return Err(cache_error(
                phase,
                "cache.invalidation.redis_ref and cache.invalidation.redis_url are mutually exclusive",
            ));
        }
        if self.invalidation.enabled && !invalidation_has_ref && !invalidation_has_url {
            return Err(cache_error(
                phase,
                "cache invalidation requires exactly one of cache.invalidation.redis_ref or cache.invalidation.redis_url",
            ));
        }
        Ok(())
    }
}

/// 业务作用：在任何缓存网络 I/O 前校验 Redis 资源引用可作为有界 qualifier。
///
/// 参数说明：
/// - `reference`: 配置边界传入的受管 Redis 名称。
/// - `field`: 用于脱敏定位的配置字段名。
/// - `phase`: 校验所属生命周期阶段。
///
/// 返回：名称符合 Redis qualifier 合同时成功；空白、超长或非法字符返回组件错误。
fn validate_redis_reference(
    reference: &str,
    field: &str,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    if reference.trim().is_empty()
        || reference.trim() != reference
        || reference.len() > 256
        || !reference
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'.' | b'_' | b'-'))
    {
        return Err(cache_error(
            phase,
            format!("{field} must be a bounded canonical Redis qualifier"),
        ));
    }
    Ok(())
}

/// 缓存组件:Start 建两级缓存管道并装入进程级 runtime,停机排空失效广播。
pub(crate) struct CacheComponent {
    /// Start 成功后保存的最终模式与连接配置，不在健康探针中重新读取。
    config: Option<CacheConfig>,
    /// Ready 后交由 Runner 监督的后端健康 monitor。
    critical_task: Option<ApplicationFuture<'static>>,
}

impl CacheComponent {
    /// 业务作用：创建尚未读取配置的缓存组件。
    ///
    /// 参数说明：无。
    ///
    /// 返回：没有配置快照和监督任务的组件，不连接 Redis 或发布缓存运行时；实际模式在 Start 确定。
    pub(crate) fn new() -> Self {
        Self {
            config: None,
            critical_task: None,
        }
    }
}

/// 业务作用：周期探测当前 generation 的 L2 后端并刷新非关键 readiness。
///
/// Cache miss 可以回源，因此运行期后端失败只降级而不摘流；启动期无法建立后端仍然 fail closed。
///
/// 参数说明：
/// - `application`：提供应用状态变更通知，启动未完成时等待，停机或失败时终止探测。
/// - `handle`：进程级只读缓存入口，每次探针固定当时的后端 Arc；不持有某一代运行时的所有权或停机权限。
/// - `contributor`：不影响整体就绪准入的缓存观测槽位，记录健康、降级和停止状态。
///
/// 返回：观察到停机或失败时标记 NotReady 并正常结束；Ready 期间每轮探针至多等待五秒，失败仅记录 Degraded，
/// 轮次间等待同样可被应用状态变更打断，不在此重连 Redis 或修订缓存配置。
async fn run_cache_monitor(
    application: Application,
    handle: cacheable::CacheHandle,
    contributor: ReadinessContributor,
) -> ApplicationResult<()> {
    let mut states = application.subscribe_state();
    loop {
        let state = *states.borrow_and_update();
        match state {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                contributor.observe(
                    DependencyState::NotReady,
                    reason::NOT_READY,
                    std::time::Instant::now(),
                );
                return Ok(());
            }
            ApplicationState::Starting => {
                let _ = states.changed().await;
                continue;
            }
            ApplicationState::Ready => {}
        }
        let now = std::time::Instant::now();
        // 后端客户端本身通常带命令超时，这里仍施加 monitor 级上限，避免错误配置或驱动缺陷让
        // 单次探针永久占住监督任务，导致 readiness 只能依赖 stale 被动翻转且停机无法及时收束。
        // 探针只读且可取消；停机状态到达后应将预算留给在途缓存工作。
        let result = tokio::select! {
            biased;
            _ = states.changed() => continue,
            result = tokio::time::timeout(CACHE_MONITOR_INTERVAL, handle.health_check()) => result,
        };
        match result {
            Ok(Ok(())) => contributor.observe(DependencyState::Ready, reason::HEALTHY, now),
            Ok(Err(_)) | Err(_) => {
                contributor.observe(DependencyState::Degraded, reason::DEGRADED, now)
            }
        }
        tokio::select! {
            biased;
            _ = states.changed() => {},
            _ = tokio::time::sleep(CACHE_MONITOR_INTERVAL) => {},
        }
    }
}

impl ApplicationComponent for CacheComponent {
    /// 业务作用：返回缓存组件稳定身份。
    ///
    /// 参数说明：无。
    ///
    /// 返回：固定的 ComponentId::Cache，供生命周期排序与错误归类，不反映缓存是否已启用或健康。
    fn id(&self) -> ComponentId {
        ComponentId::Cache
    }

    /// 业务作用：按配置审计 scene 并装配禁用、本地或两级缓存，将启用模式的所有权交给停机动作。
    ///
    /// 管道在 Start 建立(而非 Ready):Batch 不执行 Ready,但 Start 与停机都执行,故两种模式都能建立
    /// 并在退出前停机。装入进程级 `CacheRuntime` 早于流量入口 Ready,`#[cached]` 首次命中即可用 L2。
    ///
    /// 参数说明：
    ///
    /// - `context`:提供最终配置与 active stack 的 Start 上下文。
    ///
    /// 返回：配置和 scene 审计通过后，禁用模式不建缓存资源，本地模式托管刷新任务，两级模式安装运行时并登记非关键健康观测。
    /// 失败返回 Start 错误；两级运行时先安装再登记观测，登记失败时由局部 guard 撤销其所属代，不能据此保证它从未短暂可见。
    /// 健康任务在此仅保存，实际启动由 Runner 后续接管。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let config = read_cache_config(context.application())?;
            config.validate(ApplicationPhase::Start)?;
            // scene 一致性审计:同名 scene 的 `#[cached]` 值类型/TTL 合同不一致属于代码合同冲突,前移到
            // 启动期拒绝(否则运行期 L1 downcast panic)。纯编译期 descriptor 比对,无网络/无 backend 依赖,
            // 故无论 mode 是否 disabled 都先审计——声明了 cache 组件即对本进程全部 scene 负责。
            cacheable::audit_scenes().map_err(|error| {
                cache_error_src(ApplicationPhase::Start, "cache scene audit failed", error)
            })?;
            if config.mode == CacheMode::Disabled {
                tracing::info!("cache component is disabled by configuration; no cache pipeline");
                self.config = Some(config);
                return Ok(());
            }
            if config.mode == CacheMode::Local {
                let guard = cacheable::LocalCacheRuntimeGuard::install().map_err(|error| {
                    cache_error_src(
                        ApplicationPhase::Start,
                        "local cache owner is already installed",
                        error,
                    )
                })?;
                context.activate(Box::new(LocalCacheShutdown(guard)));
                self.config = Some(config);
                return Ok(());
            }

            // 装配 + 拥有:guard 把 backend 装进进程级 CacheRuntime(供 #[cached] 展开代码使用),
            // 并(可选)启动可 join 的失效广播;其停机由下面的 action 拥有。
            let guard = build_cache_runtime(context.application(), &config)
                .await
                .map_err(|error| {
                    cache_error_src(
                        ApplicationPhase::Start,
                        "cache invalidation broadcast start failed",
                        error,
                    )
                })?;
            // 运行时此时已经安装；登记失败须由局部 guard 撤销本代，避免遗留失去宿主的全局入口。
            let contributor = context.application().register_readiness(
                ComponentId::Cache,
                Arc::<str>::from("cache:l2"),
                ReadinessPolicy {
                    affects_ready: false,
                    failure_threshold: 1,
                    recovery_threshold: 1,
                    stale_after: Some(CACHE_STALE_AFTER),
                },
            )?;
            contributor.observe(
                DependencyState::Ready,
                reason::HEALTHY,
                std::time::Instant::now(),
            );
            self.critical_task = Some(Box::pin(run_cache_monitor(
                context.application().clone(),
                cacheable::cache_handle(),
                contributor,
            )));
            context.activate(Box::new(CacheShutdown {
                guard: Some(guard),
            }));
            tracing::info!(
                "cache component installed two-level runtime (broadcast={})",
                config.invalidation.enabled
            );
            self.config = Some(config);
            Ok(())
        })
    }

    /// 业务作用：取出缓存后端健康 monitor，交给 Runner 在 Ready 后监督。
    ///
    /// 参数说明：无。
    ///
    /// 返回：两级缓存已装配时首次移交命名任务；禁用、本地模式、尚未装配或已移交时为 None，不在此执行探针。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("cache-health-monitor", task))
    }
}

/// 业务作用：先复验广播的具名资源，再建立 L2 并装配缓存运行时。
///
/// 参数说明：
/// - `application`: 提供具名 Redis 资源的应用容器。
/// - `config`: 已完成无副作用校验的缓存配置。
///
/// 返回：L2 和可选广播装配完成、运行时已安装时返回拥有式句柄；具名资源缺失、feature 不足、建连或所有权冲突时失败。
/// 广播复用受管发布客户端仍会派生专用订阅连接；返回值不证明未来广播送达或 L2 持续健康。
async fn build_cache_runtime(
    application: &Application,
    config: &CacheConfig,
) -> anyhow::Result<cacheable::CacheRuntimeGuard> {
    let managed_reference = config
        .invalidation
        .redis_ref
        .as_deref()
        .filter(|reference| !reference.trim().is_empty());
    if config.invalidation.enabled {
        if let Some(reference) = managed_reference {
            #[cfg(feature = "redis")]
            {
                // 先复验引用再建立可能自有连接的 L2，未知 source 不产生任何缓存侧网络副作用。
                let client = crate::redis::redis_handle(application, reference)
                    .await
                    .map_err(anyhow::Error::new)?;
                let layer = build_cache_layer(application, config)
                    .await
                    .map_err(anyhow::Error::new)?;
                return cacheable::CacheRuntimeGuard::start_with_managed_redis(layer, client).await;
            }
            #[cfg(not(feature = "redis"))]
            {
                let _ = application;
                anyhow::bail!(
                    "cache.invalidation.redis_ref=`{reference}` requires the `redis` feature/component"
                );
            }
        }
    }
    let layer = build_cache_layer(application, config)
        .await
        .map_err(anyhow::Error::new)?;
    let broadcast_url = config
        .invalidation
        .enabled
        .then_some(config.invalidation.redis_url.as_str());
    cacheable::CacheRuntimeGuard::start(layer, broadcast_url).await
}

/// 本地缓存停机动作，负责关闭刷新准入并按宿主期限等待任务退出。
/// 字段 `0` 持有仅本地模式的任务 owner，不持有 Redis 广播资源。
struct LocalCacheShutdown(cacheable::LocalCacheRuntimeGuard);

impl ShutdownAction for LocalCacheShutdown {
    /// 业务作用：标识纯本地缓存任务清理。
    ///
    /// 参数说明：无。
    ///
    /// 返回：固定动作名称。
    fn label(&self) -> &'static str {
        "local-cache"
    }

    /// 业务作用：撤销刷新准入并等待全部本地任务退出。
    ///
    /// 参数说明：
    /// - `context`：携带统一停机截止点，本地任务等待不能另起完整超时预算。
    ///
    /// 返回：通知取消并等到任务实际退出时成功；到达截止点返回 Stopping 错误，准入保持关闭，不把取消通知视为退出证明。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            tokio::time::timeout_at(context.deadline().into(), self.0.shutdown())
                .await
                .map_err(|_| {
                    cache_error(
                        ApplicationPhase::Stopping,
                        "local cache refresh tasks did not stop",
                    )
                })?;
            Ok(())
        })
    }
}

/// 两级缓存停机动作；异步等待受 Runner 的剩余停机预算限制。
struct CacheShutdown {
    /// 尚未移交停机流程的运行时 owner，取出后不能再次关闭同一实例。
    guard: Option<cacheable::CacheRuntimeGuard>,
}

impl ShutdownAction for CacheShutdown {
    /// 业务作用：返回清理报告使用的稳定动作名称。
    ///
    /// 参数说明：无。
    ///
    /// 返回：固定的 cache-runtime 标签，用于停机记录，不含连接地址、凭据或缓存内容。
    fn label(&self) -> &'static str {
        "cache-runtime"
    }

    /// 业务作用：消费本组件的运行时所有权，先结束本地刷新，再尽力排空广播并停止后台任务。
    ///
    /// 参数说明：
    ///
    /// - `_context`:共享停机预算;等待上限由 Runner 的整体 timeout 约束。
    ///
    /// 返回：首次调用在 guard 停机完成后成功，后续调用直接成功；不报告各条广播是否送达。
    /// 等待被外层取消时，guard 的 Drop 撤销本代运行时并中止剩余任务，不能保证已经排空。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            if let Some(guard) = self.guard.take() {
                guard.shutdown().await;
            }
            Ok(())
        })
    }
}

/// 业务作用：按 `cache` 配置构造 L2 `CacheLayer`:`redis_ref` 复用受管连接,否则自建集群连接。
///
/// 复用路径经 [`RedisClientBackend`] 把受管 `RedisClient` 适配成 `CacheBackend`,不新开集群连接;受管实例
/// 未声明或未就绪时返回错误；自有来源经 `connect_cluster` 建立独立 Redis Cluster 连接。
///
/// 参数说明：
///
/// - `application`:Start 上下文的应用句柄,复用路径据此借出受管 Redis 客户端。
/// - `config`:已校验的 `two_level` 缓存配置。
///
/// 返回：取得具名客户端或建立自有连接后返回带两类 TTL 的缓存层，不安装全局运行时或广播。
/// 具名资源缺失、redis feature 未启用或自有连接失败时返回组件错误，底层原因保留在 source 中等待统一脱敏。
async fn build_cache_layer(
    application: &Application,
    config: &CacheConfig,
) -> ApplicationResult<Arc<cacheable::cache::CacheLayer>> {
    if let Some(reference) = config.redis_ref.as_deref().filter(|r| !r.trim().is_empty()) {
        // 复用受管 Redis:L2 命令走 `redis` 组件连接,不新开集群连接。
        #[cfg(feature = "redis")]
        {
            let client = crate::redis::redis_handle(application, reference)
                .await
                .map_err(|error| {
                    cache_error_src(
                        ApplicationPhase::Start,
                        format!(
                            "cache.redis_ref points at managed redis instance `{reference}`, but no such ready redis component is declared"
                        ),
                        error,
                    )
                })?;
            let backend: Arc<dyn cacheable::cache::CacheBackend> = Arc::new(RedisClientBackend {
                client,
            });
            tracing::info!(
                "cache L2 reuses managed redis instance `{reference}` (no dedicated connection)"
            );
            return Ok(Arc::new(cacheable::cache::CacheLayer::with_backend(
                backend,
                config.cache_ttl_secs,
                config.null_ttl_secs,
            )));
        }
        #[cfg(not(feature = "redis"))]
        {
            let _ = application;
            return Err(cache_error(
                ApplicationPhase::Start,
                format!(
                    "cache.redis_ref=`{reference}` requires the `redis` feature/component, which is not enabled"
                ),
            ));
        }
    }
    // 自建 Redis Cluster 连接(未配 redis_ref)。
    let connection = cacheable::connect_cluster(&config.redis_url)
        .await
        .map_err(|error| {
            cache_error_src(
                ApplicationPhase::Start,
                "cache L2 Redis connect failed",
                error,
            )
        })?;
    Ok(Arc::new(cacheable::cache::CacheLayer::new(
        connection,
        config.cache_ttl_secs,
        config.null_ttl_secs,
    )))
}

/// 业务作用：从最终配置读取 `cache` 段;缺失该段却声明了组件时报明确错误。
///
/// 参数说明：
///
/// - `application`:提供当前不可变配置快照的共享上下文。
///
/// 返回：从一次配置快照解析的 CacheConfig；缺少 cache 段、未知字段或字段类型错误时返回 Start 错误，
/// 不执行连接来源、TTL 或 scene 校验，也不建立连接。
fn read_cache_config(application: &Application) -> ApplicationResult<CacheConfig> {
    let snapshot = application.config();
    let root: CacheConfigRoot =
        serde_json::from_value((*snapshot.value()).clone()).map_err(|error| {
            cache_error_src(
                ApplicationPhase::Start,
                "invalid `cache` configuration section",
                error,
            )
        })?;
    root.cache.ok_or_else(|| {
        cache_error(
            ApplicationPhase::Start,
            "component `cache` is declared but the `cache` configuration section is missing",
        )
    })
}

/// 业务作用：在不建立任何连接的前提下校验候选配置树中的 `cache` 段。
///
/// 参数说明：
///
/// - `tree`:合并、插值完成但尚未发布的候选配置树。
/// - `phase`:本次无副作用校验所属的生命周期阶段。
///
/// 返回：段不存在时成功；存在时须可解析且满足 CacheConfig 的模式、来源和预算约束，失败带指定阶段。
/// 不验证远端连通性、具名资源存在性或 scene 一致性，不发布候选配置。
pub(crate) fn validate_cache_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let Some(section) = tree.get("cache") else {
        return Ok(());
    };
    let config: CacheConfig = serde_json::from_value(section.clone())
        .map_err(|error| cache_error_src(phase, "invalid `cache` configuration section", error))?;
    config.validate(phase)
}

/// 业务作用：创建缓存组件的稳定生命周期错误。
///
/// 参数说明：
///
/// - `phase`:故障被观察到的生命周期阶段。
/// - `message`:不含缓存 key、value 或连接串的稳定摘要。
///
/// 返回：归属于 Cache 与指定阶段、没有底层 source 的 ApplicationError；保留传入摘要，不自动脱敏或输出日志。
fn cache_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Cache, phase, message)
}

/// 业务作用：创建带底层错误链的缓存组件错误(输出前统一脱敏)。
///
/// 参数说明：
///
/// - `phase`:故障被观察到的生命周期阶段。
/// - `message`:不含敏感内容的稳定摘要。
/// - `source`:只供诊断的底层错误。
///
/// 返回：带 Cache 身份、阶段与底层原因的 ApplicationError；不在此裁剪错误链，对外呈现仍须经过统一脱敏边界。
fn cache_error_src(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Cache, phase, message, source)
}
