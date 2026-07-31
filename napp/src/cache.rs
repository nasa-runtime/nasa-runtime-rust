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
    /// 经受管客户端执行只读 `PING` 健康探针。
    async fn health_check(&self) -> anyhow::Result<()> {
        self.client.ping().await?;
        Ok(())
    }

    /// 经受管客户端读一个 key(GET)。
    ///
    /// # 参数
    /// - `key`: 完整缓存 key。
    async fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        Ok(self.client.get::<String>(key).await?)
    }

    /// 经受管客户端以毫秒 TTL 写一个 key(PSETEX)。
    ///
    /// # 参数
    /// - `key`: 完整缓存 key。
    /// - `value`: 已序列化的 JSON 载荷。
    /// - `ttl_ms`: 过期毫秒数。
    async fn set(&self, key: &str, value: &str, ttl_ms: u64) -> anyhow::Result<()> {
        self.client
            .set_ttl(key, value, std::time::Duration::from_millis(ttl_ms))
            .await?;
        Ok(())
    }

    /// 经受管客户端删一个 key(DEL)。
    ///
    /// # 参数
    /// - `key`: 完整缓存 key。
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

/// 缓存组件负责读取的顶层配置根投影。
#[derive(Default, Deserialize)]
#[serde(default)]
struct CacheConfigRoot {
    cache: Option<CacheConfig>,
}

/// 缓存工作模式。
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CacheMode {
    /// 显式关闭:组件成为无副作用空操作(不建连、不装 backend、不起广播)。
    #[default]
    Disabled,
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
    /// 广播用 Redis 连接串(pub/sub,单节点即可);启用时必填。空串=不广播。
    redis_url: String,
}

/// 正常缓存值 TTL 缺省。
fn default_cache_ttl_secs() -> u64 {
    DEFAULT_CACHE_TTL_SECS
}

/// 空哨兵 TTL 缺省。
fn default_null_ttl_secs() -> u64 {
    DEFAULT_NULL_TTL_SECS
}

impl CacheConfig {
    /// 无副作用校验:`two_level` 必须给 L2 连接串;启用广播必须给广播连接串。
    ///
    /// # 参数
    ///
    /// - `phase`:本次校验所属生命周期阶段,用于错误归因。
    fn validate(&self, phase: ApplicationPhase) -> ApplicationResult<()> {
        let has_ref = self
            .redis_ref
            .as_deref()
            .is_some_and(|r| !r.trim().is_empty());
        let has_url = !self.redis_url.trim().is_empty();
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
        if self.invalidation.enabled && self.invalidation.redis_url.trim().is_empty() {
            return Err(cache_error(
                phase,
                "cache.invalidation.redis_url is required when invalidation is enabled",
            ));
        }
        Ok(())
    }
}

/// 缓存组件:Start 建两级缓存管道并装入进程级 runtime,停机排空失效广播。
pub(crate) struct CacheComponent {
    config: Option<CacheConfig>,
    /// Ready 后交由 Runner 监督的后端健康 monitor。
    critical_task: Option<ApplicationFuture<'static>>,
}

impl CacheComponent {
    /// 创建尚未读取配置的缓存组件。
    ///
    /// # 参数
    ///
    /// 本方法无参数;L2 连接与失效广播在 Start 阶段按最终配置创建。
    pub(crate) fn new() -> Self {
        Self {
            config: None,
            critical_task: None,
        }
    }
}

/// 周期探测当前 generation 的 L2 后端并刷新非关键 readiness。
///
/// Cache miss 可以回源，因此运行期后端失败只降级而不摘流；启动期无法建立后端仍然 fail closed。
async fn run_cache_monitor(
    application: Application,
    handle: cacheable::CacheHandle,
    contributor: ReadinessContributor,
) -> ApplicationResult<()> {
    loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                contributor.observe(
                    DependencyState::NotReady,
                    reason::NOT_READY,
                    std::time::Instant::now(),
                );
                return Ok(());
            }
            ApplicationState::Starting => {
                tokio::time::sleep(CACHE_MONITOR_INTERVAL).await;
                continue;
            }
            ApplicationState::Ready => {}
        }
        let now = std::time::Instant::now();
        // 后端客户端本身通常带命令超时，这里仍施加 monitor 级上限，避免错误配置或驱动缺陷让
        // 单次探针永久占住监督任务，导致 readiness 只能依赖 stale 被动翻转且停机无法及时收束。
        match tokio::time::timeout(CACHE_MONITOR_INTERVAL, handle.health_check()).await {
            Ok(Ok(())) => contributor.observe(DependencyState::Ready, reason::HEALTHY, now),
            Ok(Err(_)) | Err(_) => {
                contributor.observe(DependencyState::Degraded, reason::DEGRADED, now)
            }
        }
        tokio::time::sleep(CACHE_MONITOR_INTERVAL).await;
    }
}

impl ApplicationComponent for CacheComponent {
    /// 返回缓存组件稳定身份。
    ///
    /// # 参数
    ///
    /// 本方法无参数;Runner 用它归类缓存相关错误。
    fn id(&self) -> ComponentId {
        ComponentId::Cache
    }

    /// 读取配置,`two_level` 时建 L2 + 失效广播、装入进程级 runtime,并压入 Stopping 停机 action。
    ///
    /// 管道在 Start 建立(而非 Ready):Batch 不执行 Ready,但 Start 与停机都执行,故两种模式都能建立
    /// 并在退出前停机。装入进程级 `CacheRuntime` 早于流量入口 Ready,`#[cached]` 首次命中即可用 L2。
    ///
    /// # 参数
    ///
    /// - `context`:提供最终配置与 active stack 的 Start 上下文。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let config = read_cache_config(context.application())?;
            config.validate(ApplicationPhase::Start)?;
            // scene 一致性审计:同名 scene 的 `#[cached]` 值类型/TTL 合同不一致是代码 bug,前移到
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

            // L2 后端:`redis_ref` 复用受管连接,否则自建集群连接。
            let layer = build_cache_layer(context.application(), &config).await?;
            let broadcast_url = if config.invalidation.enabled {
                Some(config.invalidation.redis_url.as_str())
            } else {
                None
            };
            // 装配 + 拥有:guard 把 backend 装进进程级 CacheRuntime(供 #[cached] 展开代码使用),
            // 并(可选)启动可 join 的失效广播;其停机由下面的 action 拥有。
            let guard = cacheable::CacheRuntimeGuard::start(layer, broadcast_url)
                .await
                .map_err(|error| {
                    cache_error_src(
                        ApplicationPhase::Start,
                        "cache invalidation broadcast start failed",
                        error,
                    )
                })?;
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
            context.activate(Box::new(CacheShutdown { guard: Some(guard) }));
            tracing::info!(
                "cache component installed two-level runtime (broadcast={})",
                config.invalidation.enabled
            );
            self.config = Some(config);
            Ok(())
        })
    }

    /// 取出缓存后端健康 monitor，交给 Runner 在 Ready 后监督。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("cache-health-monitor", task))
    }
}

/// 停机 action:排空并停止失效广播(发布 drainer + 订阅循环),join 后退出。
///
/// 等待上限由 Runner 对每个 action 施加的全局剩余停机预算 timeout 约束。
struct CacheShutdown {
    guard: Option<cacheable::CacheRuntimeGuard>,
}

impl ShutdownAction for CacheShutdown {
    /// 返回清理报告使用的稳定动作名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数;名称不含连接串或配置值。
    fn label(&self) -> &'static str {
        "cache-runtime"
    }

    /// 排空并停止失效广播;只能停一次(消费 guard)。
    ///
    /// # 参数
    ///
    /// - `_context`:共享停机预算;等待上限由 Runner 的整体 timeout 约束。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            if let Some(guard) = self.guard.take() {
                guard.shutdown().await;
            }
            Ok(())
        })
    }
}

/// 按 `cache` 配置构造 L2 `CacheLayer`:`redis_ref` 复用受管连接,否则自建集群连接。
///
/// 复用路径经 [`RedisClientBackend`] 把受管 `RedisClient` 适配成 `CacheBackend`,不新开集群连接;受管实例
/// 未声明/未就绪(`redis -> cache` 序保证 redis 先发布)时给清晰错误。自建路径保持 v1 的 `connect_cluster`。
///
/// # 参数
///
/// - `application`:Start 上下文的应用句柄,复用路径据此借出受管 Redis 客户端。
/// - `config`:已校验的 `two_level` 缓存配置。
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
            let backend: Arc<dyn cacheable::cache::CacheBackend> =
                Arc::new(RedisClientBackend { client });
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

/// 从最终配置读取 `cache` 段;缺失该段却声明了组件时报明确错误。
///
/// # 参数
///
/// - `application`:提供当前不可变配置快照的共享上下文。
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

/// 在不建立任何连接的前提下校验候选配置树中的 `cache` 段。
///
/// # 参数
///
/// - `tree`:合并、插值完成但尚未发布的候选配置树。
/// - `phase`:本次无副作用校验所属的生命周期阶段。
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

/// 创建缓存组件的稳定生命周期错误。
///
/// # 参数
///
/// - `phase`:故障被观察到的生命周期阶段。
/// - `message`:不含缓存 key、value 或连接串的稳定摘要。
fn cache_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Cache, phase, message)
}

/// 创建带底层错误链的缓存组件错误(输出前统一脱敏)。
///
/// # 参数
///
/// - `phase`:故障被观察到的生命周期阶段。
/// - `message`:不含敏感内容的稳定摘要。
/// - `source`:只供诊断的底层错误。
fn cache_error_src(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Cache, phase, message, source)
}
