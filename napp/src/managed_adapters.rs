//! 已有 adapter 的标准配置装配，所有依赖须在组件 Prepare 后可用。

use crate::{
    Application, ApplicationError, ApplicationPhase, ApplicationResult, ComponentId, PrepareContext,
};
#[cfg(any(
    all(feature = "cache", feature = "redis"),
    feature = "object-store",
    feature = "secret-http",
    feature = "kafka-schema-registry",
    feature = "idempotency-mysql",
    feature = "idempotency-pgsql",
    feature = "idempotency-redis",
    feature = "audit-mysql",
    feature = "audit-pgsql"
))]
use crate::{ApplicationFuture, ShutdownAction, ShutdownContext};
use std::collections::BTreeMap;
#[cfg(any(
    feature = "redis",
    feature = "object-store",
    feature = "secret-http",
    feature = "kafka-schema-registry",
    feature = "idempotency-mysql",
    feature = "idempotency-pgsql",
    feature = "idempotency-redis",
    feature = "audit-mysql",
    feature = "audit-pgsql"
))]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
#[cfg(any(
    feature = "redis",
    feature = "object-store",
    feature = "secret-http",
    feature = "kafka-schema-registry",
    feature = "idempotency-mysql",
    feature = "idempotency-pgsql",
    feature = "idempotency-redis",
    feature = "audit-mysql",
    feature = "audit-pgsql"
))]
use tokio::sync::{RwLock, RwLockReadGuard};

/// 业务作用：在任何消费者连接前拒绝显式选择但未编入或未声明依赖的标准子能力。
///
/// 参数说明：
/// - `tree`：`tree` 为当前配置。
/// - `components`：`components` 为真实组件声明。
///
/// 共同约束：历史未启用配置仍保持无副作用。
///
/// 返回：已选择能力和前置依赖齐全时成功，不对业务自有配置施加全局未知字段策略。
pub(crate) fn validate(
    tree: &serde_json::Value,
    components: &[ComponentId],
) -> ApplicationResult<()> {
    let selected = |section: &str| {
        tree.get(section)
            .and_then(serde_json::Value::as_object)
            .is_some_and(|plans| {
                plans.values().any(|plan| {
                    plan.get("enabled").and_then(serde_json::Value::as_bool) == Some(true)
                })
            })
    };
    for (section, compiled) in [
        ("object_stores", cfg!(feature = "object-store")),
        ("http_clients", cfg!(feature = "secret-http")),
        ("schema_registries", cfg!(feature = "kafka-schema-registry")),
        ("rest_clients", cfg!(feature = "rest")),
        ("redis_leaders", cfg!(feature = "redis")),
        ("redis_subscriptions", cfg!(feature = "redis")),
        ("redis_streams", cfg!(feature = "redis")),
        ("redis_proxies", cfg!(feature = "redis")),
        ("redis_pipelines", cfg!(feature = "redis")),
        ("ws_clients", cfg!(feature = "ws-client")),
        (
            "grouped_caches",
            cfg!(all(feature = "cache", feature = "redis")),
        ),
        ("secret_providers", cfg!(feature = "secret-vault")),
    ] {
        if selected(section) && !compiled {
            return Err(adapter_error(
                "explicit managed capability requires its feature",
                anyhow::anyhow!("capability feature is disabled: {section}"),
            ));
        }
    }
    if tree
        .pointer("/hystrix/enabled")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
        && !cfg!(feature = "hystrix")
    {
        return Err(adapter_error(
            "hystrix requires its feature",
            anyhow::anyhow!("managed hystrix is unavailable"),
        ));
    }
    crate::secret::validate_provider_plans(tree).map_err(|error| {
        adapter_error(
            "invalid managed secret provider declarations",
            anyhow::anyhow!(error),
        )
    })?;
    let mapper = tree
        .pointer("/mapper_cache/enabled")
        .and_then(serde_json::Value::as_bool)
        == Some(true);
    if mapper
        && !cfg!(any(
            feature = "mapper-cache",
            feature = "mapper-cache-pgsql",
            feature = "mapper-cache-sqlite"
        ))
    {
        return Err(adapter_error(
            "mapper_cache requires a mapper cache feature",
            anyhow::anyhow!("managed Mapper cache is unavailable"),
        ));
    }
    if (mapper
        || selected("grouped_caches")
        || selected("redis_leaders")
        || selected("redis_subscriptions")
        || selected("redis_streams")
        || selected("redis_proxies")
        || selected("redis_pipelines")
        || tree
            .pointer("/redis/snowflake")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|plans| {
                plans.values().any(|plan| {
                    plan.get("enabled").and_then(serde_json::Value::as_bool) == Some(true)
                })
            }))
        && !components.contains(&ComponentId::Redis)
    {
        return Err(adapter_error(
            "managed Redis subcapability requires redis component",
            anyhow::anyhow!("Redis was not declared"),
        ));
    }
    if tree
        .pointer("/config_watch/enabled")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
        && !cfg!(feature = "config-watch")
    {
        return Err(adapter_error(
            "config_watch requires config-watch feature",
            anyhow::anyhow!("file watch is unavailable"),
        ));
    }
    for section in ["idempotency_stores", "audit_sinks"] {
        if let Some(plans) = tree.get(section).and_then(serde_json::Value::as_object) {
            for plan in plans.values().filter(|plan| {
                plan.get("enabled").and_then(serde_json::Value::as_bool) == Some(true)
            }) {
                let driver = plan.get("driver").and_then(serde_json::Value::as_str);
                // 驱动名称与对应编译能力必须同时成立，配置存在不能代替 adapter 已编入。
                let supported = [
                    (
                        "idempotency_stores",
                        "mysql",
                        cfg!(feature = "idempotency-mysql"),
                    ),
                    (
                        "idempotency_stores",
                        "pgsql",
                        cfg!(feature = "idempotency-pgsql"),
                    ),
                    (
                        "idempotency_stores",
                        "redis",
                        cfg!(feature = "idempotency-redis"),
                    ),
                    ("audit_sinks", "mysql", cfg!(feature = "audit-mysql")),
                    ("audit_sinks", "pgsql", cfg!(feature = "audit-pgsql")),
                ]
                .into_iter()
                .any(|(kind, backend, enabled)| {
                    enabled && section == kind && driver == Some(backend)
                });
                if !supported
                    || !components.contains(&if driver == Some("redis") {
                        ComponentId::Redis
                    } else {
                        ComponentId::Db
                    })
                {
                    return Err(adapter_error(
                        "managed store requires a supported driver and declared source component",
                        anyhow::anyhow!("invalid managed store selection"),
                    ));
                }
                if plan.get("web_default").and_then(serde_json::Value::as_bool) == Some(true)
                    && (!cfg!(feature = "web") || !components.contains(&ComponentId::Web))
                {
                    return Err(adapter_error(
                        "web_default requires a declared Web component",
                        anyhow::anyhow!("Web was not declared"),
                    ));
                }
            }
        }
    }
    Ok(())
}

/// 命名 adapter 的永久关闭门禁；读守卫覆盖完整领域调用。
#[cfg(any(
    feature = "redis",
    feature = "object-store",
    feature = "secret-http",
    feature = "kafka-schema-registry",
    feature = "idempotency-mysql",
    feature = "idempotency-pgsql",
    feature = "idempotency-redis",
    feature = "audit-mysql",
    feature = "audit-pgsql"
))]
pub(crate) struct ManagedAdapter<T: ?Sized> {
    /// 永久撤销新调用准入；已有读守卫仍可完成本次调用。
    closed: AtomicBool,
    /// 当前资源引用；读锁覆盖后端调用，停机取得写锁后移除引用。
    inner: RwLock<Option<Arc<T>>>,
}

#[cfg(any(
    feature = "redis",
    feature = "object-store",
    feature = "secret-http",
    feature = "kafka-schema-registry",
    feature = "idempotency-mysql",
    feature = "idempotency-pgsql",
    feature = "idempotency-redis",
    feature = "audit-mysql",
    feature = "audit-pgsql"
))]
impl<T: ?Sized> ManagedAdapter<T> {
    /// 业务作用：为已验证的 adapter 建立当前应用的调用 owner。
    ///
    /// 参数说明：
    /// - `inner`：`inner` 为需要随宿主关闭的 adapter。
    ///
    /// 返回：开放且持有资源的 owner，不创建额外任务。
    pub(crate) fn new(inner: Arc<T>) -> Arc<Self> {
        Arc::new(Self {
            closed: AtomicBool::new(false),
            inner: RwLock::new(Some(inner)),
        })
    }

    /// 业务作用：在应用仍允许调用时取得覆盖整个后端操作的资源读守卫。
    ///
    /// 参数说明：无。
    ///
    /// 返回：取得锁后再次确认准入和资源存在才交还守卫；任一条件不满足时拒绝。
    /// 调用方必须保留守卫直至后端操作结束；已经准入的操作不因随后关闭而自动取消。
    pub(crate) async fn enter(&self) -> anyhow::Result<RwLockReadGuard<'_, Option<Arc<T>>>> {
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "managed adapter is closed"
        );
        let guard = self.inner.read().await;
        // 等待锁期间可能发生停机；拿到资源后必须再确认准入，不能沿用等待前的判断。
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire) && guard.is_some(),
            "managed adapter is closed"
        );
        Ok(guard)
    }

    /// 业务作用：阻止保留的旧句柄继续开始调用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：永久关闭准入，不等待已接纳工作。
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    /// 业务作用：等待已接纳的领域调用归还守卫，再移除本 owner 持有的资源引用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：取得独占锁并取出资源后完成；其它 Arc 持有者仍可延长底层资源寿命。
    /// 调用前必须关闭准入；本函数不设超时，取消等待不会重新开放准入。
    #[cfg(feature = "redis")]
    pub(crate) async fn drain(&self) {
        self.inner.write().await.take();
    }
}

#[cfg(any(
    all(feature = "cache", feature = "redis"),
    feature = "object-store",
    feature = "secret-http",
    feature = "kafka-schema-registry",
    feature = "idempotency-mysql",
    feature = "idempotency-pgsql",
    feature = "idempotency-redis",
    feature = "audit-mysql",
    feature = "audit-pgsql"
))]
pub(crate) struct AdapterShutdown<T: ?Sized + Send + Sync>(
    /// 清理动作负责关闭并排干的 adapter；Drop 仍会撤销其准入。
    pub(crate) Arc<ManagedAdapter<T>>,
);

#[cfg(any(
    all(feature = "cache", feature = "redis"),
    feature = "object-store",
    feature = "secret-http",
    feature = "kafka-schema-registry",
    feature = "idempotency-mysql",
    feature = "idempotency-pgsql",
    feature = "idempotency-redis",
    feature = "audit-mysql",
    feature = "audit-pgsql"
))]
impl<T: ?Sized + Send + Sync + 'static> ShutdownAction for AdapterShutdown<T> {
    /// 业务作用：提供标准 adapter 的稳定清理名称。
    ///
    /// 参数说明：无。
    ///
    /// 返回：固定名称，不使用业务 key 或 endpoint。
    fn label(&self) -> &'static str {
        "managed-adapter"
    }

    /// 业务作用：同步撤销调用准入，再在宿主停机预算内等待已接纳操作归还守卫。
    ///
    /// 参数说明：
    /// - `context`：宿主共享停机截止点，不为每个 adapter 重新分配完整预算。
    ///
    /// 返回：排干后移除本 owner 的资源引用；超时返回停机错误并保持关闭。
    /// 返回的 future 未执行或被取消也不重新开放准入，不主动取消已接纳操作或重试副作用。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        // 先关闭再构造 future，避免清理任务未被调度时旧句柄继续接纳工作。
        self.0.close();
        Box::pin(async move {
            let mut guard =
                tokio::time::timeout_at(context.deadline().into(), self.0.inner.write())
                    .await
                    .map_err(|_| {
                        ApplicationError::new(
                            ComponentId::Application,
                            ApplicationPhase::Stopping,
                            "managed adapter calls did not drain",
                        )
                    })?;
            guard.take();
            Ok(())
        })
    }
}

#[cfg(any(
    all(feature = "cache", feature = "redis"),
    feature = "object-store",
    feature = "secret-http",
    feature = "kafka-schema-registry",
    feature = "idempotency-mysql",
    feature = "idempotency-pgsql",
    feature = "idempotency-redis",
    feature = "audit-mysql",
    feature = "audit-pgsql"
))]
impl<T: ?Sized + Send + Sync> Drop for AdapterShutdown<T> {
    /// 业务作用：启动失败或清理 owner 提前退出时收回新调用准入。
    ///
    /// 参数说明：无。
    ///
    /// 返回：旧句柄永久关闭，不产生后台任务。
    fn drop(&mut self) {
        self.0.close();
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StorePlan {
    /// 是否装配此命名资源，省略时不启用。
    #[serde(default)]
    enabled: bool,
    /// 持久后端名称；启用时必须匹配已编入的驱动。
    driver: Option<String>,
    /// 所属数据源或 Redis 客户端名称，省略时使用 default。
    source: Option<String>,
    /// 是否作为 Web 默认幂等 store，省略为 false；审计 sink 不支持此选项。
    #[serde(default)]
    web_default: bool,
}

/// 业务作用：在基础组件准备后装配显式启用的受管能力，并汇集需要持续运行的健康任务。
///
/// 参数说明：
/// - `context`：提供当前应用配置、基础资源、资源发布入口和停机清理登记。
///
/// 返回：无健康任务时为 None，否则交还并发等待各任务的 future，任一任务失败即结束等待。
/// 配置、后端或资源登记失败时中止准备；先前成功装配的资源由宿主已登记的清理动作回收。
pub(crate) async fn prepare(
    context: &mut PrepareContext<'_>,
) -> ApplicationResult<Option<crate::ApplicationFuture<'static>>> {
    let app = context.application().clone();
    for section in ["idempotency_stores", "audit_sinks"] {
        let snapshot = app.config();
        let Some(value) = snapshot.value().get(section) else {
            continue;
        };
        let plans: BTreeMap<String, StorePlan> = serde_json::from_value(value.clone())
            .map_err(|error| adapter_error("invalid managed adapter plans", error))?;
        if plans.len() > 64 {
            return Err(adapter_error(
                "managed adapter count exceeds limit",
                anyhow::anyhow!("at most 64 plans per capability"),
            ));
        }
        for (name, plan) in plans {
            if !plan.enabled {
                continue;
            }
            if name.is_empty() || name.len() > 128 || name.trim() != name {
                return Err(adapter_error(
                    "invalid managed adapter name",
                    anyhow::anyhow!("name must be nonempty and at most 128 bytes"),
                ));
            }
            match section {
                "idempotency_stores" => prepare_idempotency(context, &app, &name, &plan).await?,
                _ => prepare_audit(context, &app, &name, &plan).await?,
            }
        }
    }
    let object_monitor = crate::object_store::prepare(context).await?;
    crate::grouped_cache::prepare(context).await?;
    #[cfg(feature = "kafka-schema-registry")]
    crate::schema_registry::prepare(context)?;
    #[cfg(feature = "rest")]
    crate::rest::prepare(context).await?;
    #[cfg(feature = "secret-http")]
    crate::tls_http::install(context)?;
    #[allow(unused_mut)]
    let mut monitors = Vec::new();
    if let Some(monitor) = object_monitor {
        monitors.push(monitor);
    }
    #[cfg(feature = "ws-client")]
    if let Some(monitor) = app.ws_clients().prepare(context).await? {
        monitors.push(monitor);
    }
    #[cfg(feature = "hystrix")]
    if let Some(monitor) = crate::hystrix_managed::prepare(context).await? {
        monitors.push(monitor);
    }
    if monitors.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Box::pin(async move {
            futures_util::future::try_join_all(monitors).await?;
            Ok(())
        })))
    }
}

/// 业务作用：为装配失败附加 Application 组件及 Prepare 阶段归属，并保留底层原因。
///
/// 参数说明：
/// - `message`：不含配置值或凭据的固定错误摘要。
/// - `error`：保留在错误链中的底层原因，调用方须确保其中没有敏感信息。
///
/// 返回：带来源的准备阶段错误；本函数不对底层错误内容做脱敏转换。
fn adapter_error(message: &'static str, error: impl Into<anyhow::Error>) -> ApplicationError {
    ApplicationError::with_source(
        ComponentId::Application,
        ApplicationPhase::Prepare,
        message,
        error,
    )
}

/// 可跨任务共享的幂等 store；应用发布的实例另受 ManagedAdapter 关闭门禁约束。
type SharedStore = Arc<dyn naidempotency::IdempotencyStore + Send + Sync>;

/// 业务作用：验证固定来源和 schema 后建立一个命名幂等 store。
///
/// 参数说明：
/// - `context`：`context` 负责清理。
/// - `app`：`app` 提供受管来源。
/// - `name`：`name` 为资源名。
/// - `plan`：`plan` 为显式计划。
///
/// 返回：成功注册资源和可选 Web 入口；所有后端选择错误都在流量前拒绝。
#[allow(unused_variables, unreachable_code)]
async fn prepare_idempotency(
    context: &mut PrepareContext<'_>,
    app: &Application,
    name: &str,
    plan: &StorePlan,
) -> ApplicationResult<()> {
    let source = plan.source.as_deref().unwrap_or("default");
    let store: SharedStore = match plan.driver.as_deref() {
        #[cfg(feature = "idempotency-mysql")]
        Some("mysql") => {
            app.datasource(source).await?;
            let store = naidempotency_mysql::MySqlIdempotencyStore::with_datasource(source)
                .map_err(|error| adapter_error("invalid idempotency datasource", error))?;
            store
                .validate_schema()
                .await
                .map_err(|error| adapter_error("idempotency schema gate failed", error))?;
            Arc::new(store)
        }
        #[cfg(feature = "idempotency-pgsql")]
        Some("pgsql") => {
            app.pg_datasource(source).await?;
            let store = naidempotency_pgsql::PgIdempotencyStore::with_datasource(source)
                .map_err(|error| adapter_error("invalid idempotency datasource", error))?;
            store
                .validate_schema()
                .await
                .map_err(|error| adapter_error("idempotency schema gate failed", error))?;
            Arc::new(store)
        }
        #[cfg(feature = "idempotency-redis")]
        Some("redis") => {
            let client = crate::redis::redis_handle(app, source).await?;
            client
                .ping()
                .await
                .map_err(|error| adapter_error("idempotency Redis is unavailable", error))?;
            Arc::new(naidempotency_redis::RedisIdempotencyStore::new(client))
        }
        _ => {
            return Err(adapter_error(
                "idempotency driver is unknown or its feature is disabled",
                anyhow::anyhow!("select an enabled mysql, pgsql or redis adapter"),
            ))
        }
    };
    #[cfg(any(
        feature = "idempotency-mysql",
        feature = "idempotency-pgsql",
        feature = "idempotency-redis"
    ))]
    {
        let managed = ManagedAdapter::new(store);
        // 先登记清理再发布句柄，后续资源注册或 Web 安装失败仍能撤销准入并回收引用。
        context.activate(Box::new(AdapterShutdown(managed.clone())));
        let shared: SharedStore = managed;
        context.register_resource(Some(name), shared.clone())?;
        if plan.web_default {
            #[cfg(feature = "web")]
            app.install_managed_idempotency_store(shared)?;
            #[cfg(not(feature = "web"))]
            return Err(adapter_error(
                "idempotency web_default requires web feature",
                anyhow::anyhow!("web is not enabled"),
            ));
        }
    }
    Ok(())
}

impl Application {
    /// 业务作用：取得已经绑定当前应用来源的命名幂等 store，无需启用 Web。
    ///
    /// 参数说明：
    /// - `name`：`name` 为 idempotency_stores 中的资源名。
    ///
    /// 返回：受永久关闭门禁保护的共享 store；不存在时返回资源错误。
    pub async fn idempotency_store_named(&self, name: &str) -> ApplicationResult<SharedStore> {
        Ok(self.named_resource::<SharedStore>(name).await?.clone())
    }
}

#[cfg(any(
    feature = "idempotency-mysql",
    feature = "idempotency-pgsql",
    feature = "idempotency-redis"
))]
#[async_trait::async_trait]
impl naidempotency::IdempotencyStore
    for ManagedAdapter<dyn naidempotency::IdempotencyStore + Send + Sync>
{
    /// 业务作用：在当前应用的完整调用责任内裁决请求身份与执行占位。
    ///
    /// 参数说明：
    /// - `key`：隔离租户、主体、路由及客户端键的幂等命名空间。
    /// - `fingerprint`：用于区分相同身份下请求内容是否等价的指纹。
    /// - `lease`：调用方提供的首次执行持有者令牌，不代表预先取得执行权，也不携带有效期。
    ///
    /// 返回：原样交还后端的首次执行、重放、冲突或执行中裁决；关闭或后端失败返回错误。
    /// 只有首次执行裁决授权本次业务执行，后端错误不证明占位尚未写入。
    async fn begin(
        &self,
        key: &naidempotency::IdempotencyKey,
        fingerprint: naidempotency::RequestFingerprint,
        lease: naidempotency::ExecutionLease,
    ) -> Result<naidempotency::IdempotencyOutcome, naidempotency::IdempotencyError> {
        let guard = self
            .enter()
            .await
            .map_err(|_| naidempotency::IdempotencyError::new("managed store is closed"))?;
        guard
            .as_ref()
            .expect("admitted adapter")
            .begin(key, fingerprint, lease)
            .await
    }

    /// 业务作用：在当前应用的完整调用责任内确认执行结果并保存响应。
    ///
    /// 参数说明：
    /// - `key`：租户、主体、路由及客户端键共同确定的幂等命名空间。
    /// - `fingerprint`：本次请求的等价类指纹，后端必须与原占位记录匹配。
    /// - `lease`：取得首次执行权时用于占位的持有者令牌，防止迟到完成覆盖其它持有者。
    /// - `response`：业务已确认可重放且经过容量限制的响应，交由后端持久化。
    ///
    /// 返回：后端确认当前租约完成时为 true，不再拥有匹配占位时为 false；关闭或存储失败返回错误。
    /// 读守卫覆盖整个后端调用，错误不代表业务未提交或完成记录未写入，本层不重试或释放占位。
    async fn complete(
        &self,
        key: &naidempotency::IdempotencyKey,
        fingerprint: naidempotency::RequestFingerprint,
        lease: naidempotency::ExecutionLease,
        response: naidempotency::StoredResponse,
    ) -> Result<bool, naidempotency::IdempotencyError> {
        let guard = self
            .enter()
            .await
            .map_err(|_| naidempotency::IdempotencyError::new("managed store is closed"))?;
        guard
            .as_ref()
            .expect("admitted adapter")
            .complete(key, fingerprint, lease, response)
            .await
    }

    /// 业务作用：在当前应用的完整调用责任内由显式调用方放弃已经确认可释放的占位。
    ///
    /// 参数说明：
    /// - `key`：待释放占位所属的完整幂等命名空间。
    /// - `fingerprint`：必须与原占位匹配的请求指纹。
    /// - `lease`：必须仍持有该占位的执行令牌。
    ///
    /// 返回：原占位被释放时为 true，已完成、不存在或不再匹配时为 false；关闭或存储失败返回错误。
    /// 本层不判断业务是否已提交；调用方不得因超时或结果不明而释放占位，错误时也不自动重试。
    async fn abort(
        &self,
        key: &naidempotency::IdempotencyKey,
        fingerprint: naidempotency::RequestFingerprint,
        lease: naidempotency::ExecutionLease,
    ) -> Result<bool, naidempotency::IdempotencyError> {
        let guard = self
            .enter()
            .await
            .map_err(|_| naidempotency::IdempotencyError::new("managed store is closed"))?;
        guard
            .as_ref()
            .expect("admitted adapter")
            .abort(key, fingerprint, lease)
            .await
    }
}

/// 业务作用：为事务审计装配同来源的 Outbox sink，不启动额外 dispatcher。
///
/// 参数说明：
/// - `context`：`context` 负责清理。
/// - `app`：`app` 提供受管来源。
/// - `name`：`name` 为资源名。
/// - `plan`：`plan` 为显式计划。
///
/// 返回：来源与 schema 门禁满足时注册 sink；缺 feature、来源错误或 schema 不符时拒绝。
#[allow(unused_variables, unreachable_code)]
async fn prepare_audit(
    context: &mut PrepareContext<'_>,
    app: &Application,
    name: &str,
    plan: &StorePlan,
) -> ApplicationResult<()> {
    if plan.web_default {
        return Err(adapter_error(
            "audit sink does not support web_default",
            anyhow::anyhow!("business decides when to record audit"),
        ));
    }
    #[cfg(any(feature = "audit-mysql", feature = "audit-pgsql"))]
    {
        let source = plan.source.as_deref().unwrap_or("default");
        let sink: Arc<dyn naaudit::TransactionalAuditSink> = match plan.driver.as_deref() {
            #[cfg(feature = "audit-mysql")]
            Some("mysql") => {
                app.datasource(source).await?;
                naoutbox_mysql::verify_outbox_event_schema_for(source)
                    .await
                    .map_err(|error| adapter_error("audit Outbox schema gate failed", error))?;
                Arc::new(
                    naaudit_mysql::MySqlOutboxAuditSink::with_datasource(source)
                        .map_err(|error| adapter_error("invalid audit datasource", error))?,
                )
            }
            #[cfg(feature = "audit-pgsql")]
            Some("pgsql") => {
                app.pg_datasource(source).await?;
                naoutbox_pgsql::verify_outbox_event_schema_for(source)
                    .await
                    .map_err(|error| adapter_error("audit Outbox schema gate failed", error))?;
                Arc::new(
                    naaudit_pgsql::PgOutboxAuditSink::with_datasource(source)
                        .map_err(|error| adapter_error("invalid audit datasource", error))?,
                )
            }
            _ => {
                return Err(adapter_error(
                    "audit driver is unknown or its feature is disabled",
                    anyhow::anyhow!("select an enabled mysql or pgsql audit adapter"),
                ))
            }
        };
        let managed = ManagedAdapter::new(sink);
        // 先将关闭责任交给宿主，再允许应用取得审计 sink。
        context.activate(Box::new(AdapterShutdown(managed.clone())));
        let shared: Arc<dyn naaudit::TransactionalAuditSink> = managed;
        context.register_resource(Some(name), shared)?;
        return Ok(());
    }
    Err(adapter_error(
        "audit adapter feature is disabled",
        anyhow::anyhow!("enable audit-mysql or audit-pgsql"),
    ))
}

#[cfg(any(feature = "audit-mysql", feature = "audit-pgsql"))]
#[async_trait::async_trait]
impl naaudit::TransactionalAuditSink for ManagedAdapter<dyn naaudit::TransactionalAuditSink> {
    /// 业务作用：将审计事件写入当前应用绑定的同源业务事务。
    ///
    /// 参数说明：
    /// - `event`：`event` 为业务明确决定记录的审计事实。
    ///
    /// 返回：审计持久意图已写入时成功；关闭或记录失败直接传播，不替业务提交事务。
    async fn record_transactional(
        &self,
        event: naaudit::AuditEvent,
    ) -> Result<(), naaudit::AuditWriteError> {
        let guard = self
            .enter()
            .await
            .map_err(|_| naaudit::AuditWriteError::new("managed audit sink is closed"))?;
        guard
            .as_ref()
            .expect("admitted adapter")
            .record_transactional(event)
            .await
    }
}

#[cfg(any(feature = "audit-mysql", feature = "audit-pgsql"))]
impl Application {
    /// 业务作用：取得当前应用的命名事务审计 sink。
    ///
    /// 参数说明：
    /// - `name`：`name` 为 audit_sinks 中的资源名。
    ///
    /// 返回：固定来源且受关闭门禁保护的 sink；未装配时返回资源错误。
    pub async fn audit_sink(
        &self,
        name: &str,
    ) -> ApplicationResult<Arc<dyn naaudit::TransactionalAuditSink>> {
        Ok(self
            .named_resource::<Arc<dyn naaudit::TransactionalAuditSink>>(name)
            .await?
            .clone())
    }
}
