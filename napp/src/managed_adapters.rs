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
/// 参数说明：`tree` 为当前配置；`components` 为真实组件声明；历史未启用配置仍保持无副作用。
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
            feature = "mapper-cache-pgsql"
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
                let supported = match (section, driver) {
                    ("idempotency_stores", Some("mysql")) => cfg!(feature = "idempotency-mysql"),
                    ("idempotency_stores", Some("pgsql")) => cfg!(feature = "idempotency-pgsql"),
                    ("idempotency_stores", Some("redis")) => cfg!(feature = "idempotency-redis"),
                    ("audit_sinks", Some("mysql")) => cfg!(feature = "audit-mysql"),
                    ("audit_sinks", Some("pgsql")) => cfg!(feature = "audit-pgsql"),
                    _ => false,
                };
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
    closed: AtomicBool,
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
    /// 参数说明：`inner` 为需要随宿主关闭的 adapter。
    /// 返回：开放且持有资源的 owner，不创建额外任务。
    pub(crate) fn new(inner: Arc<T>) -> Arc<Self> {
        Arc::new(Self {
            closed: AtomicBool::new(false),
            inner: RwLock::new(Some(inner)),
        })
    }

    /// 业务作用：在当前代次仍开放时登记一次完整领域调用。
    /// 参数说明：无。
    /// 返回：调用期间保留资源的读守卫；关闭后拒绝。
    pub(crate) async fn enter(&self) -> anyhow::Result<RwLockReadGuard<'_, Option<Arc<T>>>> {
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "managed adapter is closed"
        );
        let guard = self.inner.read().await;
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire) && guard.is_some(),
            "managed adapter is closed"
        );
        Ok(guard)
    }

    /// 业务作用：阻止保留的旧句柄继续开始调用。
    /// 参数说明：无。
    /// 返回：永久关闭准入，不等待已接纳工作。
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    /// 业务作用：等待已接纳的领域调用归还依赖并释放底层资源。
    /// 参数说明：无。
    /// 返回：独占锁证明没有在途调用后完成；调用前必须关闭准入。
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
pub(crate) struct AdapterShutdown<T: ?Sized + Send + Sync>(pub(crate) Arc<ManagedAdapter<T>>);

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
    /// 参数说明：无。
    /// 返回：固定名称，不使用业务 key 或 endpoint。
    fn label(&self) -> &'static str {
        "managed-adapter"
    }

    /// 业务作用：关闭新调用并等待当前代次已接纳工作归还依赖。
    /// 参数说明：`context` 为宿主共享停机截止点。
    /// 返回：排干后释放 adapter；超时保留关闭状态和在途责任，不重放副作用。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
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
    /// 参数说明：无。
    /// 返回：旧句柄永久关闭，不产生后台任务。
    fn drop(&mut self) {
        self.0.close();
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StorePlan {
    #[serde(default)]
    enabled: bool,
    driver: Option<String>,
    source: Option<String>,
    #[serde(default)]
    web_default: bool,
}

/// 业务作用：在数据源迁移门禁之后装配显式启用的持久 store 和审计 adapter。
/// 参数说明：`context` 提供当前应用资源与清理登记。
/// 返回：装配成功时移交可选的对象健康任务；未知 provider、缺 feature、错源或结构不符时拒绝进入工作负载。
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
    Ok(object_monitor)
}

/// 业务作用：将内部原因归类为不包含凭据的标准装配错误。
/// 参数说明：`message` 为稳定摘要；`error` 为底层原因。
/// 返回：准备阶段错误。
fn adapter_error(message: &'static str, error: impl Into<anyhow::Error>) -> ApplicationError {
    ApplicationError::with_source(
        ComponentId::Application,
        ApplicationPhase::Prepare,
        message,
        error,
    )
}

type SharedStore = Arc<dyn naidempotency::IdempotencyStore + Send + Sync>;

/// 业务作用：验证固定来源和 schema 后建立一个命名幂等 store。
/// 参数说明：`context` 负责清理；`app` 提供受管来源；`name` 为资源名；`plan` 为显式计划。
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
    /// 参数说明：`name` 为 idempotency_stores 中的资源名。
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
    /// 参数说明：`key` 为请求身份；`fingerprint` 为内容指纹；`lease` 为调用方权威。
    /// 返回：保持领域裁决和失败语义；应用关闭后拒绝新调用。
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
    /// 参数说明：`key` 为请求身份；`fingerprint` 为内容指纹；`lease` 为调用方权威。`response` 为待保存响应。
    /// 返回：保持领域裁决和失败语义；应用关闭后拒绝新调用。
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
    /// 参数说明：`key` 为请求身份；`fingerprint` 为内容指纹；`lease` 为调用方权威。
    /// 返回：保持领域裁决和失败语义；应用关闭后拒绝新调用。
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
/// 参数说明：`context` 负责清理；`app` 提供受管来源；`name` 为资源名；`plan` 为显式计划。
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
    /// 参数说明：`event` 为业务明确决定记录的审计事实。
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
    /// 参数说明：`name` 为 audit_sinks 中的资源名。
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
