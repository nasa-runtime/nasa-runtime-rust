//! 复用命名 Redis 来源的分组缓存标准装配。

#[cfg(all(feature = "cache", feature = "redis"))]
use crate::Application;
use crate::{ApplicationError, ApplicationPhase, ApplicationResult, ComponentId, PrepareContext};
use std::collections::BTreeMap;
#[cfg(all(feature = "cache", feature = "redis"))]
use std::sync::Arc;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(not(all(feature = "cache", feature = "redis")), allow(dead_code))]
struct Plan {
    #[serde(default)]
    enabled: bool,
    redis_ref: Option<String>,
    backstop_ttl_secs: Option<u64>,
    #[serde(default)]
    clear_refs: BTreeMap<String, Vec<String>>,
}

/// 业务作用：装配命名分组缓存，复用来源连接并在业务之前验证 field TTL。
/// 参数说明：`context` 提供来源、资源表和清理 owner。
/// 返回：所有启用计划通过；无对应 feature、错源或能力不符时拒绝。
pub(crate) async fn prepare(context: &mut PrepareContext<'_>) -> ApplicationResult<()> {
    let app = context.application().clone();
    let view = app.config();
    let Some(value) = view.value().get("grouped_caches") else {
        return Ok(());
    };
    let plans: BTreeMap<String, Plan> = serde_json::from_value(value.clone())
        .map_err(|_| error("invalid grouped_caches declaration"))?;
    if plans.len() > 64 {
        return Err(error("grouped cache count exceeds 64"));
    }
    for (name, plan) in plans {
        if !plan.enabled {
            continue;
        }
        if name.is_empty() || name.len() > 128 || name.trim() != name {
            return Err(error("invalid grouped cache name"));
        }
        #[cfg(not(all(feature = "cache", feature = "redis")))]
        {
            let _ = (name, plan);
            return Err(error("grouped_caches requires cache and redis features"));
        }
        #[cfg(all(feature = "cache", feature = "redis"))]
        {
            let client =
                crate::redis::redis_handle(&app, plan.redis_ref.as_deref().unwrap_or("default"))
                    .await?;
            let cache = cacheable::cache::GroupedCache::try_new(
                client.conn(),
                plan.backstop_ttl_secs.unwrap_or(86_400),
            )
            .map_err(|_| error("invalid grouped cache TTL"))?;
            if plan.clear_refs.len() > 128
                || plan.clear_refs.values().any(|targets| targets.len() > 128)
            {
                return Err(error(
                    "grouped cache invalidation relation count exceeds limit",
                ));
            }
            for (source, targets) in &plan.clear_refs {
                if source.len() > 512 || targets.iter().any(|target| target.len() > 512) {
                    return Err(error("grouped cache group name exceeds limit"));
                }
                cache.register_clear_ref(
                    source,
                    &targets.iter().map(String::as_str).collect::<Vec<_>>(),
                );
            }
            cache
                .verify_field_ttl()
                .await
                .map_err(|_| error("grouped cache field TTL readiness failed"))?;
            let owner = crate::managed_adapters::ManagedAdapter::new(Arc::new(cache));
            context.activate(Box::new(crate::managed_adapters::AdapterShutdown(
                owner.clone(),
            )));
            context.register_resource(Some(&name), ManagedGroupedCache(owner))?;
        }
    }
    Ok(())
}

/// 命名分组缓存的调用入口，保留独立回源语义并共享永久关闭门禁。
#[cfg(all(feature = "cache", feature = "redis"))]
#[derive(Clone)]
pub struct ManagedGroupedCache(
    Arc<
        crate::managed_adapters::ManagedAdapter<
            cacheable::cache::GroupedCache<nadis::client::Conn>,
        >,
    >,
);

#[cfg(all(feature = "cache", feature = "redis"))]
impl ManagedGroupedCache {
    /// 业务作用：在受管调用责任内读取或回填组内字段，loader 由业务提供。
    /// 参数说明：`group`、`field` 定义缓存身份；`loader` 为只读回源逻辑。
    /// 返回：缓存或回源值；停机后拒绝新调用。显式失效与旧 loader 仍可能竞态，不承诺强一致。
    pub async fn get_or_load<T, F, Fut>(
        &self,
        group: &str,
        field: &str,
        loader: F,
    ) -> anyhow::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        let guard = self.0.enter().await?;
        guard
            .as_ref()
            .expect("admitted grouped cache")
            .get_or_load(group, field, loader)
            .await
    }

    /// 业务作用：在受管调用责任内使单个缓存字段失效。
    /// 参数说明：`group`、`field` 为缓存身份。
    /// 返回：后端确认删除；失败交还业务，关闭后拒绝。
    pub async fn invalidate_field(&self, group: &str, field: &str) -> anyhow::Result<()> {
        let guard = self.0.enter().await?;
        guard
            .as_ref()
            .expect("admitted grouped cache")
            .invalidate_field(group, field)
            .await
    }

    /// 业务作用：使当前组及启动时声明的关联组失效。
    /// 参数说明：`group` 为失效来源。
    /// 返回：领域失效结果；关闭后拒绝，不在运行期扩大关联表。
    pub async fn invalidate_group(&self, group: &str) -> anyhow::Result<()> {
        let guard = self.0.enter().await?;
        guard
            .as_ref()
            .expect("admitted grouped cache")
            .invalidate_group(group)
            .await
    }
}

#[cfg(all(feature = "cache", feature = "redis"))]
impl Application {
    /// 业务作用：取得受管命名分组缓存，不要求业务重复建连或编写关闭流程。
    /// 参数说明：`name` 为 grouped_caches 中的名称。
    /// 返回：共享调用 owner；未知名称或关闭时拒绝。
    pub async fn grouped_cache(&self, name: &str) -> ApplicationResult<ManagedGroupedCache> {
        Ok(self
            .named_resource::<ManagedGroupedCache>(name)
            .await?
            .clone())
    }
}

/// 业务作用：按固定原因归类分组缓存装配失败。
/// 参数说明：`message` 不含配置值。
/// 返回：准备阶段错误。
fn error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ComponentId::Application, ApplicationPhase::Prepare, message)
}
