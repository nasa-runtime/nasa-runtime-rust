//! 复用命名 Redis 来源的分组缓存标准装配。

#[cfg(all(feature = "cache", feature = "redis"))]
use crate::Application;
use crate::{ApplicationError, ApplicationPhase, ApplicationResult, ComponentId, PrepareContext};
use std::collections::BTreeMap;
#[cfg(all(feature = "cache", feature = "redis"))]
use std::sync::Arc;

/// 单个命名缓存的准备计划，仅复用已声明的 Redis 来源，不自行创建连接池。
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(not(all(feature = "cache", feature = "redis")), allow(dead_code))]
struct Plan {
    /// 缺省关闭；关闭项仍参与声明结构解析，但不创建缓存或执行探针。
    #[serde(default)]
    enabled: bool,
    /// 复用的受管 Redis 来源名，缺省为 default。
    redis_ref: Option<String>,
    /// 每次回填后的字段保留秒数，缺省一天、最大一年；零表示不设置过期。
    backstop_ttl_secs: Option<u64>,
    /// 来源组到直接关联组的失效关系，不递归展开其它来源的关系。
    #[serde(default)]
    clear_refs: BTreeMap<String, Vec<String>>,
}

/// 业务作用：装配命名分组缓存，复用来源连接并在业务之前验证 field TTL。
///
/// 参数说明：
/// - `context`：提供本次应用配置、已准备的 Redis 来源、命名资源登记及逆序清理责任。
///
/// 返回：缺省配置或全部启用计划完成字段 TTL 检查并登记时成功；声明非法、容量超限、缺能力或后端检查失败时拒绝。
/// 已登记的前序计划不会在本函数内撤销，由 Runner 执行已登记的清理动作；TTL 为零时不执行后端过期探针。
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
            // 先确认配置要求的字段过期能力，避免公开会持续留下无过期数据的缓存入口。
            cache
                .verify_field_ttl()
                .await
                .map_err(|_| error("grouped cache field TTL readiness failed"))?;
            let owner = crate::managed_adapters::ManagedAdapter::new(Arc::new(cache));
            // 先交出关闭责任再发布句柄，后续资源登记失败仍能关闭本次已创建的适配器。
            context.activate(Box::new(crate::managed_adapters::AdapterShutdown(
                owner.clone(),
            )));
            context.register_resource(Some(&name), ManagedGroupedCache(owner))?;
        }
    }
    Ok(())
}

/// 命名分组缓存的调用入口，保留独立回源语义并共享永久关闭门禁。
/// 字段 0 持有同一适配器；克隆句柄不绕过关闭门禁，也不新建后端连接。
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
    ///
    /// 参数说明：
    /// - `group`：缓存组的 Redis Hash key，也是整组失效的边界。
    /// - `field`：组内单条缓存的 Hash field，业务应控制其基数。
    /// - `loader`：未命中时的回源闭包，应避免副作用；跨节点等待到期后允许并行回源。
    ///
    /// 返回：缓存命中或回源并序列化成功时返回值，关闭、回源或序列化失败时返回错误；后端读写异常按底层缓存策略降级。
    /// 调用持有资源读守卫直到结束，显式失效仍可能被旧 loader 回填覆盖，不承诺强一致或跨节点唯一执行。
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
    ///
    /// 参数说明：
    /// - `group`：待清理字段所属的 Redis Hash key，不触发关联组失效。
    /// - `field`：该组内待删除的单条缓存字段。
    ///
    /// 返回：后端确认 HDEL 时成功，字段本来不存在也成功；关闭或后端失败时返回错误。
    /// 成功不排除并发 loader 随后回填，后端错误也不证明删除未发生。
    pub async fn invalidate_field(&self, group: &str, field: &str) -> anyhow::Result<()> {
        let guard = self.0.enter().await?;
        guard
            .as_ref()
            .expect("admitted grouped cache")
            .invalidate_field(group, field)
            .await
    }

    /// 业务作用：使当前组及启动时声明的关联组失效。
    ///
    /// 参数说明：
    /// - `group`：启动时关联表中的失效来源组，同时也是必须删除的目标 Hash key。
    ///
    /// 返回：后端确认删除当前组及其关联目标集合时成功；关闭或后端失败返回错误，不在运行期修改关联表。
    /// 并发回填仍可能重新创建缓存，错误也不表示所有目标均未删除。
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
    ///
    /// 参数说明：
    /// - `name`：grouped_caches 中已启用并完成准备的命名资源键，按原值查找。
    ///
    /// 返回：共享同一永久关闭门禁的句柄；未知名称、资源尚未发布或资源表关闭时返回错误。
    /// 取得句柄不预留调用权，随后每次操作仍须通过自身准入检查。
    pub async fn grouped_cache(&self, name: &str) -> ApplicationResult<ManagedGroupedCache> {
        Ok(self
            .named_resource::<ManagedGroupedCache>(name)
            .await?
            .clone())
    }
}

/// 业务作用：按固定原因归类分组缓存装配失败。
///
/// 参数说明：
/// - `message`：由调用处选择的不含配置值的固定错误摘要。
///
/// 返回：归属 Application 组件 Prepare 阶段的统一错误，不附加底层来源，也不执行清理或重试。
fn error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ComponentId::Application, ApplicationPhase::Prepare, message)
}
