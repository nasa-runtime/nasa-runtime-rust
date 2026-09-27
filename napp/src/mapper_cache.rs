use crate::{ApplicationError, ApplicationPhase, ApplicationResult, ComponentId};

/// 业务作用：在 Service Ready 或 Batch 工作负载之前确认缓存查询的默认 L2 已装配。
/// 参数说明：无。
/// 返回：无缓存查询或已有默认 L2 时成功；缺少缓存资源时阻断工作负载。
pub(crate) fn ensure_mapper_l2_installed() -> ApplicationResult<()> {
    namapper_core::assert_l2_cache_installed_for_cached_queries().map_err(|error| {
        ApplicationError::with_source(
            ComponentId::Application,
            ApplicationPhase::Ready,
            "mapper declares cache-enabled queries but no default L2 cache is installed",
            error,
        )
    })
}

/// 显式选择受管 L2 时所需的来源绑定；关闭时不建立资源。
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedMapperCacheConfig {
    #[serde(default)]
    enabled: bool,
    redis_ref: Option<String>,
}

/// 业务作用：在 Redis 已建连后从 mapper_cache 段装配默认 L2，复用已持有的来源连接。
/// 参数说明：`context` 提供应用配置、资源目录和逆序清理链。
/// 返回：成功安装 owner 或未启用时成功；来源、能力或重复安装不满足时阻断启动。
pub(crate) async fn install_managed(
    context: &mut crate::StartContext<'_>,
) -> ApplicationResult<()> {
    let app = context.application().clone();
    let config = app.config();
    let Some(value) = config.value().get("mapper_cache") else {
        return Ok(());
    };
    let plan: ManagedMapperCacheConfig = serde_json::from_value(value.clone())
        .map_err(|error| mapper_error("invalid managed mapper cache configuration", error))?;
    if !plan.enabled {
        return Ok(());
    }
    let client =
        crate::redis::redis_handle(&app, plan.redis_ref.as_deref().unwrap_or("default")).await?;
    let cache = std::sync::Arc::new(namapper_core::RedisMapperL2Cache::new(client.conn()));
    cache
        .assert_hash_field_ttl_supported()
        .await
        .map_err(|error| mapper_error("managed mapper cache requires hash field expiry", error))?;
    let cache = std::sync::Arc::new(namapper_core::SingleFlightMapperL2Cache::new(cache));
    let owner = namapper_core::MapperCacheOwner::install(cache).map_err(|error| {
        mapper_error(
            "managed mapper cache conflicts with an existing installation",
            error,
        )
    })?;
    // 安装之后立即登记撤销责任，后续任何资源或组件失败都不能遗留全局槽。
    context.activate(Box::new(MapperCacheShutdown(owner)));
    Ok(())
}

/// 业务作用：用固定摘要归类 Mapper 装配错误，不公开源连接参数。
/// 参数说明：`message` 为稳定摘要；`error` 为内部原因。
/// 返回：带来源链的启动错误。
fn mapper_error(message: &'static str, error: impl Into<anyhow::Error>) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Redis, ApplicationPhase::Start, message, error)
}

struct MapperCacheShutdown(namapper_core::MapperCacheOwner);

impl crate::ShutdownAction for MapperCacheShutdown {
    /// 业务作用：为默认缓存 owner 提供稳定清理名称。
    /// 参数说明：无。
    /// 返回：固定动作名称。
    fn label(&self) -> &'static str {
        "mapper-cache"
    }

    /// 业务作用：撤下默认缓存并等待在途查询和回填归还 Redis 依赖。
    /// 参数说明：`context` 为宿主共享停机预算。
    /// 返回：排干后成功；超时保持关闭并报告尚未完成。
    fn shutdown<'a>(
        &'a mut self,
        context: &'a crate::ShutdownContext,
    ) -> crate::ApplicationFuture<'a> {
        Box::pin(async move {
            self.0
                .shutdown(tokio::time::Instant::from_std(context.deadline()))
                .await
                .map_err(|error| {
                    ApplicationError::with_source(
                        ComponentId::Redis,
                        ApplicationPhase::Stopping,
                        "mapper cache shutdown incomplete",
                        error,
                    )
                })
        })
    }
}
