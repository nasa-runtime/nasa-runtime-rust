use crate::{ApplicationError, ApplicationPhase, ApplicationResult, ComponentId};

/// 校验存在缓存查询的 Mapper 已在业务启动 Hook 中显式安装默认 L2。
///
/// 缓存不属于本版内置组件，但 `cache = true` 查询在缺少 L2 时会静默绕过缓存。Service 对外提供
/// 流量前执行该断言，可以保留 Hook 显式装配边界，同时把遗漏装配变成可定位的启动失败。
///
/// # 参数
///
/// 本函数无参数；二进制没有缓存查询时恒为成功。
pub(crate) fn ensure_mapper_l2_installed() -> ApplicationResult<()> {
    namapper::assert_l2_cache_installed_for_cached_queries().map_err(|error| {
        ApplicationError::with_source(
            ComponentId::Application,
            ApplicationPhase::Ready,
            "mapper declares cache-enabled queries but no default L2 cache is installed in the startup hook",
            error,
        )
    })
}
