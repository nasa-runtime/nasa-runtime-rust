use crate::{ApplicationError, ApplicationPhase, ApplicationResult, ComponentId};

struct MapperDefaults(namapper_core::MapperDefaultsOwner);

impl crate::ManagedResource for MapperDefaults {
    /// 业务作用：在资源清理边界撤销当前应用的默认 codec 和指标入口。
    /// 参数说明：`_context` 为宿主共享预算，本操作不执行 I/O。
    /// 返回：当前 owner 的默认槽已撤下。
    fn shutdown<'a>(
        &'a mut self,
        _context: &'a crate::ShutdownContext,
    ) -> crate::ApplicationFuture<'a> {
        self.0.close();
        Box::pin(async { Ok(()) })
    }
}

impl crate::Application {
    /// 业务作用：在启动登记窗口安装 Mapper 算法与观测默认值，并纳入回滚和停机。
    /// 参数说明：`codec` 为无独立生命周期的编码策略；`metrics` 为现有观测出口。
    /// 返回：两个槽均可占用且资源登记成功；任一失败立即撤销本次占用。
    pub fn configure_mapper_defaults(
        &self,
        codec: Option<std::sync::Arc<dyn namapper_core::MapperCacheCodec>>,
        metrics: Option<std::sync::Arc<dyn namapper_core::MapperMetrics>>,
    ) -> ApplicationResult<()> {
        self.ensure_user_hook_open("Mapper defaults registration")?;
        let owner =
            namapper_core::MapperDefaultsOwner::install(codec, metrics).map_err(|error| {
                ApplicationError::with_source(
                    ComponentId::Application,
                    ApplicationPhase::UserHook,
                    "Mapper defaults conflict with an existing owner",
                    error,
                )
            })?;
        self.register_managed(MapperDefaults(owner))
    }
}
