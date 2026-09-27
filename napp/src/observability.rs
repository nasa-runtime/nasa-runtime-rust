//! 与业务健康隔离的统一指标出口生命周期。

use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ComponentId, ReadyContext, ShutdownAction, ShutdownContext, StartContext,
};
use nafana::observability::{Exporter, ListenerMode, ObservabilityConfig};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// 自动装配的指标出口；不需要业务注册 Router 或后台任务。
pub struct ObservabilityComponent {
    exporter: Option<Arc<Exporter>>,
    task: Option<ApplicationFuture<'static>>,
}

impl ObservabilityComponent {
    /// 业务作用：建立尚无端口、队列或网络客户端的生命周期节点。
    /// 参数说明：无。
    /// 返回：等待最终配置的观测组件。
    pub fn new() -> Self {
        Self {
            exporter: None,
            task: None,
        }
    }
}
impl Default for ObservabilityComponent {
    /// 业务作用：提供与显式构造一致的无副作用默认组件。
    /// 参数说明：无。
    /// 返回：尚未激活的组件。
    fn default() -> Self {
        Self::new()
    }
}

impl ApplicationComponent for ObservabilityComponent {
    /// 业务作用：给出口生命周期提供稳定归属。
    /// 参数说明：无。
    /// 返回：独立观测组件标识。
    fn id(&self) -> ComponentId {
        ComponentId::Observability
    }

    /// 业务作用：在业务 Hook 前冻结身份并登记同源指标资源。
    /// 参数说明：`context` 提供最终配置与受管资源登记。
    /// 返回：配置或身份冲突时在监听前失败；关闭配置时仅输出未外送摘要。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let app = context.application();
            let root = app.config();
            let config = ObservabilityConfig::from_root(root.value()).map_err(start_error)?;
            app.metrics_hub()
                .register_legacy_source(nafana::metrics_source())
                .map_err(|_| start_error("interface metric descriptor conflict"))?;
            if !config.enabled {
                tracing::info!(target:"napp::observability", event="metrics_not_exported", "指标 source 可用，未启用外部出口");
                return Ok(());
            }
            #[cfg(not(feature = "web"))]
            if config.scrape_enabled() && config.prometheus.scrape.listener == ListenerMode::Web {
                return Err(start_error("scrape.listener=web requires web feature"));
            }
            let identity = application_identity(app)?
                .ok_or_else(|| start_error("enabled observability identity is unavailable"))?;
            let exporter = Exporter::new_with_refresh(
                config,
                identity,
                app.metrics_hub(),
                &app.secrets(),
                Some(Arc::new(ApplicationMetricRefresh(app.clone()))),
            )
            .map_err(start_error)?;
            if let Some(exporter) = exporter {
                context.register_resource(None, exporter.clone())?;
                self.exporter = Some(exporter);
            }
            Ok(())
        })
    }

    /// 业务作用：在开放业务路由前绑定专用端口并暂存后台任务。
    /// 参数说明：`context` 提供反向停机 action 登记。
    /// 返回：绑定失败阻止观测启动；成功仅完成装配，全部组件静态门禁通过后任务才可执行。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let Some(exporter) = self.exporter.clone() else {
                return Ok(());
            };
            let listener = if exporter.config.scrape_enabled()
                && exporter.config.prometheus.scrape.listener == ListenerMode::Dedicated
            {
                Some(
                    tokio::net::TcpListener::bind(exporter.config.bind())
                        .await
                        .map_err(|_| start_error("dedicated metrics listener could not bind"))?,
                )
            } else {
                None
            };
            let stop = CancellationToken::new();
            // 在任务接管前登记取消权威，后续启动失败仍会关闭已经绑定的 listener。
            context.activate(Box::new(StopExporter(stop.clone())));
            self.task = Some(Box::pin(async move {
                exporter.run(listener, stop).await;
                Ok(())
            }));
            Ok(())
        })
    }

    /// 业务作用：待所有 Ready 静态登记与 initializer 任务工厂构造完成后核对单批容量，阻止必然拒绝的出口启动。
    /// 参数说明：无。
    /// 返回：关闭出口或容量充足时成功；不足返回 Ready 阶段错误，禁止暂存的出口与业务终端开始运行。
    fn validate_ready(&self) -> ApplicationResult<()> {
        // 不能以自身 Ready 时的容量代替全表终态；后置组件及任务工厂仍可增加预留或原生 cell。
        if let Some(exporter) = &self.exporter {
            exporter.validate_capacity().map_err(|message| {
                ApplicationError::new(ComponentId::Observability, ApplicationPhase::Ready, message)
            })?;
        }
        Ok(())
    }

    /// 业务作用：把唯一出口任务交给 Runner 接管，等待最终门禁与统一执行许可。
    /// 参数说明：无。
    /// 返回：仅移交一次；正常停机由已登记 action 取消。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.task
            .take()
            .map(|task| ("observability-exporter", task))
    }
}

struct StopExporter(CancellationToken);

impl Drop for StopExporter {
    /// 业务作用：在装配中止或控制对象释放时撤销出口任务，防止后台任务超出 Application 生命周期。
    /// 参数说明：无。
    /// 返回：发布幂等取消信号，不阻塞析构。
    fn drop(&mut self) {
        self.0.cancel();
    }
}
struct ApplicationMetricRefresh(Application);
impl nafana::observability::MetricRefresh for ApplicationMetricRefresh {
    /// 业务作用：在受管快照前刷新 Outbox 等外部事实源，失败仍保留其它指标。
    /// 参数说明：无。
    /// 返回：刷新 future；外层出口负责总超时与并发门禁。
    fn refresh(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let _ = self.0.refresh_metric_sources().await;
        })
    }
}
impl ShutdownAction for StopExporter {
    /// 业务作用：标识出口取消步骤。
    /// 参数说明：无。
    /// 返回：固定 action 名。
    fn label(&self) -> &'static str {
        "stop-observability-exporter"
    }
    /// 业务作用：先关闭指标生产与抓取，再由监督器收割后台任务。
    /// 参数说明：`context` 为统一停机预算。
    /// 返回：发出取消信号，不等待外部平台恢复。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            self.0.cancel();
            Ok(())
        })
    }
}

/// 业务作用：给受管 Web 自动挂接 metrics，独立于 health 配置。
/// 参数说明：`application` 提供已经冻结的出口资源。
/// 返回：web 模式的 Router；关闭或专用监听时返回 None。
#[cfg(feature = "web")]
pub async fn web_metrics_router(
    application: &Application,
) -> ApplicationResult<Option<axum::Router<Application>>> {
    let config =
        ObservabilityConfig::from_root(application.config().value()).map_err(start_error)?;
    if !config.scrape_enabled() || config.prometheus.scrape.listener != ListenerMode::Web {
        return Ok(None);
    }
    let exporter = application.resources().get::<Arc<Exporter>>().await?;
    Ok(Some(exporter.router().with_state(())))
}

/// 业务作用：把观测配置错误归为启动门禁而不带外部错误正文。
/// 参数说明：`message` 为受控配置摘要。
/// 返回：可安全打印的应用错误。
fn start_error(message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(
        ComponentId::Observability,
        ApplicationPhase::Start,
        message.into(),
    )
}

/// 业务作用：为 scrape、remote write、OTLP 与通知提供唯一的启动身份派生入口。
/// 参数说明：`application` 提供冻结应用名、启动时刻与同代配置。
/// 返回：观测启用时的固定身份，关闭时 None；别名冲突在任何 listener 或 exporter 创建前失败。
pub(crate) fn application_identity(
    application: &Application,
) -> ApplicationResult<Option<nafana::observability::Identity>> {
    let snapshot = application.config();
    let config = ObservabilityConfig::from_root(snapshot.value()).map_err(start_error)?;
    if !config.enabled {
        return Ok(None);
    }
    let nanos = application
        .info()
        .started_at()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX));
    let local_instance = format!("{}-{nanos}", std::process::id());
    config
        .freeze_identity(application.info().name(), &local_instance, snapshot.value())
        .map(Some)
        .map_err(start_error)
}
