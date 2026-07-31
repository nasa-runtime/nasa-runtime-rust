use std::sync::{Arc, Mutex as StdMutex};

use nalog::{LogConfig, LogContext, LogManager};
use serde_json::Value;

use crate::{
    reload::ConfigApplier, ApplicationComponent, ApplicationError, ApplicationFuture,
    ApplicationPhase, ApplicationResult, BootstrapContext, ComponentId, ShutdownAction,
    ShutdownContext, StartContext,
};

/// 两阶段日志组件：Bootstrap 起早期控制台，Start 应用最终文件日志。
///
/// 复用 `nalog::LogManager` 的原子 apply 语义（失败保留旧 guard/级别/pattern）。`manager` 由组件与
/// 热重应用句柄共享，Bootstrap 立即把显式刷盘动作压到 active stack 底部，所以日志在其余资源之后
/// 最后退出；同步互斥锁只保护单次 apply，调用方不跨 await 持有。
pub(crate) struct LogComponent {
    manager: Option<Arc<StdMutex<LogManager>>>,
}

impl LogComponent {
    /// 创建尚未启动早期控制台的日志组件。
    ///
    /// # 参数
    ///
    /// 本方法无参数；`manager` 在 Bootstrap 阶段填充。
    pub(crate) fn new() -> Self {
        Self { manager: None }
    }
}

impl ApplicationComponent for LogComponent {
    /// 返回日志组件稳定身份。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Runner 使用该身份归类日志相关错误。
    fn id(&self) -> ComponentId {
        ComponentId::Log
    }

    /// 读取本地 `log` 段并启动早期控制台日志。
    ///
    /// # 参数
    ///
    /// - `context`：提供已完成同步预检的初始配置视图。
    fn bootstrap<'a>(&'a mut self, context: &'a mut BootstrapContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            // 段缺失 → None → nalog 默认 info 控制台。
            let snapshot = context.application().config();
            let cfg = optional_log_config(snapshot.value(), ApplicationPhase::Bootstrap)?;
            let manager = LogManager::try_bootstrap(cfg.as_ref()).map_err(|error| {
                ApplicationError::with_source(
                    ComponentId::Log,
                    ApplicationPhase::Bootstrap,
                    "early console logging bootstrap failed",
                    error,
                )
            })?;
            let manager = Arc::new(StdMutex::new(manager));
            self.manager = Some(manager.clone());
            let runtime = context.application().log_runtime();
            runtime.publish_initialized();
            // 日志最先启动，因此清理动作最先压栈、最终最后弹出；主错误和全部停机报告都能先写完。
            context.activate(Box::new(LogShutdown {
                manager: Some(manager),
                runtime,
            }));
            Ok(())
        })
    }

    /// 用最终配置接入文件日志与最终级别，并登记热重应用句柄。
    ///
    /// 句柄在**首次成功 apply 之后**登记：这样热刷新驱动可见的运行态一定已经初始化，
    /// 不存在"重应用先于首次应用"的乱序窗口。
    ///
    /// # 参数
    ///
    /// - `context`：提供最终配置树和应用名的 Start 上下文。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let application = context.application();
            let snapshot = application.config();
            let cfg = log_config_or_default(snapshot.value(), ApplicationPhase::Start)?;
            let app_name = application.info().name().to_owned();
            let manager = self.manager.clone().ok_or_else(|| {
                ApplicationError::new(
                    ComponentId::Log,
                    ApplicationPhase::Start,
                    "log manager was not bootstrapped before start",
                )
            })?;
            apply_log_config(&manager, &cfg, &app_name, ApplicationPhase::Start)?;
            // 日志是 V1 唯一的可热刷组件：把共享运行态交给配置热刷新驱动，
            // 运行期 `log` 段变化由它重应用并如实记入 ReloadStatus。
            application.register_config_applier(Arc::new(LogReloadApplier { manager, app_name }));
            Ok(())
        })
    }
}

/// 在反向清理链末尾关闭文件日志并等待后台刷盘线程退出的动作。
///
/// 管理器仍可能被热重应用句柄共享，所以动作持有同一个互斥容器并调用幂等关闭；真正可能阻塞的
/// guard 释放放到 blocking 任务中，Runner 对 JoinHandle 的等待继续服从全局停机 deadline。
struct LogShutdown {
    manager: Option<Arc<StdMutex<LogManager>>>,
    /// 与公开日志句柄共享的初始化状态；刷盘完成后才撤销，避免提前报告已关闭。
    runtime: Arc<crate::capabilities::LogRuntimeState>,
}

impl ShutdownAction for LogShutdown {
    /// 返回不含日志目录等配置值的稳定动作名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；名称只用于清理错误归类。
    fn label(&self) -> &'static str {
        "log-runtime"
    }

    /// 关闭文件输出并等待后台刷盘线程退出。
    ///
    /// # 参数
    ///
    /// - `_context`：Runner 在动作外层施加全局剩余预算，本实现不创建新的独立超时。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        let manager = self.manager.take();
        Box::pin(async move {
            let Some(manager) = manager else {
                return Ok(());
            };
            tokio::task::spawn_blocking(move || {
                manager
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .disable_file();
            })
            .await
            .map_err(|error| {
                ApplicationError::with_source(
                    ComponentId::Log,
                    ApplicationPhase::Stopping,
                    "log flush task failed while shutting down file output",
                    error,
                )
            })?;
            self.runtime.close();
            Ok(())
        })
    }
}

/// 把日志运行态共享给配置热刷新驱动的重应用句柄。
///
/// last-known-good 契约由 nalog 保证：apply 先备好新 guard/pattern 再原子换，失败不动旧状态；
/// 句柄只负责把候选树翻译成一次 apply 调用并上抛结果。
struct LogReloadApplier {
    manager: Arc<StdMutex<LogManager>>,
    app_name: String,
}

impl ConfigApplier for LogReloadApplier {
    /// 返回句柄负责的组件身份。
    ///
    /// # 参数
    ///
    /// 本方法无参数；驱动用它把重应用结果记到正确的状态目标上。
    fn component(&self) -> ComponentId {
        ComponentId::Log
    }

    /// 对候选配置树中的 `log` 段执行一次重应用。
    ///
    /// # 参数
    ///
    /// - `candidate`：已通过整帧校验、尚未发布的候选配置树。
    fn apply(&self, candidate: &Value) -> ApplicationResult<()> {
        let cfg = log_config_or_default(candidate, ApplicationPhase::Running)?;
        apply_log_config(
            &self.manager,
            &cfg,
            &self.app_name,
            ApplicationPhase::Running,
        )
    }
}

/// 在互斥锁内执行一次日志配置应用。
///
/// # 参数
///
/// - `manager`：组件与热刷新句柄共享的日志运行态。
/// - `cfg`：待应用的完整日志配置。
/// - `app_name`：构造日志上下文使用的稳定应用名。
/// - `phase`：失败时写入错误上下文的生命周期阶段。
fn apply_log_config(
    manager: &Arc<StdMutex<LogManager>>,
    cfg: &LogConfig,
    app_name: &str,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    manager
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .apply(cfg, &LogContext::with_app_name(app_name))
        .map(|_| ())
        // apply 失败不改动旧 guard/级别（nalog 已保证），这里只把错误如实上抛。
        .map_err(|error| {
            ApplicationError::with_source(
                ComponentId::Log,
                phase,
                "log configuration apply failed",
                error,
            )
        })
}

/// 在不改变任何 appender 的前提下校验候选配置树中的 `log` 段。
///
/// 供配置热刷新在发布候选前使用；段缺失是合法的（等价默认控制台配置），只有非法结构才拒绝整帧。
///
/// # 参数
///
/// - `tree`：合并、插值完成但尚未发布的候选配置树。
/// - `phase`：本次无副作用校验所属的生命周期阶段。
pub(crate) fn validate_log_section(tree: &Value, phase: ApplicationPhase) -> ApplicationResult<()> {
    let Some(section) = tree.get("log") else {
        return Ok(());
    };
    serde_json::from_value::<LogConfig>(section.clone())
        .map(|_| ())
        .map_err(|error| {
            ApplicationError::with_source(
                ComponentId::Log,
                phase,
                "invalid `log` configuration section",
                error,
            )
        })
}

/// 从配置树读取可选的 `log` 段；段缺失时返回 `None`。
///
/// # 参数
///
/// - `tree`：完整配置树；启动路径传当前快照，热刷新路径传候选树。
/// - `phase`：读取发生时所属的生命周期阶段。
fn optional_log_config(
    tree: &Value,
    phase: ApplicationPhase,
) -> ApplicationResult<Option<LogConfig>> {
    let Some(section) = tree.get("log") else {
        return Ok(None);
    };
    serde_json::from_value(section.clone())
        .map(Some)
        .map_err(|error| {
            ApplicationError::with_source(
                ComponentId::Log,
                phase,
                "invalid `log` configuration section",
                error,
            )
        })
}

/// 从配置树读取 `log` 段，缺失时回退到 nalog 的 `serde(default)` 缺省配置。
///
/// # 参数
///
/// - `tree`：完整配置树；启动路径传当前快照，热刷新路径传候选树。
/// - `phase`：读取或构造默认值发生时所属的生命周期阶段。
fn log_config_or_default(tree: &Value, phase: ApplicationPhase) -> ApplicationResult<LogConfig> {
    match optional_log_config(tree, phase)? {
        Some(cfg) => Ok(cfg),
        None => serde_json::from_value(Value::Object(Default::default())).map_err(|error| {
            ApplicationError::with_source(
                ComponentId::Log,
                phase,
                "cannot construct default log configuration",
                error,
            )
        }),
    }
}
