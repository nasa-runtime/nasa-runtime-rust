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
/// 复用 `nalog::LogManager` 的候选准备与安装语义，失败保留当前已安装输出。`manager` 由组件与
/// 热重应用句柄共享，Bootstrap 立即把显式刷盘动作压到 active stack 底部，所以日志在其余资源之后
/// 最后退出；同步互斥锁串行化准备、安装与关闭，不跨 await 持有，但同步文件 I/O 仍可能阻塞。
pub(crate) struct LogComponent {
    /// Bootstrap 创建后与配置热更和最终关闭动作共享的日志管理器。
    manager: Option<Arc<StdMutex<LogManager>>>,
}

impl LogComponent {
    /// 业务作用：创建尚未启动早期控制台的日志组件。
    ///
    /// 参数说明：无。
    ///
    /// 返回：未持有 LogManager 的组件；不安装全局订阅器或创建日志文件，等待 Bootstrap 接管初始化。
    pub(crate) fn new() -> Self {
        Self {
            manager: None,
        }
    }
}

impl ApplicationComponent for LogComponent {
    /// 业务作用：返回日志组件稳定身份。
    ///
    /// 参数说明：无。
    ///
    /// 返回：固定的 ComponentId::Log，供 Runner 归类生命周期与配置应用状态，不读取日志运行态。
    fn id(&self) -> ComponentId {
        ComponentId::Log
    }

    /// 业务作用：读取本地 `log` 段并启动早期控制台日志。
    ///
    /// 参数说明：
    ///
    /// - `context`：提供已完成同步预检的初始配置视图。
    ///
    /// 返回：控制台引导完成后发布初始化标记并登记最终关闭动作；配置绑定或 pattern 非法时返回 Bootstrap 错误。
    /// 此阶段不启用文件输出；底层全局订阅器与回收线程的初始化约束仍由 nalog 承担。
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

    /// 业务作用：用最终配置接入文件日志与最终级别，并登记热重应用句柄。
    ///
    /// 句柄在**首次成功 apply 之后**登记：这样热刷新驱动可见的运行态一定已经初始化，
    /// 不存在"重应用先于首次应用"的乱序窗口。
    ///
    /// 参数说明：
    ///
    /// - `context`：提供最终配置树和应用名的 Start 上下文。
    ///
    /// 返回：完整日志配置成功应用后登记热更处理器；未引导、配置非法或文件/过滤器应用失败时返回 Start 错误。
    /// SQL 输出指令在此冻结，后续只更新 log 段时仍沿用它；同步应用可能等待文件 I/O，不创建独立超时。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let application = context.application();
            let snapshot = application.config();
            let cfg = log_config_or_default(snapshot.value(), ApplicationPhase::Start)?;
            #[cfg(feature = "mapper-observability")]
            let sql_directive = sql_log_directive(snapshot.value(), ApplicationPhase::Start)?;
            #[cfg(feature = "mapper-observability")]
            let cfg = {
                let mut cfg = cfg;
                merge_sql_directive(&mut cfg.level, &sql_directive);
                cfg
            };
            let app_name = application.info().name().to_owned();
            let manager = self.manager.clone().ok_or_else(|| {
                ApplicationError::new(
                    ComponentId::Log,
                    ApplicationPhase::Start,
                    "log manager was not bootstrapped before start",
                )
            })?;
            apply_log_config(&manager, &cfg, &app_name, ApplicationPhase::Start)?;
            // 日志运行态交给配置热刷新驱动，SQL 连接策略保持启动时冻结值，
            // 运行期 `log` 段变化由它重应用并如实记入 ReloadStatus。
            application.register_config_applier(Arc::new(LogReloadApplier {
                manager,
                app_name,
                #[cfg(feature = "mapper-observability")]
                sql_directive,
            }));
            Ok(())
        })
    }
}

/// 在反向清理链末尾关闭文件日志并等待后台刷盘线程退出的动作。
///
/// 管理器仍可能被热重应用句柄共享，所以动作持有同一个互斥容器并调用幂等关闭；真正可能阻塞的
/// guard 释放放到 blocking 任务中，Runner 对 JoinHandle 的等待继续服从全局停机 deadline。
struct LogShutdown {
    /// 首次调用移交给关闭任务的管理器，取走后本动作不再重试。
    manager: Option<Arc<StdMutex<LogManager>>>,
    /// 与公开日志句柄共享的初始化状态；刷盘完成后才撤销，避免提前报告已关闭。
    runtime: Arc<crate::capabilities::LogRuntimeState>,
}

impl ShutdownAction for LogShutdown {
    /// 业务作用：返回不含日志目录等配置值的稳定动作名称。
    ///
    /// 参数说明：无。
    ///
    /// 返回：固定的 log-runtime，供停机报告归因，不包含文件路径或配置内容。
    fn label(&self) -> &'static str {
        "log-runtime"
    }

    /// 业务作用：关闭文件输出并等待后台刷盘线程退出。
    ///
    /// 参数说明：
    ///
    /// - `_context`：Runner 在动作外层施加全局剩余预算，本实现不创建新的独立超时。
    ///
    /// 返回：首次移交管理器后等待阻塞关闭任务，确认返回才撤销初始化标记；后台任务失败返回 Stopping 错误。
    /// 后续调用因管理器已取走而直接成功，不重试前次关闭。等待取消不会终止已启动的 blocking 任务，也不证明刷盘已完成。
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
                    .shutdown();
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
            // 只有后台关闭确认完成才撤销初始化事实，超时或 JoinError 不能伪装成已完成刷盘。
            self.runtime.close();
            Ok(())
        })
    }
}

/// 把日志运行态共享给配置热刷新驱动的重应用句柄。
///
/// last-known-good 契约由 nalog 保证：先准备候选，再安装过滤器及输出，失败保留已安装状态；
/// 句柄只负责把候选树翻译成一次 apply 调用并上抛结果。
struct LogReloadApplier {
    /// 与生命周期组件共用的日志所有者，串行化热更与关闭。
    manager: Arc<StdMutex<LogManager>>,
    /// 固定应用名，用于解析候选日志上下文，不随 log 段热更漂移。
    app_name: String,
    /// 启动时冻结的 SQLx 输出指令，热更业务过滤串时保持该门禁。
    #[cfg(feature = "mapper-observability")]
    sql_directive: String,
}

impl ConfigApplier for LogReloadApplier {
    /// 业务作用：返回句柄负责的组件身份。
    ///
    /// 参数说明：无。
    ///
    /// 返回：ComponentId::Log，供热更驱动记录该候选的应用状态，不表示候选已安装。
    fn component(&self) -> ComponentId {
        ComponentId::Log
    }

    /// 业务作用：在发布锁外准备候选日志文件和过滤器，不修改当前输出。
    ///
    /// 参数说明：
    ///
    /// - `candidate`：已通过整帧校验、尚未发布的候选配置树。
    ///
    /// 返回：持有已准备文件、过滤器与回收许可的候选；失败保持当前输出，但准备阶段可能已创建目录或文件。
    /// 此过程在宿主配置发布锁外、日志管理器锁内执行；成功不会自动安装，SQL 指令沿用启动时冻结值。
    fn prepare(
        &self,
        candidate: &Value,
    ) -> ApplicationResult<Box<dyn crate::reload::PreparedConfigApply>> {
        let cfg = log_config_or_default(candidate, ApplicationPhase::Running)?;
        #[cfg(feature = "mapper-observability")]
        let cfg = {
            let mut cfg = cfg;
            merge_sql_directive(&mut cfg.level, &self.sql_directive);
            cfg
        };
        let prepared = self
            .manager
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .prepare(&cfg, &LogContext::with_app_name(&self.app_name))
            .map_err(|error| {
                ApplicationError::with_source(
                    ComponentId::Log,
                    ApplicationPhase::Running,
                    "log candidate preparation failed",
                    error,
                )
            })?;
        Ok(Box::new(PreparedLogApply {
            manager: self.manager.clone(),
            prepared,
        }))
    }
}

/// 候选保留旧日志守卫，直到配置发布锁释放后再回收。
struct PreparedLogApply {
    /// 安装候选时操作的同一管理器，不新建日志所有者。
    manager: Arc<StdMutex<LogManager>>,
    /// 尚未消费的候选或已安装后保留的旧 guard，释放时交由 nalog 回收。
    prepared: nalog::PreparedLogConfig,
}

impl crate::reload::PreparedConfigApply for PreparedLogApply {
    /// 业务作用：安装已完成 I/O 准备的日志候选，保留旧输出的回收责任。
    ///
    /// 参数说明：无。
    ///
    /// 返回：过滤器安装成功后切换 pattern 与 writer，旧 guard 留在候选中等待锁外回收；被拒绝时保留当前输出。
    /// 候选的过滤器仅消费一次，失败也不能直接复用同一候选重试；成功不意味着旧输出已经刷盘退出。
    fn install(&mut self) -> ApplicationResult<()> {
        self.manager
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .install(&mut self.prepared)
            .map_err(|error| {
                ApplicationError::with_source(
                    ComponentId::Log,
                    ApplicationPhase::Running,
                    "log candidate installation failed",
                    error,
                )
            })
    }

    /// 业务作用：其它配置目标未能提交时恢复原有日志过滤器、模板和输出。
    ///
    /// 参数说明：无。
    ///
    /// 返回：精确旧态恢复时成功；恢复资格或过滤器不可用时返回错误，由宿主关闭业务准入。
    fn rollback(&mut self) -> ApplicationResult<()> {
        self.manager
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .rollback(&mut self.prepared)
            .map_err(|error| {
                ApplicationError::with_source(
                    ComponentId::Log,
                    ApplicationPhase::Running,
                    "log candidate rollback failed",
                    error,
                )
            })
    }
}

/// 业务作用：在互斥锁内执行一次日志配置应用。
///
/// 参数说明：
///
/// - `manager`：组件与热刷新句柄共享的日志运行态。
/// - `cfg`：待应用的完整日志配置。
/// - `app_name`：构造日志上下文使用的稳定应用名。
/// - `phase`：失败时写入错误上下文的生命周期阶段。
///
/// 返回：管理器完成候选准备及安装时成功，失败转换为 Log 组件错误并保留当前已安装输出。
/// 持锁期间可能执行文件 I/O；已创建候选的清理由 nalog 承担，不承诺撤销准备阶段的目录或文件创建。
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

/// 业务作用：在不改变任何 appender 的前提下校验候选配置树中的 `log` 段。
///
/// 供配置热刷新在发布候选前使用；段缺失是合法的（等价默认控制台配置），只有非法结构才拒绝整帧。
///
/// 参数说明：
///
/// - `tree`：合并、插值完成但尚未发布的候选配置树。
/// - `phase`：本次无副作用校验所属的生命周期阶段。
///
/// 返回：可绑定为 LogConfig，或 log 段缺省时成功；启用 SQL 观测时先校验其指令配置，非法结构返回阶段错误。
/// 不打开文件、不解析最终日志过滤器，也不证明目录权限和 pattern 在后续准备时一定可用。
pub(crate) fn validate_log_section(tree: &Value, phase: ApplicationPhase) -> ApplicationResult<()> {
    #[cfg(feature = "mapper-observability")]
    sql_log_directive(tree, phase)?;
    let Some(section) = tree.get("log") else {
        return Ok(());
    };
    naml::strict::bind::<LogConfig>(section.clone())
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

/// 业务作用：从 YAML 派生统一 SQL target 过滤条件，配置冲突在 I/O 前失败。
///
/// 参数说明：
/// - `tree`：`tree` 为完整配置。
/// - `phase`：`phase` 决定安全错误所属阶段。
///
/// 返回：不含输入材料的固定 SQLx 指令或配置错误。
#[cfg(feature = "mapper-observability")]
fn sql_log_directive(tree: &Value, phase: ApplicationPhase) -> ApplicationResult<String> {
    namapper_core::observability::config::console_directive(tree)
        .map_err(|message| ApplicationError::new(ComponentId::Log, phase, message))
}

/// 业务作用：热刷新全局日志时保留冻结的 SQL 输出门禁，避免把连接事件打开后意外屏蔽。
///
/// 参数说明：
/// - `filter`：`filter` 为业务日志过滤串。
/// - `directive`：`directive` 为启动冻结的 SQLx target 指令。
///
/// 返回：原地合并过滤串；其它 target 保持业务配置。
#[cfg(feature = "mapper-observability")]
fn merge_sql_directive(filter: &mut String, directive: &str) {
    let mut entries: Vec<_> = filter
        .split(',')
        .filter(|entry| !entry.trim().starts_with("sqlx::query="))
        .map(str::to_owned)
        .collect();
    entries.push(directive.to_owned());
    // 参数补充事件与连接语句使用同一开关，不能因全局 info 而静默丢弃。
    entries.push(directive.replace("sqlx::query=", "namapper::parameters="));
    *filter = entries.join(",");
}

/// 业务作用：从配置树读取可选的 `log` 段；段缺失时返回 `None`。
///
/// 参数说明：
///
/// - `tree`：完整配置树；启动路径传当前快照，热刷新路径传候选树。
/// - `phase`：读取发生时所属的生命周期阶段。
///
/// 返回：仅缺少 log 键时为 None，合法段返回配置；null、类型或未知字段不符合绑定合同时返回阶段错误。
/// 不进行文件 I/O 或应用日志设置，缺省与显式非法值不能相互替代。
fn optional_log_config(
    tree: &Value,
    phase: ApplicationPhase,
) -> ApplicationResult<Option<LogConfig>> {
    let Some(section) = tree.get("log") else {
        return Ok(None);
    };
    naml::strict::bind(section.clone())
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

/// 业务作用：从配置树读取 `log` 段，缺失时回退到 nalog 的 `serde(default)` 缺省配置。
///
/// 参数说明：
///
/// - `tree`：完整配置树；启动路径传当前快照，热刷新路径传候选树。
/// - `phase`：读取或构造默认值发生时所属的生命周期阶段。
///
/// 返回：显式段绑定成功时使用该配置，键缺省时从空对象构造 nalog 默认值；任何绑定失败直接返回错误，不降级覆盖非法输入。
/// 只构造配置，不读取环境重新插值，也不创建目录或安装输出。
fn log_config_or_default(tree: &Value, phase: ApplicationPhase) -> ApplicationResult<LogConfig> {
    match optional_log_config(tree, phase)? {
        Some(cfg) => Ok(cfg),
        None => naml::strict::bind(Value::Object(Default::default())).map_err(|error| {
            ApplicationError::with_source(
                ComponentId::Log,
                phase,
                "cannot construct default log configuration",
                error,
            )
        }),
    }
}
