use std::{
    future::Future,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
    time::{Duration, Instant as StdInstant},
};

use futures_util::FutureExt;
use tokio::time::{timeout, Instant};

use crate::{
    component::{action_panic_error, ActiveStack, ActiveStep, ShutdownActionCleanup},
    future::StartupCleanup,
    initialization::{
        freeze_plan, order_enabled, own_initializer_future, EnabledInitializer, FrozenInitializer,
        FrozenInitializerPlan, InitializerFailure, InitializerFailureKind, InitializerStage,
        StagedInitializerTask,
    },
    report::{report_shutdown, report_shutdown_summary, ShutdownSummary},
    resources::ResourceOwner,
    shutdown::{
        release_shutdown_panic_payload, ShutdownTaskEntry, ShutdownTaskOutcome, ShutdownTaskReport,
    },
    signal::{SignalBroker, SignalEvent, SignalMode},
    state::TerminalIntent,
    supervisor::{
        SupervisorEvent, TaskCompletion, TaskGroupState, TaskKind, TaskOutcome, TaskSupervisor,
    },
    Application, ApplicationComponent, ApplicationError, ApplicationInfo, ApplicationMode,
    ApplicationPhase, ApplicationResult, ApplicationState, BootstrapContext, ComponentId,
    ConfigView, InitializationContext, PrepareContext, ReadyContext, ShutdownAction,
    ShutdownContext, ShutdownReason, ShutdownSignal, StartContext,
};

/// 生命周期全局启动/停机预算的硬上限；避免外部 `u64` 毫秒配置在 `Instant` 加法处溢出。
pub(crate) const MAX_LIFECYCLE_TIMEOUT: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// Runner 正常完成时的首次终止原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationExitReason {
    /// Batch 应用完成了全部业务任务。
    BatchCompleted,
    /// 管理入口或内部控制面请求停机。
    ShutdownRequested,
    /// 操作系统信号触发停机。
    Signal(ShutdownSignal),
}

/// 一次完整生命周期的正常退出报告。
///
/// `code` 只由首次终止事件决定；清理失败单独保存在报告中，不会反向覆盖正常退出语义。
#[derive(Debug)]
pub struct ApplicationExit {
    reason: ApplicationExitReason,
    code: u8,
    shutdown_failures: Vec<ApplicationError>,
}

impl ApplicationExit {
    /// 业务作用：返回决定本次正常退出的首次事件。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回值不会因后续清理失败改变。
    pub fn reason(&self) -> ApplicationExitReason {
        self.reason
    }

    /// 业务作用：返回进程入口应使用的退出码。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回值已经包含 Batch 信号中断的非零映射。
    pub fn code(&self) -> u8 {
        self.code
    }

    /// 业务作用：返回 active stack 清理过程中收集到的次要错误。
    ///
    /// # 参数
    ///
    /// 本方法无参数；借用只在当前退出报告存活期间有效。
    pub fn shutdown_failures(&self) -> &[ApplicationError] {
        &self.shutdown_failures
    }
}

/// 启动阶段被主失败或进程信号打断的内部分类。
enum StartupStop {
    Failure(ApplicationError),
    Signal(ShutdownSignal),
    Requested,
}

/// Service 进入 Running 后的首次终止分类。
enum ServiceTerminal {
    Requested,
    Signal(ShutdownSignal),
    Failure(ApplicationError),
}

/// active stack 各类步骤的停机计数；字段集合固定，不把业务名称放入指标维度。
#[derive(Default)]
struct ShutdownStepCounts {
    component_actions: usize,
    initializer_actions: usize,
    task_gates: usize,
    business_resources: usize,
    component_resources: usize,
    initializer_resources: usize,
}

/// 宏展开层使用的生命周期执行器。
///
/// Runner 独占组件表、active stack、任务 JoinSet 和终态写权限，从而让所有关键顺序都由一个异步控制流线性化。
#[doc(hidden)]
pub struct ApplicationRunner {
    application: Application,
    supervisor: TaskSupervisor,
    components: Vec<ComponentEntry>,
    active: ActiveStack,
    // 出栈不代表任务已退出；跨 await 保留任务门，外层取消时仍约束资源释放。
    task_gate_in_progress: bool,
    signal_mode: SignalMode,
    startup_timeout: Duration,
    shutdown_timeout: Duration,
}

impl Drop for ApplicationRunner {
    /// 业务作用：在生命周期执行器被直接释放时撤销运行权威，并沿激活栈逆序释放已接管所有权。
    ///
    /// 参数说明：无。
    ///
    /// 返回：无返回值；直接取消留下 Stopping，关闭全局入口与新资源借用，不承诺异步收尾；
    /// 任务尚未析构时由其存活守卫接管清理尾部；已有 Stopped/Failed 保持不变，析构异常仅同步告警。
    fn drop(&mut self) {
        // UserHook 的中止由执行器稍后完成，其 Application 副本或任务捕获的副本可能仍然存活。
        // 先关闭登记门，再在锁外释放任务，不能依赖最后一个 Application 副本的析构来收口。
        self.application.close_user_hook();
        if matches!(
            self.application.state(),
            ApplicationState::Starting | ApplicationState::Ready
        ) {
            // 外层取消没有异步执行器继续推进生命周期；先保留首次终止意图，再撤下 Ready，
            // 使基础设施 getter 和状态观察者拒绝把保留句柄视为仍可服务的实例。
            // 异步 action 与资源 shutdown 未必执行，因此保持 Stopping，不发布 Stopped。
            self.application.set_terminal(TerminalIntent::Failure);
            if let Err(error) = self.begin_stopping() {
                report_shutdown(&error);
            }
        }
        let mut cleanup = CancelledRunnerCleanup {
            application: self.application.clone(),
            active: std::mem::replace(&mut self.active, ActiveStack::new()),
            components: std::mem::take(&mut self.components),
        };
        #[cfg(feature = "redis")]
        {
            self.application.redis_partitions().begin_shutdown();
            if !self.application.redis_partitions().unfinished().is_empty() {
                self.retain_partition_dependencies(cleanup);
                return;
            }
        }
        // 正在等待的任务门已不在栈里，必须先恢复它的释放约束，不能越过它处理 initializer 或业务资源。
        if self.task_gate_in_progress {
            self.supervisor.cancel_for_drop();
            if self.supervisor.has_live_futures() {
                self.defer_cancelled_cleanup(cleanup);
                return;
            }
        }
        while let Some(step) = cleanup.active.pop() {
            if matches!(step, ActiveStep::UserTasks | ActiveStep::InitializerTasks) {
                self.supervisor.cancel_for_drop();
                if self.supervisor.has_live_futures() {
                    self.defer_cancelled_cleanup(cleanup);
                    return;
                }
            } else {
                release_cancelled_step(&cleanup.application, step);
            }
        }
        self.supervisor.cancel_for_drop();
    }
}

/// 任务门之后尚未释放的生命周期所有权，不包含监督器，避免与任务存活守卫形成保留环。
struct CancelledRunnerCleanup {
    application: Application,
    active: ActiveStack,
    components: Vec<ComponentEntry>,
}

impl Drop for CancelledRunnerCleanup {
    /// 业务作用：在同步安全边界或最后一个受管 future 释放后，沿剩余栈逆序归还依赖所有权。
    ///
    /// 参数说明：无。
    ///
    /// 返回：无返回值；仅执行同步释放，不调用异步 shutdown、不发布终态或正常停机摘要。
    fn drop(&mut self) {
        while let Some(step) = self.active.pop() {
            release_cancelled_step(&self.application, step);
        }
        // 部分启动或已经出栈的步骤仍需兜底封口；重复调用不重放已移交的所有权。
        for error in self.application.close_shutdown_tasks() {
            report_shutdown(&error);
        }
        self.application.resources().close();
        // 延迟释放期间可能已有替代实例发布；只能清除属于本实例的全局入口。
        self.application.clear_global();
        // 对象可能持有任务依赖；只有剩余栈与资源归还后才逐项释放，单项展开不截断后续对象。
        for component in self.components.iter_mut().rev() {
            if let Some(error) = component.owner.release() {
                report_shutdown(&error);
            }
        }
    }
}

/// 生命周期只使用一次准备的元数据，阶段执行不再读取扩展对象的动态身份与依赖。
struct ComponentMetadata {
    id: ComponentId,
    dependencies: &'static [ComponentId],
}

/// 组件对象从登记起受保护，元数据成功冻结后才允许执行任何生命周期阶段。
struct ComponentEntry {
    owner: StartupCleanup<Box<dyn ApplicationComponent>>,
    metadata: Option<ComponentMetadata>,
}

impl ComponentEntry {
    /// 业务作用：接管组件对象但不调用扩展方法，登记失败或未运行时也保留析构隔离。
    /// 参数说明：`component` 为调用方移交的生命周期实现。
    /// 返回：尚未读取身份和依赖的组件项；元数据准备前的释放归因于 Application。
    fn new(component: Box<dyn ApplicationComponent>) -> Self {
        Self {
            owner: StartupCleanup::new(
                component,
                ComponentId::Application,
                ApplicationPhase::Stopping,
                "releasing component instance",
            ),
            metadata: None,
        }
    }

    /// 业务作用：为自动附加组件完成与显式声明相同的元数据门禁。
    /// 参数说明：`component` 为框架构造但尚未执行生命周期的组件。
    /// 返回：元数据已经冻结的受保护对象；扩展读取展开时返回 Bootstrap 错误。
    #[cfg(any(
        feature = "config-watch",
        feature = "mapper-observability",
        feature = "observability"
    ))]
    fn prepared(component: Box<dyn ApplicationComponent>) -> ApplicationResult<Self> {
        let mut entry = Self::new(component);
        entry.freeze_metadata()?;
        Ok(entry)
    }

    /// 业务作用：在所有阶段开始前一次性读取稳定元数据，阻止阶段间身份漂移与异常越过回滚边界。
    /// 参数说明：无。
    /// 返回：身份与依赖均可读取时冻结；任一读取展开转为固定 Bootstrap 错误，不读取异常正文。
    fn freeze_metadata(&mut self) -> ApplicationResult<()> {
        if self.metadata.is_some() {
            return Ok(());
        }
        let id = catch_unwind(AssertUnwindSafe(|| self.owner.value().id())).map_err(|payload| {
            release_shutdown_panic_payload(payload);
            ApplicationError::new(
                ComponentId::Application,
                ApplicationPhase::Bootstrap,
                "component identity metadata panicked",
            )
        })?;
        // 取得可信身份后立即更新释放归因；依赖读取失败也不能再次询问扩展对象的身份。
        self.owner = StartupCleanup::new(
            self.owner.take(),
            id,
            ApplicationPhase::Stopping,
            "releasing component instance",
        );
        let dependencies = catch_unwind(AssertUnwindSafe(|| self.owner.value().dependencies()))
            .map_err(|payload| {
                release_shutdown_panic_payload(payload);
                ApplicationError::new(
                    id,
                    ApplicationPhase::Bootstrap,
                    "component dependency metadata panicked",
                )
            })?;
        self.metadata = Some(ComponentMetadata { id, dependencies });
        Ok(())
    }

    /// 业务作用：向排序、配置投影和阶段上下文提供同一冻结元数据，避免再进入扩展代码。
    /// 参数说明：无。
    /// 返回：已通过准备门禁的身份与静态依赖；准备前读取属于内部状态错误。
    fn metadata(&self) -> &ComponentMetadata {
        self.metadata
            .as_ref()
            .expect("component metadata is frozen")
    }
}

/// 业务作用：按单个已激活步骤同步释放 action、停机任务或所属资源，保留真实依赖顺序。
///
/// 参数说明：
/// - `application`：拥有本实例停机登记和资源表的句柄。
/// - `step`：从激活栈逆序移出的单个步骤；任务门由调用者先确认或移交保留权。
///
/// 返回：无返回值；单项析构异常独立告警，不截断其余步骤，不调用业务异步主体。
fn release_cancelled_step(application: &Application, step: ActiveStep) {
    match step {
        ActiveStep::Action { mut action, .. }
        | ActiveStep::InitializerAction { mut action, .. } => {
            if let Some(error) = action.release() {
                report_shutdown(&error);
            }
        }
        ActiveStep::BusinessShutdownTasks => {
            for error in application.close_shutdown_tasks() {
                report_shutdown(&error);
            }
        }
        ActiveStep::BusinessResources => application
            .resources()
            .release_owner(ResourceOwner::Business),
        ActiveStep::ComponentResources(component) => application
            .resources()
            .release_owner(ResourceOwner::Component(component)),
        ActiveStep::InitializerResources(initializer) => application
            .resources()
            .release_owner(ResourceOwner::Initializer(initializer)),
        ActiveStep::UserTasks | ActiveStep::InitializerTasks => {}
    }
}

impl ApplicationRunner {
    /// 业务作用：将未排干消费器的整段依赖所有权延迟到实际退出后释放。
    /// 参数说明：`cleanup` 为尚未执行清理的动作、资源和组件集合。
    /// 返回：同步关闭外部借用；所有来源和受管任务都释放守卫后才析构依赖，不声明异步清理成功。
    #[cfg(feature = "redis")]
    fn retain_partition_dependencies(&mut self, cleanup: CancelledRunnerCleanup) {
        self.application.resources().close_borrowing();
        self.application.clear_global();
        let retained = std::sync::Arc::new(std::sync::Mutex::new(Some(cleanup)));
        for runtime in self.application.redis_partitions().unfinished() {
            let _ = runtime.retain_shutdown_dependency(retained.clone());
        }
        // 消费器与普通受管任务可能同时持有依赖，任一侧尚未退出都不能触发另一侧的资源释放。
        self.supervisor.cancel_for_drop();
        self.supervisor.retain_until_tasks_release(retained);
    }

    /// 业务作用：外层取消时立即撤销公共入口，并把任务门之后的资源释放责任交给任务存活边界。
    ///
    /// 参数说明：`cleanup` 持有尚未消费的 active stack、资源表和组件所有权。
    ///
    /// 返回：无返回值；不等待任务退出，最后一个任务 future 析构后才释放清理尾部。
    fn defer_cancelled_cleanup(&mut self, cleanup: CancelledRunnerCleanup) {
        // 资源继续存活不等于实例仍可服务；先关闭新借用和全局入口，再移交延迟释放责任。
        self.application.resources().close_borrowing();
        self.application.clear_global();
        self.supervisor.retain_until_tasks_release(cleanup);
    }

    /// 业务作用：使用已完成同步预检的应用信息和配置视图创建 Runner。
    ///
    /// 参数说明：
    /// - `info`：已经固定名称、profile 和 Service/Batch 模式的进程元数据。
    /// - `initial_config`：版本为 1 的不可变配置视图。
    ///
    /// 返回：尚未启动组件或任务的执行器；任务门和激活栈从空状态开始，由 run 推进生命周期。
    #[doc(hidden)]
    pub fn new(info: ApplicationInfo, initial_config: Arc<ConfigView>) -> Self {
        let (application, supervisor) = Application::create(info, initial_config);
        Self {
            application,
            supervisor,
            components: Vec::new(),
            active: ActiveStack::new(),
            task_gate_in_progress: false,
            signal_mode: SignalMode::Disabled,
            startup_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(15),
        }
    }

    /// 业务作用：设置覆盖全部启动阶段的单一绝对预算。
    ///
    /// # 参数
    ///
    /// - `timeout`：同步预检完成后允许异步启动消费的总时长。
    #[doc(hidden)]
    pub fn startup_timeout(self, timeout: Duration) -> Self {
        self.try_startup_timeout(timeout)
            .expect("application startup timeout must be within (0, 365 days]")
    }

    /// 业务作用：校验并设置启动预算；低层装配器可用本入口把外部边界错误转成 Bootstrap 失败。
    #[doc(hidden)]
    pub fn try_startup_timeout(mut self, timeout: Duration) -> ApplicationResult<Self> {
        validate_lifecycle_timeout(timeout, ApplicationPhase::Bootstrap, "startup")?;
        self.startup_timeout = timeout;
        Ok(self)
    }

    /// 业务作用：设置一次反向清理可以消费的全局预算。
    ///
    /// # 参数
    ///
    /// - `timeout`：所有任务、资源和 action 共享的总时长，不会按步骤重置。
    #[doc(hidden)]
    pub fn shutdown_timeout(self, timeout: Duration) -> Self {
        self.try_shutdown_timeout(timeout)
            .expect("application shutdown timeout must be within (0, 365 days]")
    }

    /// 业务作用：校验并设置停机预算；低层装配器可用本入口把外部边界错误转成配置失败。
    #[doc(hidden)]
    pub fn try_shutdown_timeout(mut self, timeout: Duration) -> ApplicationResult<Self> {
        validate_lifecycle_timeout(timeout, ApplicationPhase::Stopping, "shutdown")?;
        self.shutdown_timeout = timeout;
        Ok(self)
    }

    /// 业务作用：开启真实进程信号控制面。
    ///
    /// # 参数
    ///
    /// 本方法无参数；同步进程入口必须调用，嵌入式调用方可保持关闭并自行管理终止条件。
    #[doc(hidden)]
    pub fn with_process_signals(mut self) -> Self {
        self.signal_mode = SignalMode::Process;
        self
    }

    /// 业务作用：按声明顺序追加一个生命周期组件。
    ///
    /// 参数说明：`component` 由 Runner 独占并依次执行 Bootstrap、Start、Prepare 和 Ready。
    /// 返回：登记不读取动态元数据；run 在阶段执行前统一冻结。对象从登记起受析构隔离保护，依赖收口后才释放。
    #[doc(hidden)]
    pub fn with_component(mut self, component: Box<dyn ApplicationComponent>) -> Self {
        self.components.push(ComponentEntry::new(component));
        self
    }

    /// 业务作用：将已编入的观测能力纳入相同生命周期，不要求业务额外声明组件。
    /// 参数说明：无。
    /// 返回：在数据库 I/O 与业务路由之前插入已冻结元数据的组件，保留已有相对顺序；元数据读取失败阻止启动。
    fn attach_observability_components(&mut self) -> ApplicationResult<()> {
        #[cfg(feature = "config-watch")]
        if !self
            .components
            .iter()
            .any(|component| component.metadata().id == ComponentId::Config)
        {
            let components = self.declared_components();
            self.application
                .mark_component_declared(ComponentId::Config);
            self.components.push(ComponentEntry::prepared(Box::new(
                crate::config_watch::LocalConfigComponent::new(components),
            ))?);
        }
        #[cfg(feature = "mapper-observability")]
        if !self
            .components
            .iter()
            .any(|component| component.metadata().id == ComponentId::SqlObservability)
        {
            if let Some(index) = self
                .components
                .iter()
                .position(|component| component.metadata().id == ComponentId::Db)
            {
                // 策略与系列预算必须先于连接探测冻结，避免启动半途才发现观测配置无效。
                self.application
                    .mark_component_declared(ComponentId::SqlObservability);
                self.components.insert(
                    index,
                    ComponentEntry::prepared(Box::new(
                        crate::sql_observability::SqlObservabilityComponent::new(),
                    ))?,
                );
            }
        }
        #[cfg(feature = "observability")]
        if !self
            .components
            .iter()
            .any(|component| component.metadata().id == ComponentId::Observability)
        {
            let index = self
                .components
                .iter()
                .position(|component| {
                    matches!(
                        component.metadata().id,
                        ComponentId::SqlObservability | ComponentId::Db | ComponentId::Web
                    )
                })
                .unwrap_or(self.components.len());
            self.application
                .mark_component_declared(ComponentId::Observability);
            self.components.insert(
                index,
                ComponentEntry::prepared(Box::new(
                    crate::observability::ObservabilityComponent::new(),
                ))?,
            );
        }
        Ok(())
    }

    /// 业务作用：原子替换当前配置视图并通知订阅者。
    ///
    /// # 参数
    ///
    /// - `next`：已经完成 bootstrap-only 比较和组件校验的新视图。
    #[doc(hidden)]
    pub fn replace_config(&self, next: Arc<ConfigView>) {
        self.application.publish_config(next);
    }

    /// 业务作用：执行完整的组件启动、UserHook、Running 和反向清理生命周期。
    ///
    /// 参数说明：
    /// - `user_hook`：接收 Application 所有权副本并返回受监督 future 的业务启动入口。
    ///
    /// 返回：Service 正常停机或 Batch 工作完成时返回含退出原因、退出码和次要清理错误的结果；
    /// 启动或关键任务失败先执行统一回滚再返回主错误；无法建立控制面或提交最终状态也返回错误。
    #[doc(hidden)]
    pub async fn run<F, Fut, E>(mut self, user_hook: F) -> ApplicationResult<ApplicationExit>
    where
        F: FnOnce(Application) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Into<anyhow::Error> + 'static,
    {
        // 生命周期入口不止进程 `run` 一个:公开 Runner 直接执行时同样会运行业务
        // initializer、UserHook 与组件代码。panic hook 必须在任何业务代码可能 panic
        // 之前安装——catch_unwind 在 hook 之后才生效,拦不住默认 hook 先把 payload
        // 写进 stderr。重复安装由 Once 收敛。
        crate::panic_hook::install_process_panic_hook();
        // handler ready ACK 是所有异步组件的前置屏障，确保启动卡住时仍可被终止。
        let signal_mode = std::mem::replace(&mut self.signal_mode, SignalMode::Disabled);
        let mut broker = match SignalBroker::start(signal_mode, self.application.clone()).await {
            Ok(broker) => broker,
            Err(error) => return Err(self.fail_without_broker(error).await),
        };
        let startup_deadline = Instant::now() + self.startup_timeout;

        // 元数据属于启动信任边界；所有显式组件先通过门禁，任何一个失败都不执行其它组件的阶段。
        for component in &mut self.components {
            if let Err(error) = component.freeze_metadata() {
                return self
                    .handle_startup_stop(StartupStop::Failure(error), &mut broker)
                    .await;
            }
        }
        for component in &self.components {
            self.application
                .mark_component_declared(component.metadata().id);
        }
        if let Err(error) = self.attach_observability_components() {
            return self
                .handle_startup_stop(StartupStop::Failure(error), &mut broker)
                .await;
        }

        if let Err(error) = self.validate_component_order() {
            return self
                .handle_startup_stop(StartupStop::Failure(error), &mut broker)
                .await;
        }
        if let Err(stop) = self
            .bootstrap_components(startup_deadline, &mut broker)
            .await
        {
            return self.handle_startup_stop(stop, &mut broker).await;
        }
        // Bootstrap 结束后配置树已是最终形态（含 Nacos overlay），且日志组件已接管早期控制台：
        // 此时输出一次“配置段存在但组件未声明”的告警，本地段与远端段一并覆盖。
        self.warn_undeclared_config_sections();
        // 最终树对已声明组件执行一次初始段校验：非法配置在建立任何监听/连接副作用之前失败。
        if let Err(error) = self.validate_declared_config_sections() {
            return self
                .handle_startup_stop(StartupStop::Failure(error), &mut broker)
                .await;
        }
        if let Err(stop) = self.start_components(startup_deadline, &mut broker).await {
            return self.handle_startup_stop(stop, &mut broker).await;
        }

        let mode = self.application.info().mode();
        match mode {
            ApplicationMode::Batch => {
                // Batch 的 UserHook 就是工作负载；先冻结静态计划并完成准备与初始化，
                // 才能保证初始化失败时业务负载从未被 poll。
                let plan = match self.freeze_initializer_plan(mode) {
                    Ok(plan) => plan,
                    Err(error) => {
                        return self
                            .handle_startup_stop(StartupStop::Failure(error), &mut broker)
                            .await;
                    }
                };
                if let Err(stop) = self.prepare_components(startup_deadline, &mut broker).await {
                    // 未进入屏障的实例先释放捕获依赖，再由统一回滚关闭组件资源。
                    drop(plan);
                    return self.handle_startup_stop(stop, &mut broker).await;
                }
                if let Err(stop) = self
                    .run_initializers(plan, startup_deadline, &mut broker)
                    .await
                {
                    return self.handle_startup_stop(stop, &mut broker).await;
                }

                #[cfg(any(feature = "mapper-cache", feature = "mapper-cache-pgsql"))]
                if let Err(error) = crate::mapper_cache::ensure_mapper_l2_installed() {
                    return self
                        .handle_startup_stop(StartupStop::Failure(error), &mut broker)
                        .await;
                }

                // 工作负载可登记现有业务资源/任务，但 initializer 登记门从未开放。
                // Batch 不开放业务入口，但采集出口必须先就位，才能覆盖整个工作负载。
                if let Err(stop) = self.ready_components(startup_deadline, &mut broker).await {
                    return self.handle_startup_stop(stop, &mut broker).await;
                }
                if let Err(stop) = self
                    .final_startup_check(startup_deadline, &mut broker)
                    .await
                {
                    return self.handle_startup_stop(stop, &mut broker).await;
                }
                // Batch 不发布 Service Ready；只统一放行已完成门禁的观测任务，以覆盖后续工作负载。
                self.supervisor.release_startup_tasks();
                self.active.push_business_resources();
                self.active.push_business_shutdown_tasks();
                self.active.push_user_tasks();
                if let Err(stop) = self
                    .run_user_hook(user_hook, false, startup_deadline, &mut broker)
                    .await
                {
                    return self.handle_startup_stop(stop, &mut broker).await;
                }
                self.supervisor.close_registration().await;
                if let Err(error) = self.seal_initialization() {
                    return self
                        .handle_startup_stop(StartupStop::Failure(error), &mut broker)
                        .await;
                }
                self.application
                    .set_terminal(TerminalIntent::BatchCompleted);
                let shutdown_failures = self
                    .finish(ShutdownReason::BatchCompleted, false, &mut broker)
                    .await?;
                Ok(ApplicationExit {
                    reason: ApplicationExitReason::BatchCompleted,
                    code: 0,
                    shutdown_failures,
                })
            }
            ApplicationMode::Service => {
                // 动态步骤在 poll hook 之前入栈，保证部分装配也能走同一回滚链。
                self.active.push_business_resources();
                self.active.push_business_shutdown_tasks();
                self.active.push_user_tasks();
                if let Err(stop) = self
                    .run_user_hook(user_hook, true, startup_deadline, &mut broker)
                    .await
                {
                    return self.handle_startup_stop(stop, &mut broker).await;
                }

                // 先关闭 UserHook 与 initializer 登记，再关闭 Supervisor 公共通道并排净 ACK；
                // 这是任何工厂开始前的唯一冻结边界。
                self.supervisor.close_registration().await;
                let plan = match self.freeze_initializer_plan(mode) {
                    Ok(plan) => plan,
                    Err(error) => {
                        return self
                            .handle_startup_stop(StartupStop::Failure(error), &mut broker)
                            .await;
                    }
                };
                if let Err(stop) = self.prepare_components(startup_deadline, &mut broker).await {
                    // 未进入屏障的实例先释放捕获依赖，再由统一回滚关闭组件资源。
                    drop(plan);
                    return self.handle_startup_stop(stop, &mut broker).await;
                }
                let staged_tasks = match self
                    .run_initializers(plan, startup_deadline, &mut broker)
                    .await
                {
                    Ok(tasks) => tasks,
                    Err(stop) => {
                        // 已启动的公共任务必须先于 initializer action/资源停止，
                        // 失败出口额外压栈使这一顺序在反向清理时成立。
                        self.active.push_initializer_tasks();
                        return self.handle_startup_stop(stop, &mut broker).await;
                    }
                };
                // Ready action 会压在该清理门之上：停机先关闭入站能力，再终止全部受管任务，
                // 最后才撤销 initializer action 和资源。这也覆盖 Seal/Ready 失败时尚未激活 staged task 的路径。
                self.active.push_initializer_tasks();

                // 静态装配与 initializer 均已完成，接流前确认缓存查询具有可用默认 L2。
                #[cfg(any(feature = "mapper-cache", feature = "mapper-cache-pgsql"))]
                if let Err(error) = crate::mapper_cache::ensure_mapper_l2_installed() {
                    // 尚未移交的捕获值必须先于依赖资源释放，守卫隔离各工厂的析构异常。
                    drop(staged_tasks);
                    return self
                        .handle_startup_stop(StartupStop::Failure(error), &mut broker)
                        .await;
                }
                if let Err(error) = self.seal_initialization() {
                    // 封存失败不再构造终端，先释放工厂捕获值，再撤销依赖。
                    drop(staged_tasks);
                    return self
                        .handle_startup_stop(StartupStop::Failure(error), &mut broker)
                        .await;
                }
                // 全局 Weak 槽在 sealed 后、Ready 前安装，Ready action 可以构造需要 Application 的终端资源。
                if let Err(error) = self.application.install_global() {
                    // 未取得全局发布权时保持未接流，撤销暂存所有权后才回滚资源。
                    drop(staged_tasks);
                    return self
                        .handle_startup_stop(StartupStop::Failure(error), &mut broker)
                        .await;
                }
                if let Err(stop) = self.ready_components(startup_deadline, &mut broker).await {
                    // 组件拒绝 Ready 后不激活 initializer，捕获依赖的工厂先独立释放。
                    drop(staged_tasks);
                    return self.handle_startup_stop(stop, &mut broker).await;
                }
                if let Err(stop) = self
                    .activate_initializer_tasks(staged_tasks, startup_deadline, &mut broker)
                    .await
                {
                    return self.handle_startup_stop(stop, &mut broker).await;
                }
                if let Err(stop) = self
                    .final_startup_check(startup_deadline, &mut broker)
                    .await
                {
                    return self.handle_startup_stop(stop, &mut broker).await;
                }
                if let Err(error) = self.application.mark_ready() {
                    return self
                        .handle_startup_stop(StartupStop::Failure(error), &mut broker)
                        .await;
                }
                // 所有工厂构造、任务登记和预算复验成功后，先发布 Ready，再用同一个信号开放全部终端。
                self.supervisor.release_startup_tasks();

                match self.wait_for_service_terminal(&mut broker).await {
                    ServiceTerminal::Requested => {
                        self.application.set_terminal(TerminalIntent::ServiceStop);
                        let shutdown_failures = self
                            .finish(ShutdownReason::Requested, false, &mut broker)
                            .await?;
                        Ok(ApplicationExit {
                            reason: ApplicationExitReason::ShutdownRequested,
                            code: 0,
                            shutdown_failures,
                        })
                    }
                    ServiceTerminal::Signal(signal) => {
                        self.application.set_terminal(TerminalIntent::ServiceStop);
                        let shutdown_failures = self
                            .finish(ShutdownReason::Signal(signal), false, &mut broker)
                            .await?;
                        Ok(ApplicationExit {
                            reason: ApplicationExitReason::Signal(signal),
                            code: 0,
                            shutdown_failures,
                        })
                    }
                    ServiceTerminal::Failure(primary) => {
                        let mut primary = primary;
                        // 主错误必须在清理日志组件之前写出；同步入口通过 reported 标记避免重复输出。
                        crate::report::report_runtime(&primary);
                        primary.mark_reported();
                        self.application.set_terminal(TerminalIntent::Failure);
                        let shutdown_failures = self
                            .finish(ShutdownReason::CriticalTaskFailed, true, &mut broker)
                            .await
                            .unwrap_or_default();
                        for failure in &shutdown_failures {
                            report_shutdown(failure);
                        }
                        Err(primary)
                    }
                }
            }
        }
    }

    /// 业务作用：对最终配置树中“有配置段却没有声明组件”的情况输出一次脱敏告警。
    ///
    /// # 参数
    ///
    /// 本方法无参数；告警只包含配置段名与组件名，不含段内任何值。
    fn warn_undeclared_config_sections(&self) {
        let snapshot = self.application.config();
        crate::sections::warn_undeclared_sections(&self.declared_components(), snapshot.value());
    }

    /// 业务作用：对最终配置树中所有已声明组件的配置段做一次无副作用校验。
    ///
    /// # 参数
    ///
    /// 本方法无参数；校验失败时组件尚未创建任何连接或监听副作用。
    fn validate_declared_config_sections(&self) -> ApplicationResult<()> {
        let snapshot = self.application.config();
        crate::sections::validate_declared_sections(
            &self.declared_components(),
            snapshot.value(),
            ApplicationPhase::Start,
        )
    }

    /// 业务作用：返回当前组件表按声明顺序展开的组件身份列表。
    ///
    /// # 参数
    ///
    /// 本方法无参数；顺序与属性入口的源码声明顺序一致。
    fn declared_components(&self) -> Vec<ComponentId> {
        self.components
            .iter()
            .map(|component| component.metadata().id)
            .collect()
    }

    /// 业务作用：校验动态组件对象的唯一性和显式依赖顺序。
    ///
    /// # 参数
    ///
    /// 本方法无参数；校验只读取 Runner 已持有的组件表，不重排声明顺序。
    fn validate_component_order(&self) -> ApplicationResult<()> {
        let component_ids = self.declared_components();
        crate::spec::validate_component_order(&component_ids)?;
        #[cfg(any(feature = "db", feature = "db-pgsql"))]
        if !crate::migrations::MIGRATION_PLANS.is_empty()
            && !component_ids.contains(&ComponentId::Db)
        {
            // 静态计划承诺工作负载之前完成迁移；缺少 DB owner 时不能悄然跳过此门禁。
            return Err(runner_error(
                ApplicationPhase::Bootstrap,
                "migration plans require the db component",
            ));
        }
        let mut declared = std::collections::HashSet::new();
        for component in &self.components {
            let id = component.metadata().id;
            if !declared.insert(id) {
                return Err(runner_error(
                    ApplicationPhase::Bootstrap,
                    format!("component `{id}` is declared more than once"),
                ));
            }
            for dependency in component.metadata().dependencies {
                if !declared.contains(dependency) {
                    return Err(runner_error(
                        ApplicationPhase::Bootstrap,
                        format!("component `{id}` requires `{dependency}` to be declared earlier"),
                    ));
                }
            }
        }
        Ok(())
    }

    /// 业务作用：按声明顺序执行所有组件的 Bootstrap 阶段。
    ///
    /// 参数说明：
    ///
    /// - `deadline`：整个异步启动共享的绝对截止时间。
    /// - `broker`：在组件 future 卡住时仍被并发轮询的信号控制面。
    /// 返回：全部组件及阶段释放完成时成功；构造、轮询或析构展开、普通错误与启动中断交给统一回滚。
    async fn bootstrap_components(
        &mut self,
        deadline: Instant,
        broker: &mut SignalBroker,
    ) -> Result<(), StartupStop> {
        crate::managed_adapters::validate(
            self.application.config().value(),
            &self.declared_components(),
        )
        .map_err(StartupStop::Failure)?;
        // 外部材料必须先于日志、配置中心认证和其余消费者准备；同步 preflight 不执行远端 I/O。
        let view = self.application.config_view();
        if let Some(raw) = view.bootstrap_candidate().map_err(StartupStop::Failure)? {
            let prepare = async {
                let candidate = crate::config::resolve_candidate_async(
                    view.snapshot().version(),
                    raw,
                    Vec::new(),
                )
                .await
                .map_err(|error| {
                    ApplicationError::with_source(
                        ComponentId::Config,
                        ApplicationPhase::Bootstrap,
                        "external bootstrap material preparation failed",
                        error,
                    )
                })?;
                self.application
                    .publish_config(candidate.finish(view.reload_statuses().clone()));
                Ok(())
            };
            await_startup_future(
                prepare,
                deadline,
                ApplicationPhase::Bootstrap,
                &self.application,
                broker,
            )
            .await?;
        }
        for component in &mut self.components {
            let component_id = component.metadata().id;
            let mut context = BootstrapContext::new(
                &self.application,
                component_id,
                &mut self.active,
                deadline.into(),
            );
            await_startup_future(
                // trait 调用延迟到隔离 future 内，已激活的副作用在构造或轮询展开后仍由回滚栈接管。
                isolate_component_startup(
                    async {
                        retain_startup_future(
                            component.owner.value_mut().bootstrap(&mut context),
                            component_id,
                            ApplicationPhase::Bootstrap,
                        )
                        .await
                    },
                    component_id,
                    ApplicationPhase::Bootstrap,
                ),
                deadline,
                ApplicationPhase::Bootstrap,
                &self.application,
                broker,
            )
            .await?;
        }
        Ok(())
    }

    /// 业务作用：按声明顺序执行所有组件的 Start 阶段。
    ///
    /// 参数说明：
    ///
    /// - `deadline`：与前序阶段共享的绝对启动截止时间。
    /// - `broker`：持续观察启动中断信号的控制面。
    /// 返回：全部组件及阶段释放完成时成功；构造、轮询或析构展开、普通错误与启动中断交给统一回滚。
    async fn start_components(
        &mut self,
        deadline: Instant,
        broker: &mut SignalBroker,
    ) -> Result<(), StartupStop> {
        crate::managed_adapters::validate(
            self.application.config().value(),
            &self.declared_components(),
        )
        .map_err(StartupStop::Failure)?;
        for component in &mut self.components {
            let component_id = component.metadata().id;
            let mut context = StartContext::new(
                &self.application,
                component_id,
                &mut self.active,
                deadline.into(),
            );
            await_startup_future(
                // 监听或注册已成功时必须保留异步撤销责任，不能让组件展开直接析构整个 Runner。
                isolate_component_startup(
                    async {
                        retain_startup_future(
                            component.owner.value_mut().start(&mut context),
                            component_id,
                            ApplicationPhase::Start,
                        )
                        .await
                    },
                    component_id,
                    ApplicationPhase::Start,
                ),
                deadline,
                ApplicationPhase::Start,
                &self.application,
                broker,
            )
            .await?;
        }
        Ok(())
    }

    /// 业务作用：关闭运行时 initializer 登记并在任何工厂调用前完成全量元数据校验。
    ///
    /// 参数说明：
    /// - `mode`：preflight 已固定的 Service 或 Batch 模式。
    ///
    /// 返回：静态描述与运行时实例合并后的冻结计划；重名、超限或模式冲突时返回错误。
    fn freeze_initializer_plan(
        &self,
        mode: ApplicationMode,
    ) -> ApplicationResult<FrozenInitializerPlan> {
        let runtime = self.application.freeze_initializers()?;
        freeze_plan(mode, runtime)
    }

    /// 业务作用：按声明顺序执行组件 Prepare，为 initializer 建立 migration 与出站依赖边界。
    ///
    /// 参数说明：
    /// - `deadline`：与 Bootstrap、Start 和后续初始化共享的绝对截止时刻。
    /// - `broker`：组件 future 阻塞时仍持续观察启动中断信号的控制面。
    ///
    /// 返回：全部出站门禁通过时成功；组件错误或展开、任务失败、超时或终止时返回唯一启动中断。
    async fn prepare_components(
        &mut self,
        deadline: Instant,
        broker: &mut SignalBroker,
    ) -> Result<(), StartupStop> {
        let cancellation = self.application.cancellation_token();
        for component in &mut self.components {
            let component_id = component.metadata().id;
            let mut context = PrepareContext::new(
                &self.application,
                component_id,
                &mut self.active,
                deadline.into(),
            );
            // 迁移工厂和出站装配可在任一次 poll 中展开，阶段内收敛后才能按原栈逆序撤销。
            let future = isolate_component_startup(
                async {
                    retain_startup_future(
                        component.owner.value_mut().prepare(&mut context),
                        component_id,
                        ApplicationPhase::Prepare,
                    )
                    .await
                },
                component_id,
                ApplicationPhase::Prepare,
            );
            let mut owned = StartupCleanup::new(
                Box::pin(future),
                component_id,
                ApplicationPhase::Prepare,
                "releasing component wait",
            );
            let result = async {
                loop {
                    let remaining = startup_remaining(deadline, ApplicationPhase::Prepare)?;
                    tokio::select! {
                        result = owned.value_mut() => {
                            result.map_err(StartupStop::Failure)?;
                            break;
                        }
                        _ = cancellation.cancelled() => return Err(StartupStop::Requested),
                        signal = broker.next() => return Err(signal_to_startup_stop(signal)),
                        completion = self.supervisor.join_next(), if self.supervisor.has_tasks() => {
                            if let Some(completion) = completion {
                                classify_startup_completion(completion)?;
                            }
                        }
                        _ = tokio::time::sleep(remaining) => {
                            return Err(StartupStop::Failure(startup_timeout_error(ApplicationPhase::Prepare)));
                        }
                    }
                }
                Ok(())
            }
            .await;
            finish_startup_release(result, owned.release())?;
        }
        let mut context = PrepareContext::new(
            &self.application,
            ComponentId::Application,
            &mut self.active,
            deadline.into(),
        );
        let adapters = crate::managed_adapters::prepare(&mut context);
        tokio::pin!(adapters);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(StartupStop::Requested),
                signal = broker.next() => return Err(signal_to_startup_stop(signal)),
                _ = tokio::time::sleep_until(deadline) => return Err(StartupStop::Failure(startup_timeout_error(ApplicationPhase::Prepare))),
                completion = self.supervisor.join_next(), if self.supervisor.has_tasks() => {
                    if let Some(completion) = completion {
                        classify_startup_completion(completion)?;
                    }
                }
                result = &mut adapters => {
                    if let Some(task) = result.map_err(StartupStop::Failure)? {
                        // 资源清理责任已经压栈；探测任务交给同一监督器，在统一屏障后运行并在释放资源前收割。
                        self.supervisor.spawn_component_critical(
                            "object-store-health-monitor",
                            Box::pin(async move { task.await.map_err(anyhow::Error::from) }),
                        ).map_err(StartupStop::Failure)?;
                    }
                    break;
                },
            }
        }
        Ok(())
    }

    /// 业务作用：构造条件启用项、稳定拓扑排序，并执行三轮全局 initializer 屏障。
    ///
    /// 参数说明：
    /// - `plan`：Prepare 前已完成元数据校验的冻结计划。
    /// - `deadline`：全启动流程共享的绝对截止时刻。
    /// - `broker`：在工厂和每个阶段 future 期间保持活跃的信号控制面。
    ///
    /// 返回：三轮与实例释放全部成功时返回尚未构造的长期任务工厂；任一失败停止后续项并逐项释放实例。
    async fn run_initializers(
        &mut self,
        plan: FrozenInitializerPlan,
        deadline: Instant,
        broker: &mut SignalBroker,
    ) -> Result<Vec<StagedInitializerTask>, StartupStop> {
        let mut staged_tasks = Vec::new();
        let mut enabled = Vec::with_capacity(plan.entries.len());
        let mut pending = std::collections::VecDeque::from(plan.entries);
        let mut result = async {
            while let Some(entry) = pending.pop_front() {
                match entry {
                    FrozenInitializer::Static { spec, factory } => {
                        let name: Arc<str> = Arc::from(spec.name());
                        let started = StdInstant::now();
                        let future = std::panic::catch_unwind(AssertUnwindSafe(|| {
                            factory(self.application.clone())
                        }))
                        .map_err(|payload| {
                            release_shutdown_panic_payload(payload);
                            crate::initialization::record_duration(
                                &self.application,
                                &name,
                                InitializerStage::Factory,
                                started.elapsed(),
                            );
                            crate::initialization::record_failure(
                                &self.application,
                                &name,
                                InitializerStage::Factory,
                                InitializerFailureKind::Panicked,
                            );
                            StartupStop::Failure(initializer_failure_error(
                                name.clone(),
                                InitializerStage::Factory,
                                InitializerFailureKind::Panicked,
                                None,
                            ))
                        })?;
                        let initializer = await_initializer_future(
                            // 工厂产出实例后先接管它，再释放工厂 future；两者的析构异常不能相互穿透。
                            own_initializer_future(future),
                            name.clone(),
                            InitializerStage::Factory,
                            deadline,
                            &self.application,
                            &mut self.supervisor,
                            broker,
                        )
                        .await?;
                        tracing::info!(
                            initializer = %name,
                            stage = %InitializerStage::Factory,
                            duration_seconds = started.elapsed().as_secs_f64(),
                            enabled = initializer.is_some(),
                            "initializer factory completed"
                        );
                        if let Some(initializer) = initializer {
                            enabled.push(EnabledInitializer { spec, initializer });
                        }
                    }
                    FrozenInitializer::Runtime { spec, initializer } => {
                        enabled.push(EnabledInitializer { spec, initializer });
                    }
                }
            }

            // 排序只借用元数据；失败后实例仍留在本层，统一释放不会依赖局部容器的隐式析构。
            let ordered = order_enabled(&enabled).map_err(StartupStop::Failure)?;
            for stage in [
                InitializerStage::Before,
                InitializerStage::Initialize,
                InitializerStage::After,
            ] {
                for index in &ordered {
                    let entry = &mut enabled[*index];
                    let name: Arc<str> = Arc::from(entry.spec.name());
                    let started = StdInstant::now();
                    let cancellation = self.application.cancellation_token();
                    let mut context = InitializationContext {
                        application: &self.application,
                        initializer: name.clone(),
                        kind: entry.spec.initializer_kind(),
                        stage,
                        active: &mut self.active,
                        staged_tasks: &mut staged_tasks,
                        deadline,
                        cancellation,
                    };
                    // trait 方法调用也放进 async 边界，使“构造 future 时 panic”与
                    // “poll 时 panic”都被同一 `catch_unwind` 收敛，不越过 Runner 回滚边界。
                    let future = async {
                        match stage {
                            InitializerStage::Before => {
                                retain_startup_future(
                                    entry.initializer.value_mut().before(&mut context),
                                    ComponentId::Application,
                                    ApplicationPhase::Initialization,
                                )
                                .await
                            }
                            InitializerStage::Initialize => {
                                retain_startup_future(
                                    entry.initializer.value_mut().initialize(&mut context),
                                    ComponentId::Application,
                                    ApplicationPhase::Initialization,
                                )
                                .await
                            }
                            InitializerStage::After => {
                                retain_startup_future(
                                    entry.initializer.value_mut().after(&mut context),
                                    ComponentId::Application,
                                    ApplicationPhase::Initialization,
                                )
                                .await
                            }
                            InitializerStage::Factory | InitializerStage::Activation => {
                                unreachable!(
                                    "initializer barrier only executes before, initialize, and after"
                                )
                            }
                        }
                    };
                    await_initializer_future(
                        future,
                        name.clone(),
                        stage,
                        deadline,
                        &self.application,
                        &mut self.supervisor,
                        broker,
                    )
                    .await?;
                    tracing::info!(
                        initializer = %name,
                        stage = %stage,
                        duration_seconds = started.elapsed().as_secs_f64(),
                        "initializer stage completed"
                    );
                }
            }
            Ok(())
        }
        .await;
        // 无论屏障成功还是中断，先释放全部实例；首个释放错误只在尚无停止原因时阻止启动。
        for entry in &mut enabled {
            result = finish_startup_release(result, entry.initializer.release());
        }
        for entry in &mut pending {
            if let FrozenInitializer::Runtime { initializer, .. } = entry {
                result = finish_startup_release(result, initializer.release());
            }
        }
        if result.is_err() {
            for task in &mut staged_tasks {
                if let Some(error) = task.factory.release() {
                    report_shutdown(&error);
                }
            }
        }
        result.map(|()| staged_tasks)
    }

    /// 业务作用：封存 initializer 登记期允许扩展的资源 key 和 readiness 名称集合。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：资源容器首次封存成功时返回成功；重复或非法阶段返回 Seal 错误。
    fn seal_initialization(&self) -> ApplicationResult<()> {
        // 业务停机任务必须与资源表在同一 Seal 边界冻结，Ready 之后的调用不能把新 future
        // 插入已经确定的停机顺序。
        self.application.seal_shutdown_tasks().map_err(|error| {
            ApplicationError::with_source(
                ComponentId::Application,
                ApplicationPhase::Seal,
                "failed to seal graceful shutdown tasks after initialization",
                error,
            )
        })?;
        self.application.resources().seal().map_err(|error| {
            ApplicationError::with_source(
                ComponentId::Application,
                ApplicationPhase::Seal,
                "failed to seal application resources after initialization",
                error,
            )
        })?;
        // readiness 必须与资源同一阶段封口，防止 Ready 后无界增长名称和指标基数。
        self.application.seal_readiness();
        Ok(())
    }

    /// 业务作用：在全部组件 Ready action 成功后构造 initializer 暂存任务，并移交监督所有权但不放行执行。
    ///
    /// 参数说明：
    /// - `tasks`：`stage_*` 保存的一次性任务工厂。
    /// - `deadline`：激活仍必须遵守的共享启动截止时刻。
    /// - `broker`：每个任务激活边界都复验的启动信号控制面。
    ///
    /// 返回：全部任务已加入 Supervisor 的关闭屏障时成功；超时、工厂 panic 或名称冲突时保持未接流并清理。
    async fn activate_initializer_tasks(
        &mut self,
        mut tasks: Vec<StagedInitializerTask>,
        deadline: Instant,
        broker: &mut SignalBroker,
    ) -> Result<(), StartupStop> {
        if tasks.is_empty() {
            return Ok(());
        }
        let result = async {
            while !tasks.is_empty() {
                let mut task = tasks.remove(0);
                startup_remaining(deadline, ApplicationPhase::Ready)?;
                let started = StdInstant::now();
                let cancellation = self.application.cancellation_token();
                // 激活循环可能包含多个同步工厂；每项之间显式让出并优先处理停止事件，
                // 避免启动已取消时仍继续构造后续业务 future。
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => {
                        crate::initialization::record_duration(
                            &self.application,
                            &task.initializer,
                            InitializerStage::Activation,
                            started.elapsed(),
                        );
                        crate::initialization::record_failure(
                            &self.application,
                            &task.initializer,
                            InitializerStage::Activation,
                            InitializerFailureKind::Cancelled,
                        );
                        return Err(StartupStop::Requested);
                    }
                    signal = broker.next() => {
                        crate::initialization::record_duration(
                            &self.application,
                            &task.initializer,
                            InitializerStage::Activation,
                            started.elapsed(),
                        );
                        crate::initialization::record_failure(
                            &self.application,
                            &task.initializer,
                            InitializerStage::Activation,
                            InitializerFailureKind::Cancelled,
                        );
                        return Err(signal_to_startup_stop(signal));
                    }
                    completion = self.supervisor.join_next(), if self.supervisor.has_tasks() => {
                        if let Some(completion) = completion {
                            if let Err(stop) = classify_startup_completion(completion) {
                                crate::initialization::record_duration(
                                    &self.application,
                                    &task.initializer,
                                    InitializerStage::Activation,
                                    started.elapsed(),
                                );
                                crate::initialization::record_failure(
                                    &self.application,
                                    &task.initializer,
                                    InitializerStage::Activation,
                                    InitializerFailureKind::Cancelled,
                                );
                                return Err(stop);
                            }
                        }
                    }
                    _ = tokio::task::yield_now() => {}
                }
                let token = self.supervisor.task_token();
                let future = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    (task.factory.take())(token.clone())
                }))
                .map_err(|payload| {
                    release_shutdown_panic_payload(payload);
                    crate::initialization::record_failure(
                        &self.application,
                        &task.initializer,
                        InitializerStage::Activation,
                        InitializerFailureKind::Panicked,
                    );
                    crate::initialization::record_duration(
                        &self.application,
                        &task.initializer,
                        InitializerStage::Activation,
                        started.elapsed(),
                    );
                    StartupStop::Failure(initializer_failure_error(
                        task.initializer.clone(),
                        InitializerStage::Activation,
                        InitializerFailureKind::Panicked,
                        None,
                    ))
                })?;
                self.supervisor
                    .spawn_initializer_task(task.name, task.kind, future)
                    .map_err(|error| {
                        crate::initialization::record_failure(
                            &self.application,
                            &task.initializer,
                            InitializerStage::Activation,
                            InitializerFailureKind::Error,
                        );
                        crate::initialization::record_duration(
                            &self.application,
                            &task.initializer,
                            InitializerStage::Activation,
                            started.elapsed(),
                        );
                        StartupStop::Failure(initializer_failure_error(
                            task.initializer.clone(),
                            InitializerStage::Activation,
                            InitializerFailureKind::Error,
                            Some(anyhow::Error::from(error)),
                        ))
                    })?;
                crate::initialization::record_duration(
                    &self.application,
                    &task.initializer,
                    InitializerStage::Activation,
                    started.elapsed(),
                );
            }
            Ok(())
        }
        .await;
        for task in &mut tasks {
            if let Some(error) = task.factory.release() {
                report_shutdown(&error);
            }
        }
        result
    }

    /// 业务作用：按声明顺序装配组件并移交终端所有权；任务主体等待全应用屏障，Batch 不开放业务 listener。
    /// 参数说明：
    ///
    /// - `deadline`：与 Bootstrap、Start、UserHook、Prepare 和 Initialization 共享的启动截止时间。
    /// - `broker`：持续观察启动中断信号的控制面。
    ///
    /// 返回：全部 Ready 装配成功且所有权登记完成时成功；错误或展开由调用方收割未执行任务并反向清理资源。
    async fn ready_components(
        &mut self,
        deadline: Instant,
        broker: &mut SignalBroker,
    ) -> Result<(), StartupStop> {
        let cancellation = self.application.cancellation_token();
        let batch = self.application.info().mode() == ApplicationMode::Batch;
        let mut staged_tasks = Vec::new();
        let result = async {
            for component in &mut self.components {
                let component_id = component.metadata().id;
                if batch
                    && !matches!(
                        component_id,
                        ComponentId::Observability | ComponentId::SqlObservability
                    )
                {
                    continue;
                }
                let mut context = ReadyContext::new(
                    &self.application,
                    component_id,
                    &mut self.active,
                    deadline.into(),
                );
                {
                    // Ready future 的可变组件借用必须在取终端任务前结束，避免两个生命周期操作交叠。
                    let future = isolate_component_startup(
                        async {
                            retain_startup_future(
                                component.owner.value_mut().ready(&mut context),
                                component_id,
                                ApplicationPhase::Ready,
                            )
                            .await
                        },
                        component_id,
                        ApplicationPhase::Ready,
                    );
                    let mut owned = StartupCleanup::new(
                        Box::pin(future),
                        component_id,
                        ApplicationPhase::Ready,
                        "releasing component wait",
                    );
                    let result = async {
                        loop {
                            let remaining = startup_remaining(deadline, ApplicationPhase::Ready)?;
                            tokio::select! {
                                result = owned.value_mut() => {
                                    result.map_err(StartupStop::Failure)?;
                                    break;
                                }
                                _ = cancellation.cancelled() => return Err(StartupStop::Requested),
                                signal = broker.next() => return Err(signal_to_startup_stop(signal)),
                                completion = self.supervisor.join_next(), if self.supervisor.has_tasks() => {
                                    if let Some(completion) = completion {
                                        classify_startup_completion(completion)?;
                                    }
                                }
                                _ = tokio::time::sleep(remaining) => {
                                    return Err(StartupStop::Failure(startup_timeout_error(ApplicationPhase::Ready)));
                                }
                            }
                        }
                        Ok(())
                    }
                    .await;
                    finish_startup_release(result, owned.release())?;
                }
                // 任务移交仍属于 Ready 门禁，构造展开时不得放行已暂存的任务主体。
                let task = catch_unwind(AssertUnwindSafe(|| component.owner.value_mut().take_critical_task())).map_err(
                    |payload| {
                        StartupStop::Failure(component_startup_panic(
                            component_id,
                            ApplicationPhase::Ready,
                            payload,
                        ))
                    },
                )?;
                if let Some((name, task)) = task {
                    // 后置组件仍可登记静态指标源；先持有但不 poll，防止出口或业务监听越过最终容量门禁。
                    staged_tasks.push((
                        name,
                        StartupCleanup::new(
                            task,
                            component_id,
                            ApplicationPhase::Ready,
                            "releasing staged component task",
                        ),
                    ));
                }
            }
            // 这里只移交所有权；所有 initializer 工厂构造与最终复验之前，Supervisor 禁止主体 poll。
            while !staged_tasks.is_empty() {
                let (name, mut task) = staged_tasks.remove(0);
                self.supervisor
                    .spawn_component_critical(
                        name,
                        Box::pin(async move { task.value_mut().await.map_err(anyhow::Error::from) }),
                    )
                    .map_err(StartupStop::Failure)?;
            }
            Ok(())
        }
        .await;
        for (_, task) in &mut staged_tasks {
            if let Some(error) = task.release() {
                report_shutdown(&error);
            }
        }
        result
    }

    /// 业务作用：在全部任务工厂构造后复验最终静态条件、预算、取消及既有任务失败，保护唯一启动发布点。
    /// 参数说明：`deadline` 是全局启动截止时刻，`broker` 提供待处理停止信号。
    /// 返回：仍可发布执行许可时成功；终止证据存在时保持屏障关闭并进入统一清理。
    async fn final_startup_check(
        &mut self,
        deadline: Instant,
        broker: &mut SignalBroker,
    ) -> Result<(), StartupStop> {
        let batch = self.application.info().mode() == ApplicationMode::Batch;
        for component in &self.components {
            if batch
                && !matches!(
                    component.metadata().id,
                    ComponentId::Observability | ComponentId::SqlObservability
                )
            {
                continue;
            }
            startup_remaining(deadline, ApplicationPhase::Ready)?;
            // 扩展实现的同步异常必须收敛到启动失败，不能绕过未执行任务收割和反向清理。
            match catch_unwind(AssertUnwindSafe(|| {
                component.owner.value().validate_ready()
            })) {
                Ok(result) => result.map_err(StartupStop::Failure)?,
                Err(payload) => {
                    release_shutdown_panic_payload(payload);
                    return Err(StartupStop::Failure(ApplicationError::new(
                        component.metadata().id,
                        ApplicationPhase::Ready,
                        "component final readiness check panicked",
                    )));
                }
            }
            // 同步检查无法被异步 timeout 抢占；返回后必须复验，尤其不能漏掉最后一个检查的耗时。
            startup_remaining(deadline, ApplicationPhase::Ready)?;
        }
        let cancellation = self.application.cancellation_token();
        loop {
            startup_remaining(deadline, ApplicationPhase::Ready)?;
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(StartupStop::Requested),
                signal = broker.next() => return Err(signal_to_startup_stop(signal)),
                completion = self.supervisor.join_next(), if self.supervisor.has_tasks() => {
                    if let Some(completion) = completion {
                        classify_startup_completion(completion)?;
                    }
                }
                _ = tokio::task::yield_now() => break,
            }
        }
        startup_remaining(deadline, ApplicationPhase::Ready)?;
        Ok(())
    }

    /// 业务作用：把 UserHook 放入 Runner 独占的 JoinSet，并处理其任务登记 ACK、panic 和启动中断。
    ///
    /// # 参数
    ///
    /// - `user_hook`：业务启动闭包；其 future 必须可在线程间移动并拥有全部捕获值。
    /// - `deadline`：整个异步启动共享的绝对截止时间。
    /// - `broker`：与 Hook 和 Supervisor 同时轮询的信号控制面。
    async fn run_user_hook<F, Fut, E>(
        &mut self,
        user_hook: F,
        allow_initializer_registration: bool,
        deadline: Instant,
        broker: &mut SignalBroker,
    ) -> Result<(), StartupStop>
    where
        F: FnOnce(Application) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Into<anyhow::Error> + 'static,
    {
        self.application.open_user_hook();
        if allow_initializer_registration {
            self.application.open_initializer_registration();
        }
        let application = self.application.clone();
        self.supervisor.spawn_user_hook(Box::pin(async move {
            user_hook(application).await.map_err(Into::into)
        }));
        let cancellation = self.application.cancellation_token();

        let result = loop {
            let remaining = match startup_remaining(deadline, ApplicationPhase::UserHook) {
                Ok(remaining) => remaining,
                Err(stop) => break Err(stop),
            };
            tokio::select! {
                event = self.supervisor.next_event() => {
                    match event {
                        SupervisorEvent::RegistrationAccepted => {}
                        SupervisorEvent::RegistrationChannelClosed => {
                            break Err(StartupStop::Failure(runner_error(
                                ApplicationPhase::UserHook,
                                "managed task registration channel closed during startup",
                            )));
                        }
                        SupervisorEvent::TaskCompleted(completion) => {
                            match completion.kind {
                                TaskKind::UserHook => break classify_user_hook_completion(completion),
                                TaskKind::Critical => {
                                    break Err(StartupStop::Failure(critical_task_error(completion)));
                                }
                                TaskKind::Background => report_background_completion(completion),
                            }
                        }
                    }
                }
                _ = cancellation.cancelled() => break Err(StartupStop::Requested),
                signal = broker.next() => break Err(signal_to_startup_stop(signal)),
                _ = tokio::time::sleep(remaining) => {
                    break Err(StartupStop::Failure(startup_timeout_error(ApplicationPhase::UserHook)));
                }
            }
        };
        // 关闭是 Hook 完成事件的第一项后续动作；仍在调度队列中的业务 future 不能越过该边界继续登记。
        self.application.close_user_hook();
        result
    }

    /// 业务作用：等待 Service 的显式停机请求、进程信号或关键任务退出。
    ///
    /// # 参数
    ///
    /// - `broker`：在整个 Running 阶段保持活跃的信号控制面。
    async fn wait_for_service_terminal(&mut self, broker: &mut SignalBroker) -> ServiceTerminal {
        let cancellation = self.application.cancellation_token();
        loop {
            if !self.supervisor.has_tasks() {
                tokio::select! {
                    _ = cancellation.cancelled() => return ServiceTerminal::Requested,
                    signal = broker.next() => return signal_to_service_terminal(signal),
                }
            }

            tokio::select! {
                _ = cancellation.cancelled() => return ServiceTerminal::Requested,
                signal = broker.next() => return signal_to_service_terminal(signal),
                completion = self.supervisor.join_next() => {
                    let Some(completion) = completion else {
                        continue;
                    };
                    match completion.kind {
                        TaskKind::Critical => {
                            debug_assert_eq!(
                                self.supervisor.task_group_state(),
                                TaskGroupState::Running
                            );
                            return ServiceTerminal::Failure(critical_task_error(completion));
                        }
                        TaskKind::Background => report_background_completion(completion),
                        TaskKind::UserHook => {
                            return ServiceTerminal::Failure(runner_error(
                                ApplicationPhase::Running,
                                "user-hook task appeared after startup completed",
                            ));
                        }
                    }
                }
            }
        }
    }

    /// 业务作用：处理启动阶段的主失败或信号中断，并保证两者共享同一反向回滚链。
    ///
    /// 参数说明：`stop` 决定终态和退出码，`broker` 在清理期间继续监听强退信号。
    /// 返回：先完成清理栈与组件释放，再返回首次主错误或带次要清理错误的信号、主动停止结果。
    async fn handle_startup_stop(
        &mut self,
        stop: StartupStop,
        broker: &mut SignalBroker,
    ) -> ApplicationResult<ApplicationExit> {
        match stop {
            StartupStop::Failure(primary) => {
                let mut primary = primary;
                // 先记录主错误再回滚，确保统一诊断通道保留一条带完整启动上下文的记录。
                crate::report::report_runtime(&primary);
                primary.mark_reported();
                self.application.set_terminal(TerminalIntent::Failure);
                self.begin_stopping()?;
                self.supervisor.close_registration().await;
                let context = ShutdownContext::new(
                    std::time::Instant::now() + self.shutdown_timeout,
                    ShutdownReason::StartupFailed,
                );
                let mut failures = self.shutdown_active_stack(&context).await;
                self.application.resources().close();
                failures.extend(self.release_components());
                self.application.clear_global();
                self.application.mark_failed()?;
                broker.stop().await;
                for failure in &failures {
                    report_shutdown(failure);
                }
                Err(primary)
            }
            StartupStop::Signal(signal) => {
                let mode = self.application.info().mode();
                let intent = match mode {
                    ApplicationMode::Service => TerminalIntent::ServiceStop,
                    ApplicationMode::Batch => TerminalIntent::BatchInterrupted,
                };
                self.application.set_terminal(intent);
                self.begin_stopping()?;
                self.supervisor.close_registration().await;
                let context = ShutdownContext::new(
                    std::time::Instant::now() + self.shutdown_timeout,
                    ShutdownReason::Signal(signal),
                );
                let mut failures = self.shutdown_active_stack(&context).await;
                self.application.resources().close();
                failures.extend(self.release_components());
                self.application.clear_global();
                self.application.mark_stopped()?;
                broker.stop().await;
                Ok(ApplicationExit {
                    reason: ApplicationExitReason::Signal(signal),
                    code: match mode {
                        ApplicationMode::Service => 0,
                        ApplicationMode::Batch => signal.exit_code(),
                    },
                    shutdown_failures: failures,
                })
            }
            StartupStop::Requested => {
                self.application.set_terminal(TerminalIntent::ServiceStop);
                self.begin_stopping()?;
                self.supervisor.close_registration().await;
                let context = ShutdownContext::new(
                    std::time::Instant::now() + self.shutdown_timeout,
                    ShutdownReason::Requested,
                );
                let mut failures = self.shutdown_active_stack(&context).await;
                self.application.resources().close();
                failures.extend(self.release_components());
                self.application.clear_global();
                self.application.mark_stopped()?;
                broker.stop().await;
                Ok(ApplicationExit {
                    reason: ApplicationExitReason::ShutdownRequested,
                    code: 0,
                    shutdown_failures: failures,
                })
            }
        }
    }

    /// 业务作用：在信号控制面自身无法建立时执行最小启动失败收敛。
    ///
    /// 参数说明：`primary` 为 handler 注册或 ready ACK 失败形成的主错误。
    /// 返回：原主错误；组件释放错误单独告警，不覆盖控制面建立失败的原因。
    async fn fail_without_broker(&mut self, mut primary: ApplicationError) -> ApplicationError {
        crate::report::report_runtime(&primary);
        primary.mark_reported();
        self.application.set_terminal(TerminalIntent::Failure);
        if self.begin_stopping().is_ok() {
            self.supervisor.close_registration().await;
            let context = ShutdownContext::new(
                std::time::Instant::now() + self.shutdown_timeout,
                ShutdownReason::StartupFailed,
            );
            let mut failures = self.shutdown_active_stack(&context).await;
            self.application.resources().close();
            failures.extend(self.release_components());
            self.application.clear_global();
            let _ = self.application.mark_failed();
            for failure in &failures {
                report_shutdown(failure);
            }
        }
        primary
    }

    /// 业务作用：发布 Stopping 状态，并保持“先提交终态、后发布状态”的顺序。
    ///
    /// # 参数
    ///
    /// 本方法无参数；调用方必须已经通过 `set_terminal` 提交非空首次终态。
    fn begin_stopping(&self) -> ApplicationResult<()> {
        debug_assert_ne!(
            self.application.terminal_intent(),
            TerminalIntent::Undecided
        );
        match self.application.state() {
            ApplicationState::Starting => self.application.mark_startup_stopping(),
            ApplicationState::Ready => self.application.mark_stopping(),
            ApplicationState::Stopping => Ok(()),
            ApplicationState::Stopped | ApplicationState::Failed => Err(runner_error(
                ApplicationPhase::Stopping,
                "cannot re-enter stopping from a terminal application state",
            )),
        }
    }

    /// 业务作用：完成正常或故障触发的 Running/Batch 反向清理并写入最终公开状态。
    ///
    /// 参数说明：
    /// - `reason`：传递给资源和 action 的首次停机原因。
    /// - `failed`：首次终态是否为框架或关键任务失败。
    /// - `broker`：清理完成前持续观察强退信号，最终状态写入后才停止。
    /// 返回：正常停止返回次要清理错误；批任务完成时组件释放异常返回主错误并发布 Failed。
    async fn finish(
        &mut self,
        reason: ShutdownReason,
        failed: bool,
        broker: &mut SignalBroker,
    ) -> ApplicationResult<Vec<ApplicationError>> {
        self.begin_stopping()?;
        let context =
            ShutdownContext::new(std::time::Instant::now() + self.shutdown_timeout, reason);
        let mut shutdown_failures = self.shutdown_active_stack(&context).await;
        self.application.resources().close();
        self.application.clear_global();
        let mut component_failures = self.release_components();
        // 已有失败、请求与信号保持首次原因；批任务完成尚需归还全部组件所有权才能交付成功结果。
        let primary = if matches!(context.reason(), ShutdownReason::BatchCompleted)
            && !component_failures.is_empty()
        {
            Some(component_failures.remove(0))
        } else {
            None
        };
        shutdown_failures.extend(component_failures);
        if failed || primary.is_some() {
            self.application.mark_failed()?;
        } else {
            self.application.mark_stopped()?;
        }
        broker.stop().await;
        match primary {
            Some(error) => Err(error),
            None => Ok(shutdown_failures),
        }
    }

    /// 业务作用：在资源收口后释放组件对象，尚未退出的任务保留最终释放责任。
    /// 参数说明：无。
    /// 返回：已同步释放对象的稳定错误；任务仍存活时移交延迟清理，仅在实际退出后释放并告警。
    fn release_components(&mut self) -> Vec<ApplicationError> {
        // 发出取消不等于任务已经退出，不能提前释放它仍可能依赖的组件对象。
        if self.supervisor.has_live_futures() {
            let cleanup = CancelledRunnerCleanup {
                application: self.application.clone(),
                active: std::mem::replace(&mut self.active, ActiveStack::new()),
                components: std::mem::take(&mut self.components),
            };
            self.defer_cancelled_cleanup(cleanup);
            return Vec::new();
        }
        let mut failures = Vec::new();
        for component in self.components.iter_mut().rev() {
            if let Some(error) = component.owner.release() {
                report_shutdown(&error);
                failures.push(error);
            }
        }
        self.components.clear();
        failures
    }

    /// 业务作用：严格逆序执行所有 active step，并让每一步只消费全局 deadline 的剩余预算。
    ///
    /// 参数说明：
    /// - `context`：携带首次停机原因和不可重置的绝对截止时间。
    ///
    /// 返回：按发生顺序累计的次要清理错误；无论成功、失败或预算耗尽都会输出一次有界停机摘要。
    async fn shutdown_active_stack(&mut self, context: &ShutdownContext) -> Vec<ApplicationError> {
        let shutdown_started = StdInstant::now();
        let planned_steps = self.active.len();
        // UserHook 失败可能尚未生成冻结计划；先撤销其登记实例，随后才执行资源栈。
        let mut failures = self.application.release_pending_initializers();
        let mut counts = ShutdownStepCounts::default();
        let mut business_shutdown_report = ShutdownTaskReport::default();
        let mut attempted_steps = 0_usize;
        let mut shutdown_sequence = 0_usize;
        let mut abandoned_steps = 0_usize;
        let mut task_abort_attempted = false;

        #[cfg(feature = "redis")]
        {
            // 消费 handler 可以依赖 initializer、数据库及缓存；在逆序栈关闭这些依赖前先排干消费。
            // 所有来源先同步关闭，再使用宿主预留预算并发等待，不因单源缓慢延迟其他来源停止。
            let partitions = self.application.redis_partitions();
            let partition_context = context.child_context(Duration::MAX);
            if let Err(error) = partitions.shutdown(partition_context.deadline()).await {
                failures.push(error);
                let cleanup = CancelledRunnerCleanup {
                    application: self.application.clone(),
                    active: std::mem::replace(&mut self.active, ActiveStack::new()),
                    components: std::mem::take(&mut self.components),
                };
                self.retain_partition_dependencies(cleanup);
                return failures;
            }
        }

        // 启动失败可能发生在正常 Seal 之前；先关闭登记集合，才能保证已经成功 ACK 的任务进入
        // 本次回滚，同时阻止任何晚到的 UserHook 调用改变停机计划。
        if let Err(error) = self.application.seal_shutdown_tasks() {
            failures.push(error);
        }
        while let Some(step) = self.active.pop() {
            if context.is_expired() {
                // 当前步骤已经出栈但尚未执行；连同仍在栈内的步骤统一计入放弃数，避免摘要谎报完成。
                abandoned_steps = planned_steps.saturating_sub(attempted_steps);
                failures.push(runner_error(
                    ApplicationPhase::Stopping,
                    "global shutdown deadline expired; remaining active steps were abandoned",
                ));
                // 放弃不再调用业务 label 或 shutdown，但必须在摘要前逐一释放 action，
                // 将析构异常保留为次要失败，不能推迟到 Runner 普通析构时才展开。
                release_abandoned_action(step, &mut failures);
                while let Some(remaining) = self.active.pop() {
                    release_abandoned_action(remaining, &mut failures);
                }
                break;
            }

            let step_started = StdInstant::now();
            let failures_before = failures.len();
            match step {
                ActiveStep::Action { component, action } => {
                    counts.component_actions += 1;
                    // 任一 action 都是公开扩展点，Runner 必须在外层强制预留后续逆序清理预算；
                    // 即使实现方误用全部 remaining，也不能阻断其后的资源释放。
                    let action_context = context.child_context(Duration::MAX);
                    let (label, action_failures) =
                        shutdown_action(action, component, None, &action_context).await;
                    failures.extend(action_failures);
                    log_shutdown_step(
                        next_shutdown_sequence(&mut shutdown_sequence),
                        "component-action",
                        component,
                        label,
                        step_started,
                        failures_before,
                        failures.len(),
                    );
                }
                ActiveStep::InitializerAction {
                    initializer,
                    action,
                } => {
                    counts.initializer_actions += 1;
                    // initializer action 与框架 action 共享同一安全边界，不能因来源不同获得耗尽
                    // 全局 deadline 的权限。
                    let action_context = context.child_context(Duration::MAX);
                    let (label, action_failures) = shutdown_action(
                        action,
                        ComponentId::Application,
                        Some(initializer.as_ref()),
                        &action_context,
                    )
                    .await;
                    failures.extend(action_failures);
                    log_shutdown_step(
                        next_shutdown_sequence(&mut shutdown_sequence),
                        "initializer-action",
                        initializer.as_ref(),
                        label,
                        step_started,
                        failures_before,
                        failures.len(),
                    );
                }
                ActiveStep::UserTasks | ActiveStep::InitializerTasks => {
                    counts.task_gates += 1;
                    // 跨 await 保留门禁，取消等待 future 不能使后续资源失去任务存活约束。
                    self.task_gate_in_progress = true;
                    // 先切状态再 cancel，Runner 随后收割到的 critical exit 才不会被误判成运行期故障。
                    self.supervisor.begin_stopping();
                    // 任务主体由业务提供，收割与强制中止同样必须预留后续资源和组件动作的预算。
                    let task_context = context.child_context(Duration::MAX);
                    while self.supervisor.has_tasks() && !task_context.is_expired() {
                        match timeout(task_context.remaining(), self.supervisor.join_next()).await {
                            Ok(Some(completion)) => {
                                if let TaskOutcome::Failed(error) = completion.outcome {
                                    failures.push(ApplicationError::with_source(
                                        ComponentId::Supervisor,
                                        ApplicationPhase::Stopping,
                                        format!(
                                            "managed task `{}` failed after stopping began",
                                            completion.name
                                        ),
                                        error,
                                    ));
                                }
                            }
                            Ok(None) => break,
                            Err(_) => break,
                        }
                    }
                    if self.supervisor.has_tasks() {
                        task_abort_attempted = true;
                        let drained = self
                            .supervisor
                            .abort_and_drain(task_context.remaining())
                            .await;
                        if !drained {
                            failures.push(runner_error(
                                ApplicationPhase::Stopping,
                                "managed tasks did not finish before their derived deadline",
                            ));
                        }
                    }
                    self.task_gate_in_progress = false;
                    log_shutdown_step(
                        next_shutdown_sequence(&mut shutdown_sequence),
                        "task-gate",
                        "supervisor",
                        None,
                        step_started,
                        failures_before,
                        failures.len(),
                    );
                }
                ActiveStep::BusinessShutdownTasks => {
                    self.shutdown_business_tasks(
                        context,
                        &mut business_shutdown_report,
                        &mut failures,
                        &mut shutdown_sequence,
                    )
                    .await;
                    log_shutdown_step(
                        next_shutdown_sequence(&mut shutdown_sequence),
                        "business-shutdown-tasks",
                        "application",
                        None,
                        step_started,
                        failures_before,
                        failures.len(),
                    );
                }
                ActiveStep::BusinessResources => {
                    counts.business_resources += 1;
                    // 全局槽在业务资源进入 Closing 前清除，阻止新的迁移期查找延长资源借用。
                    self.application.clear_global();
                    // 资源批次不得取得父上下文的全部余量，否则其中一个公开 ManagedResource
                    // 就能让后续组件动作和资源释放永久失去执行机会。
                    let resource_context = context.child_context(Duration::MAX);
                    match timeout(
                        resource_context.remaining(),
                        self.application
                            .resources()
                            .shutdown_business(&resource_context),
                    )
                    .await
                    {
                        Ok(mut resource_failures) => failures.append(&mut resource_failures),
                        Err(_) => failures.push(runner_error(
                            ApplicationPhase::Stopping,
                            "business resource shutdown exceeded its derived deadline",
                        )),
                    }
                    log_shutdown_step(
                        next_shutdown_sequence(&mut shutdown_sequence),
                        "business-resources",
                        "application",
                        None,
                        step_started,
                        failures_before,
                        failures.len(),
                    );
                }
                ActiveStep::ComponentResources(component) => {
                    counts.component_resources += 1;
                    let resource_context = context.child_context(Duration::MAX);
                    match timeout(
                        resource_context.remaining(),
                        self.application
                            .resources()
                            .shutdown_component(component, &resource_context),
                    )
                    .await
                    {
                        Ok(mut resource_failures) => failures.append(&mut resource_failures),
                        Err(_) => failures.push(ApplicationError::new(
                            component,
                            ApplicationPhase::Stopping,
                            "component resource shutdown exceeded its derived deadline",
                        )),
                    }
                    log_shutdown_step(
                        next_shutdown_sequence(&mut shutdown_sequence),
                        "component-resources",
                        component,
                        None,
                        step_started,
                        failures_before,
                        failures.len(),
                    );
                }
                ActiveStep::InitializerResources(initializer) => {
                    counts.initializer_resources += 1;
                    let resource_context = context.child_context(Duration::MAX);
                    match timeout(
                        resource_context.remaining(),
                        self.application
                            .resources()
                            .shutdown_initializer(initializer.clone(), &resource_context),
                    )
                    .await
                    {
                        Ok(mut resource_failures) => failures.append(&mut resource_failures),
                        Err(_) => failures.push(ApplicationError::new(
                            ComponentId::Application,
                            ApplicationPhase::Stopping,
                            format!(
                                "initializer `{initializer}` resource shutdown exceeded its derived deadline"
                            ),
                        )),
                    }
                    log_shutdown_step(
                        next_shutdown_sequence(&mut shutdown_sequence),
                        "initializer-resources",
                        initializer.as_ref(),
                        None,
                        step_started,
                        failures_before,
                        failures.len(),
                    );
                }
            }
            attempted_steps += 1;
        }

        // deadline 可能在更高层 action 上耗尽；仍需切换任务组并 abort，不能让 JoinSet 随普通析构失去分类信息。
        if self.supervisor.task_group_state() == TaskGroupState::Running {
            self.supervisor.begin_stopping();
        }
        if self.supervisor.has_tasks() && !task_abort_attempted {
            task_abort_attempted = true;
            let drained = self.supervisor.abort_and_drain(context.remaining()).await;
            if !drained {
                failures.push(runner_error(
                    ApplicationPhase::Stopping,
                    "remaining managed tasks were aborted but could not be reaped before the global shutdown deadline",
                ));
            }
        }
        // 高层 action 可能已经耗尽预算，使 BusinessShutdownTasks 尚未取得执行机会；仍需移交并
        // 释放剩余 future，明确记为 Abandoned，不能让容器析构掩盖任务从未执行的事实。
        match self.application.take_shutdown_tasks() {
            Ok(remaining) if !remaining.is_empty() => {
                business_shutdown_report.registered += remaining.len();
                business_shutdown_report.record_abandoned(remaining.len());
                release_abandoned_shutdown_tasks(remaining, &mut failures);
                failures.push(runner_error(
                    ApplicationPhase::Stopping,
                    "business shutdown tasks were abandoned after the group deadline expired",
                ));
            }
            Ok(_) => {}
            Err(error) => failures.push(error),
        }
        failures.extend(self.application.close_shutdown_tasks());
        let deadline_exhausted = abandoned_steps > 0
            || business_shutdown_report.abandoned > 0
            || task_abort_attempted
            || (context.is_expired() && (!failures.is_empty() || self.supervisor.has_tasks()));
        report_shutdown_summary(&ShutdownSummary {
            reason: shutdown_reason_label(context.reason()),
            planned_steps,
            attempted_steps,
            abandoned_steps,
            component_actions: counts.component_actions,
            initializer_actions: counts.initializer_actions,
            task_gates: counts.task_gates,
            business_resources: counts.business_resources,
            component_resources: counts.component_resources,
            initializer_resources: counts.initializer_resources,
            business_shutdown_registered: business_shutdown_report.registered,
            business_shutdown_attempted: business_shutdown_report.attempted,
            business_shutdown_completed: business_shutdown_report.completed,
            business_shutdown_failed: business_shutdown_report.failed,
            business_shutdown_timed_out: business_shutdown_report.timed_out,
            business_shutdown_panicked: business_shutdown_report.panicked,
            business_shutdown_abandoned: business_shutdown_report.abandoned,
            failures: failures.len(),
            task_abort_attempted,
            deadline_exhausted,
            duration: shutdown_started.elapsed(),
        });
        failures
    }

    /// 业务作用：在业务停机任务组的绝对子截止时间内按优先级顺序执行所有任务，并隔离单项终态。
    ///
    /// 参数说明：
    /// - `context`：完整 active stack 共享的停机上下文；业务任务只能消费其派生的更早截止时间。
    /// - `report`：接收登记数、实际尝试数和各终态计数的低基数报告。
    /// - `failures`：接收业务任务失败、超时、panic 或整体放弃的次要错误。
    /// - `shutdown_sequence`：本次停机事件流的单调序号，由 active step 与业务任务共同递增。
    ///
    /// 返回：无返回值；任务所有权在函数内被逐项消费，函数结束后注册表不再持有任何业务 future。
    async fn shutdown_business_tasks(
        &mut self,
        context: &ShutdownContext,
        report: &mut ShutdownTaskReport,
        failures: &mut Vec<ApplicationError>,
        shutdown_sequence: &mut usize,
    ) {
        let mut tasks = match self.application.take_shutdown_tasks() {
            Ok(tasks) => tasks,
            Err(error) => {
                failures.push(error);
                failures.extend(self.application.close_shutdown_tasks());
                return;
            }
        };
        report.registered = tasks.len();
        if tasks.is_empty() {
            failures.extend(self.application.close_shutdown_tasks());
            return;
        }

        // 业务任务组单独提前收口，为后续 BusinessResources 和更早启动的组件保留尾部预算。
        let group_context = context.child_context(Duration::MAX);
        while !tasks.is_empty() {
            if group_context.is_expired() || context.is_expired() {
                let abandoned = tasks.len();
                report.record_abandoned(abandoned);
                release_abandoned_shutdown_tasks(std::mem::take(&mut tasks), failures);
                failures.push(runner_error(
                    ApplicationPhase::Stopping,
                    "business shutdown tasks were abandoned after the group deadline expired",
                ));
                break;
            }

            let remaining_count = tasks.len();
            let fair_share = group_context.remaining() / remaining_count as u32;
            if fair_share.is_zero() {
                let abandoned = tasks.len();
                report.record_abandoned(abandoned);
                release_abandoned_shutdown_tasks(std::mem::take(&mut tasks), failures);
                failures.push(runner_error(
                    ApplicationPhase::Stopping,
                    "business shutdown tasks had no remaining execution budget",
                ));
                break;
            }

            let mut entry = tasks.remove(0);
            let task_name = entry.name.clone();
            let task_priority = entry.priority;
            let task_context = group_context.fair_child_context(fair_share);
            if task_context.is_expired() {
                // 子预算不足以取得一次可靠 poll 时，当前项及其后续项都直接释放，避免把未执行项记成失败。
                report.record_abandoned(remaining_count);
                release_abandoned_shutdown_tasks(
                    std::iter::once(entry).chain(std::mem::take(&mut tasks)),
                    failures,
                );
                failures.push(runner_error(
                    ApplicationPhase::Stopping,
                    "business shutdown tasks had no remaining fair-share budget",
                ));
                break;
            }

            let step_started = StdInstant::now();
            let failures_before = failures.len();
            // timeout 只借用 future；超时包装器释放时不能顺带在隔离边界之外析构业务捕获对象。
            let task_result = timeout(
                task_context.remaining(),
                AssertUnwindSafe(&mut entry.task).catch_unwind(),
            )
            .await;
            let (mut outcome, source) = match task_result {
                Ok(Ok(Ok(()))) => (ShutdownTaskOutcome::Completed, None),
                Ok(Ok(Err(error))) => (ShutdownTaskOutcome::Failed, Some(error)),
                Ok(Err(payload)) => {
                    // 异常对象也可能持有业务资源；在新的展开边界内释放，不读取不受信任的正文。
                    release_shutdown_panic_payload(payload);
                    (ShutdownTaskOutcome::Panicked, None)
                }
                Err(_) => (ShutdownTaskOutcome::TimedOut, None),
            };
            match (outcome, source) {
                (ShutdownTaskOutcome::Completed, None) => {}
                (ShutdownTaskOutcome::Failed, Some(error)) => {
                    failures.push(ApplicationError::with_source(
                        ComponentId::Application,
                        ApplicationPhase::Stopping,
                        format!("business shutdown task `{task_name}` failed"),
                        error,
                    ));
                }
                (ShutdownTaskOutcome::TimedOut, None) => failures.push(ApplicationError::new(
                    ComponentId::Application,
                    ApplicationPhase::Stopping,
                    format!(
                        "business shutdown task `{task_name}` exceeded its fair shutdown budget"
                    ),
                )),
                (ShutdownTaskOutcome::Panicked, None) => failures.push(ApplicationError::new(
                    ComponentId::Application,
                    ApplicationPhase::Stopping,
                    format!("business shutdown task `{task_name}` panicked during shutdown"),
                )),
                _ => unreachable!("shutdown task result and error source must match"),
            }
            // poll 已结束才释放所有权；单项析构失败归入 Panicked，原始超时或业务错误仍保留为次要证据。
            if entry.task.release() {
                outcome = ShutdownTaskOutcome::Panicked;
                failures.push(runner_error(
                    ApplicationPhase::Stopping,
                    format!("business shutdown task `{task_name}` panicked while being released"),
                ));
            }
            report.record(outcome);
            log_business_shutdown_task(
                next_shutdown_sequence(shutdown_sequence),
                task_name.as_ref(),
                task_priority,
                step_started,
                outcome_label(outcome),
                failures_before,
                failures.len(),
            );
        }
        failures.extend(self.application.close_shutdown_tasks());
    }
}

/// 业务作用：在同一执行边界隔离两类 action 的业务回调与所有权释放，保留后续逆序清理机会。
///
/// 参数说明：
/// - `action`：从 active stack 移交的受控所有权。
/// - `component`：组件 action 的稳定身份，initializer action 使用 Application。
/// - `initializer`：可选的冻结 initializer 名称，用于普通错误归因。
/// - `context`：已为后续步骤预留预算的绝对清理上下文。
///
/// 返回：成功取得的静态 label 和累计次要失败；label 展开时不再调用该 action 的 shutdown。
/// 外层取消时先释放 future 借用，再释放 action，无法回传的析构异常由守卫单独报告。
async fn shutdown_action(
    mut action: ShutdownActionCleanup<Box<dyn ShutdownAction>>,
    component: ComponentId,
    initializer: Option<&str>,
    context: &ShutdownContext,
) -> (Option<&'static str>, Vec<ApplicationError>) {
    let mut failures = Vec::new();
    let label = match catch_unwind(AssertUnwindSafe(|| action.value_mut().label())) {
        Ok(label) => Some(label),
        Err(payload) => {
            release_shutdown_panic_payload(payload);
            failures.push(action_panic_error(component, "reading its label"));
            None
        }
    };
    if let Some(label) = label {
        let description = match initializer {
            Some(initializer) => format!("initializer `{initializer}` shutdown action `{label}`"),
            None => format!("shutdown action `{label}`"),
        };
        // future 工厂也是业务代码；先建立同步展开边界，再让执行包装器仅借用 future。
        // 单独保留所有权，避免 timeout 或 catch_unwind 的析构在保护边界外释放捕获对象。
        let borrowed = action.value_mut();
        match catch_unwind(AssertUnwindSafe(move || {
            let owned_borrow = borrowed;
            owned_borrow.shutdown(context)
        })) {
            Ok(future) => {
                let mut future =
                    ShutdownActionCleanup::new(future, component, "releasing its shutdown future");
                let result = timeout(
                    context.remaining(),
                    AssertUnwindSafe(future.value_mut()).catch_unwind(),
                )
                .await;
                match result {
                    Ok(Ok(Ok(()))) => {}
                    Ok(Ok(Err(error))) => failures.push(ApplicationError::with_source(
                        component,
                        ApplicationPhase::Stopping,
                        format!("{description} failed"),
                        error,
                    )),
                    Ok(Err(payload)) => {
                        release_shutdown_panic_payload(payload);
                        failures.push(action_panic_error(component, "polling its shutdown future"));
                    }
                    Err(_) => failures.push(ApplicationError::new(
                        component,
                        ApplicationPhase::Stopping,
                        format!("{description} exceeded its derived deadline"),
                    )),
                }
                // 原错误或超时与析构异常是独立事实，逐项累计，不能用后者覆盖前者。
                failures.extend(future.release());
            }
            Err(payload) => {
                release_shutdown_panic_payload(payload);
                failures.push(action_panic_error(
                    component,
                    "creating its shutdown future",
                ));
            }
        }
    }
    // future 已释放并归还独占借用，才能释放 action 本体；两处析构分别拥有展开边界。
    failures.extend(action.release());
    (label, failures)
}

/// 业务作用：释放因全局预算耗尽而未执行的 action，不调用业务回调或冒充已尝试步骤。
///
/// 参数说明：`step` 是已经出栈的放弃步骤，`failures` 接收独立的 action 析构异常。
///
/// 返回：无返回值；非 action 步骤不执行，资源所有权仍由注册表收口。
fn release_abandoned_action(step: ActiveStep, failures: &mut Vec<ApplicationError>) {
    match step {
        ActiveStep::Action { mut action, .. }
        | ActiveStep::InitializerAction { mut action, .. } => failures.extend(action.release()),
        _ => {}
    }
}

/// 业务作用：逐项释放未取得 poll 机会的停机任务，保证某个捕获对象的析构异常不阻止其它资源释放。
///
/// 参数说明：
/// - `tasks`：已从注册表移出的未执行任务，调用方已经将其计入 Abandoned。
/// - `failures`：接收每项析构异常的次要错误，不改变未执行任务的计数。
///
/// 返回：无返回值；所有任务均尝试释放，析构异常不向外展开。
fn release_abandoned_shutdown_tasks(
    tasks: impl IntoIterator<Item = ShutdownTaskEntry>,
    failures: &mut Vec<ApplicationError>,
) {
    for mut entry in tasks {
        if entry.task.release() {
            failures.push(runner_error(
                ApplicationPhase::Stopping,
                format!(
                    "business shutdown task `{}` panicked while being released",
                    entry.name
                ),
            ));
        }
    }
}

/// 业务作用：为停机事件分配严格递增的序号，统一 active step 与业务任务的观测顺序。
///
/// 参数说明：
/// - `sequence`：本次停机事件流当前已分配的最大序号。
///
/// 返回：递增后的序号；计数到达 `usize` 上限时饱和，避免诊断路径反向中断清理。
fn next_shutdown_sequence(sequence: &mut usize) -> usize {
    *sequence = sequence.saturating_add(1);
    *sequence
}

/// 业务作用：把一个已尝试的 active step 记录为带严格序号的结构化 debug 事件，供停机顺序复盘。
///
/// 参数说明：
/// - `sequence`：本次停机事件流中从 1 开始、与业务任务事件共同递增的序号。
/// - `step_kind`：固定集合内的步骤类型。
/// - `owner`：稳定组件名、canonical initializer 名或框架所有者名。
/// - `action`：`ShutdownAction::label()` 返回的静态名称；资源和任务门没有 action 名。
/// - `started`：当前步骤开始执行的单调时钟。
/// - `failures_before`：执行当前步骤前的累计失败数。
/// - `failures_after`：执行当前步骤后的累计失败数。
///
/// 返回：无返回值；日志是否启用不影响清理结果和顺序。
fn log_shutdown_step(
    sequence: usize,
    step_kind: &'static str,
    owner: impl std::fmt::Display,
    action: Option<&'static str>,
    started: StdInstant,
    failures_before: usize,
    failures_after: usize,
) {
    let failures_added = failures_after.saturating_sub(failures_before);
    tracing::debug!(
        shutdown_sequence = sequence,
        shutdown_step = step_kind,
        owner = %owner,
        action = action.unwrap_or("-"),
        duration_seconds = started.elapsed().as_secs_f64(),
        failures_added,
        outcome = if failures_added == 0 {
            "completed"
        } else {
            "failed"
        },
        "application shutdown step completed"
    );
}

/// 业务作用：记录一次实际取得 poll 机会的业务停机任务，供排查任务顺序与公平预算分配。
///
/// 参数说明：
/// - `sequence`：本次停机事件流内从 1 开始、与 active step 事件共同递增的序号。
/// - `name`：启动期校验后的稳定任务名。
/// - `priority`：启动期登记的业务优先级。
/// - `started`：当前任务开始执行的单调时钟。
/// - `outcome`：固定集合内的任务终态名称。
/// - `failures_before`：当前任务执行前的累计失败数。
/// - `failures_after`：当前任务执行后的累计失败数。
///
/// 返回：无返回值；日志关闭时不影响任务结果。
fn log_business_shutdown_task(
    sequence: usize,
    name: &str,
    priority: i32,
    started: StdInstant,
    outcome: &'static str,
    failures_before: usize,
    failures_after: usize,
) {
    tracing::debug!(
        shutdown_sequence = sequence,
        shutdown_step = "business-shutdown-task",
        shutdown_task = name,
        shutdown_priority = priority,
        duration_seconds = started.elapsed().as_secs_f64(),
        failures_added = failures_after.saturating_sub(failures_before),
        outcome,
        "business shutdown task completed"
    );
}

/// 业务作用：把业务停机任务终态映射为固定观测名称。
///
/// 参数说明：
/// - `outcome`：Runner 归一化的业务停机任务终态。
///
/// 返回：不含任务名、错误正文或捕获值的稳定分类。
fn outcome_label(outcome: ShutdownTaskOutcome) -> &'static str {
    match outcome {
        ShutdownTaskOutcome::Completed => "completed",
        ShutdownTaskOutcome::Failed => "failed",
        ShutdownTaskOutcome::TimedOut => "timed_out",
        ShutdownTaskOutcome::Panicked => "panicked",
        ShutdownTaskOutcome::Abandoned => "abandoned",
    }
}

/// 业务作用：把首次停机原因映射为固定、无业务输入的摘要分类。
///
/// 参数说明：
/// - `reason`：贯穿全部清理步骤且不可变的首次停机原因。
///
/// 返回：用于同步停机摘要的稳定短名称。
fn shutdown_reason_label(reason: &ShutdownReason) -> &'static str {
    match reason {
        ShutdownReason::Requested => "requested",
        ShutdownReason::Signal(signal) => signal.name(),
        ShutdownReason::BatchCompleted => "batch-completed",
        ShutdownReason::CriticalTaskFailed => "critical-task-failed",
        ShutdownReason::StartupFailed => "startup-failed",
        ShutdownReason::ComponentFailed => "component-failed",
    }
}

/// 业务作用：在 initializer future、panic 边界、全局 deadline、取消、信号与关键任务之间统一裁决。
///
/// 参数说明：
/// - `future`：当前工厂或三轮屏障的唯一受监督 future。
/// - `name`：冻结计划中的 canonical initializer 身份。
/// - `stage`：当前工厂或屏障阶段。
/// - `deadline`：完整启动流程共享的绝对截止时刻。
/// - `application`：提供启动取消观察的应用容器。
/// - `supervisor`：用于在初始化阻塞时继续收割早退任务。
/// - `broker`：启动期间的进程信号控制面。
///
/// 返回：future 成功时返回业务值；错误、panic、超时或中断收敛为单一 `StartupStop`。
async fn await_initializer_future<T, F>(
    future: F,
    name: Arc<str>,
    stage: InitializerStage,
    deadline: Instant,
    application: &Application,
    supervisor: &mut TaskSupervisor,
    broker: &mut SignalBroker,
) -> Result<T, StartupStop>
where
    F: Future<Output = ApplicationResult<T>> + Send,
{
    let started = StdInstant::now();
    let mut owned = StartupCleanup::new(
        Box::pin(future),
        ComponentId::Application,
        ApplicationPhase::Initialization,
        "releasing initializer future",
    );
    let result = async {
        let future = AssertUnwindSafe(owned.value_mut()).catch_unwind();
        tokio::pin!(future);
        let cancellation = application.cancellation_token();
        loop {
            let remaining = startup_remaining(deadline, ApplicationPhase::Initialization)?;
            tokio::select! {
                result = &mut future => {
                    return match result {
                        Ok(Ok(value)) => {
                            crate::initialization::record_duration(
                                application,
                                &name,
                                stage,
                                started.elapsed(),
                            );
                            Ok(value)
                        }
                        Ok(Err(error)) => {
                            crate::initialization::record_duration(
                                application,
                                &name,
                                stage,
                                started.elapsed(),
                            );
                            crate::initialization::record_failure(
                                application,
                                &name,
                                stage,
                                InitializerFailureKind::Error,
                            );
                            Err(StartupStop::Failure(initializer_failure_error(
                                name,
                                stage,
                                InitializerFailureKind::Error,
                                Some(anyhow::Error::new(error)),
                            )))
                        }
                        Err(payload) => {
                            release_shutdown_panic_payload(payload);
                            crate::initialization::record_duration(
                                application,
                                &name,
                                stage,
                                started.elapsed(),
                            );
                            crate::initialization::record_failure(
                                application,
                                &name,
                                stage,
                                InitializerFailureKind::Panicked,
                            );
                            Err(StartupStop::Failure(initializer_failure_error(
                                name,
                                stage,
                                InitializerFailureKind::Panicked,
                                None,
                            )))
                        }
                    };
                }
                _ = cancellation.cancelled() => {
                    crate::initialization::record_duration(application, &name, stage, started.elapsed());
                    crate::initialization::record_failure(
                        application,
                        &name,
                        stage,
                        InitializerFailureKind::Cancelled,
                    );
                    return Err(StartupStop::Requested);
                }
                signal = broker.next() => {
                    crate::initialization::record_duration(application, &name, stage, started.elapsed());
                    crate::initialization::record_failure(
                        application,
                        &name,
                        stage,
                        InitializerFailureKind::Cancelled,
                    );
                    return Err(signal_to_startup_stop(signal));
                }
                completion = supervisor.join_next(), if supervisor.has_tasks() => {
                    if let Some(completion) = completion {
                        if let Err(stop) = classify_startup_completion(completion) {
                            crate::initialization::record_duration(application, &name, stage, started.elapsed());
                            crate::initialization::record_failure(
                                application,
                                &name,
                                stage,
                                InitializerFailureKind::Cancelled,
                            );
                            return Err(stop);
                        }
                    }
                }
                _ = tokio::time::sleep(remaining) => {
                    crate::initialization::record_duration(application, &name, stage, started.elapsed());
                    crate::initialization::record_failure(
                        application,
                        &name,
                        stage,
                        InitializerFailureKind::TimedOut,
                    );
                    return Err(StartupStop::Failure(initializer_failure_error(
                        name,
                        stage,
                        InitializerFailureKind::TimedOut,
                        None,
                    )));
                }
            }
        }
    }
    .await;
    finish_startup_release(result, owned.release())
}

/// 业务作用：构造带 initializer 身份、阶段和低基数分类的全局初始化错误。
///
/// 参数说明：
/// - `name`：冻结计划中的 canonical 身份。
/// - `stage`：失败所属工厂、三轮屏障或激活阶段。
/// - `kind`：不依赖底层错误文本的稳定分类。
/// - `source`：可选底层错误，只交给统一脱敏诊断链。
///
/// 返回：归因到 Application/Initialization 的公开错误。
fn initializer_failure_error(
    name: Arc<str>,
    stage: InitializerStage,
    kind: InitializerFailureKind,
    source: Option<anyhow::Error>,
) -> ApplicationError {
    let message = format!("initializer `{name}` failed during {stage}: {kind}");
    let failure = match source {
        Some(source) => InitializerFailure::with_source(name, stage, source),
        None => InitializerFailure::new(name, stage, kind),
    };
    ApplicationError::with_source(
        ComponentId::Application,
        ApplicationPhase::Initialization,
        message,
        failure,
    )
}

/// 业务作用：持有真实扩展 future 到结果裁决之后，避免 await 的隐式析构覆盖已返回的业务错误。
/// 参数说明：`future` 是扩展返回的任务；`component` 和 `phase` 提供释放失败归因。
/// 返回：业务失败保持原样；成功后的析构异常阻止启动，外层取消时守卫仍隔离释放。
fn retain_startup_future<T, F>(
    future: F,
    component: ComponentId,
    phase: ApplicationPhase,
) -> impl Future<Output = ApplicationResult<T>>
where
    F: Future<Output = ApplicationResult<T>>,
{
    let mut owned = StartupCleanup::new(
        Box::pin(future),
        component,
        phase,
        "releasing extension future",
    );
    async move {
        let result = owned.value_mut().await;
        match (result, owned.release()) {
            (Ok(_), Some(error)) => Err(error),
            (result, release) => {
                if let Some(error) = release {
                    report_shutdown(&error);
                }
                result
            }
        }
    }
}

/// 业务作用：隔离组件阶段的展开，使已登记副作用继续由统一异步回滚负责。
/// 参数说明：`future` 须包含 trait 方法调用及其返回 future 的轮询；`component` 与 `phase` 提供稳定归因。
/// 返回：正常结果保持不变；单次展开转为当前组件阶段错误，不读取异常正文。
fn isolate_component_startup<F>(
    future: F,
    component: ComponentId,
    phase: ApplicationPhase,
) -> impl Future<Output = ApplicationResult<()>>
where
    F: Future<Output = ApplicationResult<()>>,
{
    let mut owned = StartupCleanup::new(
        Box::pin(future),
        component,
        phase,
        "releasing component future",
    );
    async move {
        let result = AssertUnwindSafe(owned.value_mut())
            .catch_unwind()
            .await
            .unwrap_or_else(|payload| Err(component_startup_panic(component, phase, payload)));
        match (result, owned.release()) {
            (Ok(()), Some(error)) => Err(error),
            (result, release) => {
                if let Some(error) = release {
                    report_shutdown(&error);
                }
                result
            }
        }
    }
}

/// 业务作用：在启动等待结束后合并释放结果，保持首次错误或停止原因的优先级。
/// 参数说明：`result` 是已裁决的阶段结果；`release` 是停止轮询后独立析构产生的错误。
/// 返回：成功阶段的释放异常阻止启动；已有中断保持原归因，次要释放异常单独报告。
fn finish_startup_release<T>(
    result: Result<T, StartupStop>,
    release: Option<ApplicationError>,
) -> Result<T, StartupStop> {
    match (result, release) {
        (Ok(_), Some(error)) => Err(StartupStop::Failure(error)),
        (result, release) => {
            if let Some(error) = release {
                report_shutdown(&error);
            }
            result
        }
    }
}

/// 业务作用：将组件展开转换为可回滚的固定错误，并隔离异常对象自身的析构。
/// 参数说明：`component` 与 `phase` 标识失败边界；`payload` 只用于释放所有权，不参与诊断内容。
/// 返回：保留组件与生命周期归因的首次失败，随后清理错误由统一回滚另行记录。
fn component_startup_panic(
    component: ComponentId,
    phase: ApplicationPhase,
    payload: Box<dyn std::any::Any + Send>,
) -> ApplicationError {
    crate::shutdown::release_shutdown_panic_payload(payload);
    ApplicationError::new(component, phase, "component panicked during startup")
}

/// 业务作用：裁决组件启动、绝对 deadline、显式取消与信号，并在回滚前隔离等待值的释放。
///
/// 参数说明：
///
/// - `future`：当前组件阶段的受监督异步动作。
/// - `deadline`：整个启动流程共享的绝对截止时间。
/// - `phase`：超时时写入错误上下文的生命周期阶段。
/// - `application`：提供显式停机请求的取消令牌。
/// - `broker`：启动期间持续轮询的信号控制面。
///
/// 返回：阶段和释放均成功时继续启动；否则保留首次失败或中断，交由调用方统一回滚。
async fn await_startup_future<F>(
    future: F,
    deadline: Instant,
    phase: ApplicationPhase,
    application: &Application,
    broker: &mut SignalBroker,
) -> Result<(), StartupStop>
where
    F: Future<Output = ApplicationResult<()>>,
{
    let mut owned = StartupCleanup::new(
        Box::pin(future),
        ComponentId::Application,
        phase,
        "releasing startup wait",
    );
    let cancellation = application.cancellation_token();
    let result = async {
        let remaining = startup_remaining(deadline, phase)?;
        tokio::select! {
            result = owned.value_mut() => result.map_err(StartupStop::Failure),
            _ = cancellation.cancelled() => Err(StartupStop::Requested),
            signal = broker.next() => Err(signal_to_startup_stop(signal)),
            _ = tokio::time::sleep(remaining) => {
                Err(StartupStop::Failure(startup_timeout_error(phase)))
            }
        }
    }
    .await;
    finish_startup_release(result, owned.release())
}

/// 业务作用：计算当前启动阶段可消费的剩余预算。
///
/// # 参数
///
/// - `deadline`：同步预检完成后确定的绝对启动截止时间。
/// - `phase`：预算耗尽时用于定位的生命周期阶段。
fn startup_remaining(deadline: Instant, phase: ApplicationPhase) -> Result<Duration, StartupStop> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(StartupStop::Failure(startup_timeout_error(phase)));
    }
    Ok(remaining)
}

/// 业务作用：构造统一的全局启动超时错误。
///
/// # 参数
///
/// - `phase`：预算首次被观察为耗尽的生命周期阶段。
fn startup_timeout_error(phase: ApplicationPhase) -> ApplicationError {
    runner_error(phase, "application startup exceeded the global deadline")
}

/// 业务作用：校验会与 `Instant` 相加的生命周期预算，阻止零值和跨平台溢出值进入 Runner。
fn validate_lifecycle_timeout(
    timeout: Duration,
    phase: ApplicationPhase,
    kind: &str,
) -> ApplicationResult<()> {
    if timeout.is_zero() || timeout > MAX_LIFECYCLE_TIMEOUT {
        return Err(runner_error(
            phase,
            format!("application {kind} timeout must be within (0, 365 days]"),
        ));
    }
    Ok(())
}

/// 业务作用：把信号控制面事件转换为启动阶段中断。
///
/// # 参数
///
/// - `event`：真实信号或 Broker 意外关闭事件。
fn signal_to_startup_stop(event: SignalEvent) -> StartupStop {
    match event {
        SignalEvent::Received(signal) => StartupStop::Signal(signal),
        SignalEvent::BrokerClosed => StartupStop::Failure(runner_error(
            ApplicationPhase::Bootstrap,
            "signal broker closed before application startup completed",
        )),
    }
}

/// 业务作用：把信号控制面事件转换为 Service 运行期终止分类。
///
/// # 参数
///
/// - `event`：真实信号或 Broker 意外关闭事件。
fn signal_to_service_terminal(event: SignalEvent) -> ServiceTerminal {
    match event {
        SignalEvent::Received(signal) => ServiceTerminal::Signal(signal),
        SignalEvent::BrokerClosed => ServiceTerminal::Failure(runner_error(
            ApplicationPhase::Running,
            "signal broker closed while service was running",
        )),
    }
}

/// 业务作用：分类 Ready 阶段观察到的受管任务退出。
///
/// # 参数
///
/// - `completion`：Supervisor 收割到的任务身份、角色和退出结果。
fn classify_startup_completion(completion: TaskCompletion) -> Result<(), StartupStop> {
    match completion.kind {
        TaskKind::Critical => Err(StartupStop::Failure(critical_task_error(completion))),
        TaskKind::Background => {
            report_background_completion(completion);
            Ok(())
        }
        TaskKind::UserHook => Err(StartupStop::Failure(runner_error(
            ApplicationPhase::Ready,
            "user-hook task appeared after startup hook completed",
        ))),
    }
}

/// 业务作用：记录后台任务的提前退出，不把可降级任务提升为应用主失败。
///
/// 成功完成同样会移出监督表，但无需产生告警；失败和 panic 使用稳定任务名与统一脱敏管道记录，
/// 使“继续运行”不等于“静默丢失故障”。
///
/// 参数说明：
/// - `completion`：监督器收割到的后台任务身份与退出结果。
///
/// 返回：无返回值；业务错误报告及释放的单次 panic 不得把可降级退出升级为应用主失败。
fn report_background_completion(completion: TaskCompletion) {
    match completion.outcome {
        TaskOutcome::Completed | TaskOutcome::Cancelled => {}
        TaskOutcome::Failed(error) => {
            // 可降级任务的错误对象同样来自业务；先纳入统一所有权边界，避免报告或析构异常击穿 Runner。
            let error = ApplicationError::with_source(
                ComponentId::Supervisor,
                ApplicationPhase::Running,
                "background managed task failed",
                error,
            );
            let summary =
                crate::report::bounded(&crate::report::redact(&crate::report::error_chain(&error)));
            tracing::warn!(
                "background managed task `{}` (id={}) failed and was removed: {summary}",
                completion.name,
                completion.id.get()
            );
        }
        TaskOutcome::Panicked => {
            tracing::warn!(
                "background managed task `{}` (id={}) panicked and was removed",
                completion.name,
                completion.id.get()
            );
        }
    }
}

/// 业务作用：把 UserHook 的 JoinSet 退出结果转换为启动结果。
///
/// # 参数
///
/// - `completion`：保留 UserHook 任务身份但不暴露原始 panic payload 的退出记录。
fn classify_user_hook_completion(completion: TaskCompletion) -> Result<(), StartupStop> {
    match completion.outcome {
        TaskOutcome::Completed => Ok(()),
        TaskOutcome::Failed(error) => Err(StartupStop::Failure(ApplicationError::with_source(
            ComponentId::UserHook,
            ApplicationPhase::UserHook,
            "application user hook failed",
            error,
        ))),
        TaskOutcome::Panicked => Err(StartupStop::Failure(ApplicationError::new(
            ComponentId::UserHook,
            ApplicationPhase::UserHook,
            "application user hook panicked",
        ))),
        TaskOutcome::Cancelled => Err(StartupStop::Failure(ApplicationError::new(
            ComponentId::UserHook,
            ApplicationPhase::UserHook,
            "application user hook was cancelled",
        ))),
    }
}

/// 业务作用：构造关键任务提前退出的主错误，不读取 panic payload。
///
/// # 参数
///
/// - `completion`：关键任务的稳定名称、TaskId 和退出结果。
fn critical_task_error(completion: TaskCompletion) -> ApplicationError {
    let message = format!(
        "critical managed task `{}` (id={}) terminated while its group was running",
        completion.name,
        completion.id.get()
    );
    match completion.outcome {
        TaskOutcome::Failed(error) => ApplicationError::with_source(
            ComponentId::Supervisor,
            ApplicationPhase::Running,
            message,
            error,
        ),
        TaskOutcome::Completed | TaskOutcome::Panicked | TaskOutcome::Cancelled => {
            ApplicationError::new(ComponentId::Supervisor, ApplicationPhase::Running, message)
        }
    }
}

/// 业务作用：创建 Runner 自身的稳定错误形状。
///
/// # 参数
///
/// - `phase`：错误发生或被观察到的生命周期阶段。
/// - `message`：不包含配置值和业务秘密的稳定摘要。
fn runner_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Application, phase, message)
}
