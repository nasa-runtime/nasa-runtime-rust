use std::{
    future::Future,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::{Duration, Instant as StdInstant},
};

use futures_util::FutureExt;
use tokio::time::{timeout, Instant};

use crate::{
    component::{ActiveStack, ActiveStep},
    initialization::{
        freeze_plan, order_enabled, EnabledInitializer, FrozenInitializer, FrozenInitializerPlan,
        InitializerFailure, InitializerFailureKind, InitializerStage, StagedInitializerTask,
    },
    report::{report_shutdown, report_shutdown_summary, ShutdownSummary},
    signal::{SignalBroker, SignalEvent, SignalMode},
    state::TerminalIntent,
    supervisor::{
        SupervisorEvent, TaskCompletion, TaskGroupState, TaskKind, TaskOutcome, TaskSupervisor,
    },
    Application, ApplicationComponent, ApplicationError, ApplicationInfo, ApplicationMode,
    ApplicationPhase, ApplicationResult, ApplicationState, BootstrapContext, ComponentId,
    ConfigView, InitializationContext, PrepareContext, ReadyContext, ShutdownContext,
    ShutdownReason, ShutdownSignal, StartContext,
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
    components: Vec<Box<dyn ApplicationComponent>>,
    active: ActiveStack,
    signal_mode: SignalMode,
    startup_timeout: Duration,
    shutdown_timeout: Duration,
}

impl ApplicationRunner {
    /// 业务作用：使用已完成同步预检的应用信息和配置视图创建 Runner。
    ///
    /// # 参数
    ///
    /// - `info`：已经固定名称、profile 和 Service/Batch 模式的进程元数据。
    /// - `initial_config`：版本为 1 的不可变配置视图。
    #[doc(hidden)]
    pub fn new(info: ApplicationInfo, initial_config: Arc<ConfigView>) -> Self {
        let (application, supervisor) = Application::create(info, initial_config);
        Self {
            application,
            supervisor,
            components: Vec::new(),
            active: ActiveStack::new(),
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
    /// # 参数
    ///
    /// - `component`：由 Runner 独占所有权并依次执行 Bootstrap、Start、Prepare 和 Ready 的组件对象。
    #[doc(hidden)]
    pub fn with_component(mut self, component: Box<dyn ApplicationComponent>) -> Self {
        // 这里是唯一完整观察组件表的位置；先发布声明位，全部能力入口才能用同一来源区分未声明与未就绪。
        self.application.mark_component_declared(component.id());
        self.components.push(component);
        self
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
    /// 无法建立生命周期控制面或无法提交最终状态时返回主错误。
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
                    return self.handle_startup_stop(stop, &mut broker).await;
                }
                if let Err(stop) = self
                    .run_initializers(plan, startup_deadline, &mut broker)
                    .await
                {
                    return self.handle_startup_stop(stop, &mut broker).await;
                }

                // 工作负载可登记现有业务资源/任务，但 initializer 登记门从未开放。
                self.active.push_business_resources();
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

                // Mapper L2 兜底断言（Service 专属）：存在 cache=true 的 Mapper 查询却没有在
                // Hook 中显式安装 L2 时，在对外提供服务之前 fail-fast，避免生产流量静默绕过缓存。
                // 断言放在 Hook 之后，业务装配已经完成；Batch 不做该断言，因为它的 Hook 本身就是
                // 工作负载，事后断言会把已经完成的批任务错误改判为失败。
                #[cfg(feature = "mapper-cache")]
                if let Err(error) = crate::mapper_cache::ensure_mapper_l2_installed() {
                    return self
                        .handle_startup_stop(StartupStop::Failure(error), &mut broker)
                        .await;
                }
                if let Err(error) = self.seal_initialization() {
                    return self
                        .handle_startup_stop(StartupStop::Failure(error), &mut broker)
                        .await;
                }
                // 全局 Weak 槽在 sealed 后、Ready 前安装，Ready action 可以构造需要 Application 的终端资源。
                if let Err(error) = self.application.install_global() {
                    return self
                        .handle_startup_stop(StartupStop::Failure(error), &mut broker)
                        .await;
                }
                if let Err(stop) = self.ready_components(startup_deadline, &mut broker).await {
                    return self.handle_startup_stop(stop, &mut broker).await;
                }
                if let Err(stop) = self
                    .activate_initializer_tasks(staged_tasks, startup_deadline, &mut broker)
                    .await
                {
                    return self.handle_startup_stop(stop, &mut broker).await;
                }
                if let Err(error) = self.application.mark_ready() {
                    return self
                        .handle_startup_stop(StartupStop::Failure(error), &mut broker)
                        .await;
                }

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
            .map(|component| component.id())
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
        let mut declared = std::collections::HashSet::new();
        for component in &self.components {
            let id = component.id();
            if !declared.insert(id) {
                return Err(runner_error(
                    ApplicationPhase::Bootstrap,
                    format!("component `{id}` is declared more than once"),
                ));
            }
            for dependency in component.dependencies() {
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
    /// # 参数
    ///
    /// - `deadline`：整个异步启动共享的绝对截止时间。
    /// - `broker`：在组件 future 卡住时仍被并发轮询的信号控制面。
    async fn bootstrap_components(
        &mut self,
        deadline: Instant,
        broker: &mut SignalBroker,
    ) -> Result<(), StartupStop> {
        for component in &mut self.components {
            let component_id = component.id();
            let mut context = BootstrapContext::new(
                &self.application,
                component_id,
                &mut self.active,
                deadline.into(),
            );
            await_startup_future(
                component.bootstrap(&mut context),
                deadline,
                ApplicationPhase::Bootstrap,
                broker,
            )
            .await?;
        }
        Ok(())
    }

    /// 业务作用：按声明顺序执行所有组件的 Start 阶段。
    ///
    /// # 参数
    ///
    /// - `deadline`：与前序阶段共享的绝对启动截止时间。
    /// - `broker`：持续观察启动中断信号的控制面。
    async fn start_components(
        &mut self,
        deadline: Instant,
        broker: &mut SignalBroker,
    ) -> Result<(), StartupStop> {
        for component in &mut self.components {
            let component_id = component.id();
            let mut context = StartContext::new(
                &self.application,
                component_id,
                &mut self.active,
                deadline.into(),
            );
            await_startup_future(
                component.start(&mut context),
                deadline,
                ApplicationPhase::Start,
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
    /// 返回：全部出站门禁通过时成功；组件失败、任务失败、超时或终止时返回唯一启动中断。
    async fn prepare_components(
        &mut self,
        deadline: Instant,
        broker: &mut SignalBroker,
    ) -> Result<(), StartupStop> {
        let cancellation = self.application.cancellation_token();
        for component in &mut self.components {
            let component_id = component.id();
            let mut context = PrepareContext::new(
                &self.application,
                component_id,
                &mut self.active,
                deadline.into(),
            );
            let future = component.prepare(&mut context);
            tokio::pin!(future);
            loop {
                let remaining = startup_remaining(deadline, ApplicationPhase::Prepare)?;
                tokio::select! {
                    result = &mut future => {
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
    /// 返回：三轮全部成功时返回尚未构造/轮询的长期任务工厂；任一失败立即停止后续项。
    async fn run_initializers(
        &mut self,
        plan: FrozenInitializerPlan,
        deadline: Instant,
        broker: &mut SignalBroker,
    ) -> Result<Vec<StagedInitializerTask>, StartupStop> {
        let mut enabled = Vec::with_capacity(plan.entries.len());
        for entry in plan.entries {
            match entry {
                FrozenInitializer::Static { spec, factory } => {
                    let name: Arc<str> = Arc::from(spec.name());
                    let started = StdInstant::now();
                    let future = std::panic::catch_unwind(AssertUnwindSafe(|| {
                        factory(self.application.clone())
                    }))
                    .map_err(|_| {
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
                        future,
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

        let mut ordered = order_enabled(enabled).map_err(StartupStop::Failure)?;
        let mut staged_tasks = Vec::new();
        for stage in [
            InitializerStage::Before,
            InitializerStage::Initialize,
            InitializerStage::After,
        ] {
            for entry in &mut ordered {
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
                        InitializerStage::Before => entry.initializer.before(&mut context).await,
                        InitializerStage::Initialize => {
                            entry.initializer.initialize(&mut context).await
                        }
                        InitializerStage::After => entry.initializer.after(&mut context).await,
                        InitializerStage::Factory | InitializerStage::Activation => unreachable!(
                            "initializer barrier only executes before, initialize, and after"
                        ),
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
        Ok(staged_tasks)
    }

    /// 业务作用：封存 initializer 登记期允许扩展的资源 key 和 readiness 名称集合。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：资源容器首次封存成功时返回成功；重复或非法阶段返回 Seal 错误。
    fn seal_initialization(&self) -> ApplicationResult<()> {
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

    /// 业务作用：在全部组件 Ready action 成功后激活 initializer 暂存的受管任务。
    ///
    /// 参数说明：
    /// - `tasks`：`stage_*` 保存的一次性任务工厂。
    /// - `deadline`：激活仍必须遵守的共享启动截止时刻。
    /// - `broker`：每个任务激活边界都复验的启动信号控制面。
    ///
    /// 返回：全部任务已加入 Supervisor 时成功；超时、工厂 panic 或名称冲突时停止接流。
    async fn activate_initializer_tasks(
        &mut self,
        tasks: Vec<StagedInitializerTask>,
        deadline: Instant,
        broker: &mut SignalBroker,
    ) -> Result<(), StartupStop> {
        if tasks.is_empty() {
            return Ok(());
        }
        for task in tasks {
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
            let future =
                std::panic::catch_unwind(AssertUnwindSafe(|| (task.factory)(token.clone())))
                    .map_err(|_| {
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
            let application = self.application.clone();
            let guarded = Box::pin(async move {
                let mut states = application.subscribe_state();
                loop {
                    match *states.borrow() {
                        ApplicationState::Starting => {}
                        ApplicationState::Ready => break,
                        ApplicationState::Stopping
                        | ApplicationState::Stopped
                        | ApplicationState::Failed => return Ok(()),
                    }
                    tokio::select! {
                        _ = token.cancelled() => return Ok(()),
                        changed = states.changed() => {
                            if changed.is_err() {
                                return Ok(());
                            }
                        }
                    }
                }
                future.await
            });
            self.supervisor
                .spawn_initializer_task(task.name, task.kind, guarded)
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

    /// 业务作用：按声明顺序执行 Service 组件的 Ready 阶段，同时监督已经登记的关键任务。
    ///
    /// # 参数
    ///
    /// - `deadline`：与 Bootstrap、Start、UserHook、Prepare 和 Initialization 共享的启动截止时间。
    /// - `broker`：持续观察启动中断信号的控制面。
    async fn ready_components(
        &mut self,
        deadline: Instant,
        broker: &mut SignalBroker,
    ) -> Result<(), StartupStop> {
        for component in &mut self.components {
            let component_id = component.id();
            let mut context = ReadyContext::new(
                &self.application,
                component_id,
                &mut self.active,
                deadline.into(),
            );
            {
                // Ready future 的可变组件借用必须在取终端任务前结束，避免两个生命周期操作交叠。
                let future = component.ready(&mut context);
                tokio::pin!(future);
                loop {
                    let remaining = startup_remaining(deadline, ApplicationPhase::Ready)?;
                    tokio::select! {
                        result = &mut future => {
                            result.map_err(StartupStop::Failure)?;
                            break;
                        }
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
            }
            if let Some((name, task)) = component.take_critical_task() {
                // Ready action 已经压栈后才加入监督集合，保证任务即刻退出时仍存在完整回滚路径。
                self.supervisor
                    .spawn_component_critical(
                        name,
                        Box::pin(async move { task.await.map_err(anyhow::Error::from) }),
                    )
                    .map_err(StartupStop::Failure)?;
            }
        }
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
    /// # 参数
    ///
    /// - `stop`：决定终态和退出码的启动中断分类。
    /// - `broker`：清理期间继续监听停机中强退信号的控制面。
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
                let failures = self.shutdown_active_stack(&context).await;
                self.application.resources().close();
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
                let failures = self.shutdown_active_stack(&context).await;
                self.application.resources().close();
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
                let failures = self.shutdown_active_stack(&context).await;
                self.application.resources().close();
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
    /// # 参数
    ///
    /// - `primary`：handler 注册或 ready ACK 失败形成的主错误。
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
            let failures = self.shutdown_active_stack(&context).await;
            self.application.resources().close();
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
    /// # 参数
    ///
    /// - `reason`：传递给资源和 action 的首次停机原因。
    /// - `failed`：首次终态是否为框架或关键任务失败。
    /// - `broker`：清理完成前持续观察强退信号，最终状态写入后才停止。
    async fn finish(
        &mut self,
        reason: ShutdownReason,
        failed: bool,
        broker: &mut SignalBroker,
    ) -> ApplicationResult<Vec<ApplicationError>> {
        self.begin_stopping()?;
        let context =
            ShutdownContext::new(std::time::Instant::now() + self.shutdown_timeout, reason);
        let shutdown_failures = self.shutdown_active_stack(&context).await;
        self.application.resources().close();
        self.application.clear_global();
        if failed {
            self.application.mark_failed()?;
        } else {
            self.application.mark_stopped()?;
        }
        broker.stop().await;
        Ok(shutdown_failures)
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
        let mut failures = Vec::new();
        let mut counts = ShutdownStepCounts::default();
        let mut attempted_steps = 0_usize;
        let mut abandoned_steps = 0_usize;
        let mut task_abort_attempted = false;
        while let Some(step) = self.active.pop() {
            if context.is_expired() {
                // 当前步骤已经出栈但尚未执行；连同仍在栈内的步骤统一计入放弃数，避免摘要谎报完成。
                abandoned_steps = planned_steps.saturating_sub(attempted_steps);
                failures.push(runner_error(
                    ApplicationPhase::Stopping,
                    "global shutdown deadline expired; remaining active steps were abandoned",
                ));
                break;
            }

            let sequence = attempted_steps + 1;
            let step_started = StdInstant::now();
            let failures_before = failures.len();
            match step {
                ActiveStep::Action {
                    component,
                    mut action,
                } => {
                    counts.component_actions += 1;
                    let label = action.label();
                    match timeout(context.remaining(), action.shutdown(context)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => failures.push(ApplicationError::with_source(
                            component,
                            ApplicationPhase::Stopping,
                            format!("shutdown action `{label}` failed"),
                            error,
                        )),
                        Err(_) => failures.push(ApplicationError::new(
                            component,
                            ApplicationPhase::Stopping,
                            format!("shutdown action `{label}` exceeded the global deadline"),
                        )),
                    }
                    log_shutdown_step(
                        sequence,
                        "component-action",
                        component,
                        Some(label),
                        step_started,
                        failures_before,
                        failures.len(),
                    );
                }
                ActiveStep::InitializerAction {
                    initializer,
                    mut action,
                } => {
                    counts.initializer_actions += 1;
                    let label = action.label();
                    match timeout(context.remaining(), action.shutdown(context)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => failures.push(ApplicationError::with_source(
                            ComponentId::Application,
                            ApplicationPhase::Stopping,
                            format!(
                                "initializer `{initializer}` shutdown action `{label}` failed"
                            ),
                            error,
                        )),
                        Err(_) => failures.push(ApplicationError::new(
                            ComponentId::Application,
                            ApplicationPhase::Stopping,
                            format!(
                                "initializer `{initializer}` shutdown action `{label}` exceeded the global deadline"
                            ),
                        )),
                    }
                    log_shutdown_step(
                        sequence,
                        "initializer-action",
                        initializer.as_ref(),
                        Some(label),
                        step_started,
                        failures_before,
                        failures.len(),
                    );
                }
                ActiveStep::UserTasks | ActiveStep::InitializerTasks => {
                    counts.task_gates += 1;
                    // 先切状态再 cancel，Runner 随后收割到的 critical exit 才不会被误判成运行期故障。
                    self.supervisor.begin_stopping();
                    while self.supervisor.has_tasks() && !context.is_expired() {
                        match timeout(context.remaining(), self.supervisor.join_next()).await {
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
                        let drained = self.supervisor.abort_and_drain(context.remaining()).await;
                        if !drained {
                            failures.push(runner_error(
                                ApplicationPhase::Stopping,
                                "managed tasks did not finish before the global shutdown deadline",
                            ));
                        }
                    }
                    log_shutdown_step(
                        sequence,
                        "task-gate",
                        "supervisor",
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
                    match timeout(
                        context.remaining(),
                        self.application.resources().shutdown_business(context),
                    )
                    .await
                    {
                        Ok(mut resource_failures) => failures.append(&mut resource_failures),
                        Err(_) => failures.push(runner_error(
                            ApplicationPhase::Stopping,
                            "business resource shutdown exceeded the global deadline",
                        )),
                    }
                    log_shutdown_step(
                        sequence,
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
                    match timeout(
                        context.remaining(),
                        self.application
                            .resources()
                            .shutdown_component(component, context),
                    )
                    .await
                    {
                        Ok(mut resource_failures) => failures.append(&mut resource_failures),
                        Err(_) => failures.push(ApplicationError::new(
                            component,
                            ApplicationPhase::Stopping,
                            "component resource shutdown exceeded the global deadline",
                        )),
                    }
                    log_shutdown_step(
                        sequence,
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
                    match timeout(
                        context.remaining(),
                        self.application
                            .resources()
                            .shutdown_initializer(initializer.clone(), context),
                    )
                    .await
                    {
                        Ok(mut resource_failures) => failures.append(&mut resource_failures),
                        Err(_) => failures.push(ApplicationError::new(
                            ComponentId::Application,
                            ApplicationPhase::Stopping,
                            format!(
                                "initializer `{initializer}` resource shutdown exceeded the global deadline"
                            ),
                        )),
                    }
                    log_shutdown_step(
                        sequence,
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
        let deadline_exhausted = abandoned_steps > 0
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
            failures: failures.len(),
            task_abort_attempted,
            deadline_exhausted,
            duration: shutdown_started.elapsed(),
        });
        failures
    }
}

/// 业务作用：把一个已尝试的 active step 记录为带严格序号的结构化 debug 事件，供停机顺序复盘。
///
/// 参数说明：
/// - `sequence`：本次反向清理中从 1 开始、严格递增的执行序号。
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
        outcome = if failures_added == 0 { "completed" } else { "failed" },
        "application shutdown step completed"
    );
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
    let future = AssertUnwindSafe(future).catch_unwind();
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
                    Err(_) => {
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

/// 业务作用：在启动组件 future、绝对 deadline 和信号控制面之间进行选择。
///
/// # 参数
///
/// - `future`：当前组件阶段的受监督异步动作。
/// - `deadline`：整个启动流程共享的绝对截止时间。
/// - `phase`：超时时写入错误上下文的生命周期阶段。
/// - `broker`：启动期间持续轮询的信号控制面。
async fn await_startup_future<F>(
    future: F,
    deadline: Instant,
    phase: ApplicationPhase,
    broker: &mut SignalBroker,
) -> Result<(), StartupStop>
where
    F: Future<Output = ApplicationResult<()>>,
{
    let remaining = startup_remaining(deadline, phase)?;
    tokio::select! {
        result = future => result.map_err(StartupStop::Failure),
        signal = broker.next() => Err(signal_to_startup_stop(signal)),
        _ = tokio::time::sleep(remaining) => {
            Err(StartupStop::Failure(startup_timeout_error(phase)))
        }
    }
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
/// # 参数
///
/// - `completion`：监督器收割到的后台任务身份与退出结果。
fn report_background_completion(completion: TaskCompletion) {
    match completion.outcome {
        TaskOutcome::Completed | TaskOutcome::Cancelled => {}
        TaskOutcome::Failed(error) => {
            let summary = crate::report::redact(&error.to_string());
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
