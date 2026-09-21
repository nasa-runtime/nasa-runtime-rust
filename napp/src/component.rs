use std::{
    collections::HashSet,
    panic::{catch_unwind, AssertUnwindSafe},
    time::{Duration, Instant},
};

use crate::{
    Application, ApplicationError, ApplicationFuture, ApplicationPhase, ApplicationResult,
    ComponentId, ManagedResource, ShutdownContext,
};

/// 一个已成功产生副作用的可逆步骤。同步回调和 `Drop` 必须有限时间返回。
/// Runner 分别隔离 label、future 创建、poll、future 释放和 action 释放的单次展开式 panic；
/// 同步阻塞、`panic=abort` 或同一次展开中的再次 panic 无法安全抢占。
pub trait ShutdownAction: Send {
    /// 业务作用：返回用于清理报告的稳定 action 名称。
    ///
    /// 参数说明：无。
    ///
    /// 返回：不包含配置值或业务秘密的静态名称；展开时 Runner 跳过该 action 的 shutdown 并释放所有权。
    fn label(&self) -> &'static str;

    /// 业务作用：在共享全局 deadline 内撤销该 action 已成功产生的副作用。
    ///
    /// 参数说明：
    ///
    /// - `context`：携带首次停机原因和剩余预算的统一清理上下文。
    ///
    /// 返回：成功表示副作用已撤销；错误、超时或单次展开记为次要失败，不覆盖首次终止原因。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a>;
}

/// 从激活入栈到清理结束持续持有 action 或其 future 的一次性受控释放权。
pub(crate) struct ShutdownActionCleanup<T> {
    value: Option<T>,
    component: ComponentId,
    operation: &'static str,
}

impl<T> ShutdownActionCleanup<T> {
    /// 业务作用：接管 action 或 future，使尚未执行、外层取消和正常结束共用析构隔离边界。
    ///
    /// 参数说明：`value` 为被接管对象，`component` 为稳定组件归因，`operation` 为框架固定释放阶段。
    ///
    /// 返回：持有唯一释放权的守卫，不调用业务方法。
    pub(crate) fn new(value: T, component: ComponentId, operation: &'static str) -> Self {
        Self {
            value: Some(value),
            component,
            operation,
        }
    }

    /// 业务作用：向隔离执行器借出仍由守卫拥有的对象，不移交析构责任。
    ///
    /// 参数说明：无。
    ///
    /// 返回：显式 release 之前的独占借用；已释放后调用属于内部状态错误。
    pub(crate) fn value_mut(&mut self) -> &mut T {
        self.value
            .as_mut()
            .expect("shutdown action ownership is retained")
    }

    /// 业务作用：一次性释放所有权，并把单次析构展开转成可累计的次要失败。
    ///
    /// 参数说明：无。
    ///
    /// 返回：正常或重复释放返回 None；异常返回固定阶段错误，不读取 panic payload。
    pub(crate) fn release(&mut self) -> Option<ApplicationError> {
        // 先撤销守卫的所有权，析构展开后 Drop 也不能再次释放同一对象。
        let value = self.value.take();
        match catch_unwind(AssertUnwindSafe(|| drop(value))) {
            Ok(()) => None,
            Err(payload) => {
                crate::shutdown::release_shutdown_panic_payload(payload);
                Some(action_panic_error(self.component, self.operation))
            }
        }
    }
}

impl<T> Drop for ShutdownActionCleanup<T> {
    /// 业务作用：在执行器被取消或激活栈被放弃时隔离析构，避免异常截断其它所有权释放。
    ///
    /// 参数说明：无。
    ///
    /// 返回：无返回值；无法纳入退出报告的异常同步告警，不追改已经发布的摘要。
    fn drop(&mut self) {
        if let Some(error) = self.release() {
            crate::report::report_shutdown(&error);
        }
    }
}

/// 业务作用：为 action 展开生成稳定归因，不调用业务 label 或访问异常正文。
///
/// 参数说明：`component` 是所属组件，`operation` 是框架固定的失败阶段。
///
/// 返回：Stopping 阶段的次要错误，不替换首次终止原因。
pub(crate) fn action_panic_error(
    component: ComponentId,
    operation: &'static str,
) -> ApplicationError {
    ApplicationError::new(
        component,
        ApplicationPhase::Stopping,
        format!("shutdown action panicked while {operation}"),
    )
}

/// 内置组件的受管生命周期协议；boxed future 保持 trait object-safe。
pub trait ApplicationComponent: Send {
    /// 业务作用：返回组件的稳定身份。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Runner 使用结果校验唯一性和依赖顺序。
    fn id(&self) -> ComponentId;

    /// 业务作用：返回必须在当前组件之前声明的静态依赖。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Runner 只校验而不会自动重排组件。
    fn dependencies(&self) -> &'static [ComponentId] {
        &[]
    }

    /// 业务作用：执行只依赖本地引导配置的早期初始化。
    ///
    /// # 参数
    ///
    /// - `_context`：提供 Application、组件资源登记和 action 激活能力的阶段上下文。
    fn bootstrap<'a>(
        &'a mut self,
        _context: &'a mut BootstrapContext<'_>,
    ) -> ApplicationFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    /// 业务作用：使用最终初始配置创建组件运行资源。
    ///
    /// # 参数
    ///
    /// - `_context`：提供 Application、组件资源登记和 action 激活能力的阶段上下文。
    fn start<'a>(&'a mut self, _context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    /// 业务作用：在业务 initializer 之前完成 migration 和出站依赖门禁。
    ///
    /// 参数说明：
    /// - `_context`：只提供 Application、组件资源登记和 action 激活能力的阶段上下文。
    ///
    /// 返回：出站依赖已可供 initializer 安全使用时成功；失败时禁止初始化和接流。
    fn prepare<'a>(&'a mut self, _context: &'a mut PrepareContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    /// 业务作用：在 Prepare、业务初始化与 Seal 全部成功后装配对外服务或调度终端。
    /// 参数说明：
    ///
    /// - `_context`：提供 Application、组件资源登记和 action 激活能力的阶段上下文。
    ///
    /// 返回：装配成功时等待全表复验；失败由 Runner 反向清理，暂存终端任务尚未执行。
    fn ready<'a>(&'a mut self, _context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    /// 业务作用：在所有 Ready 装配及 initializer 任务工厂构造后，核对启动终端所需的最终静态条件。
    /// 参数说明：无。
    /// 返回：允许激活时成功；失败使所有暂存终端任务未经 poll 即释放，并反向清理已登记 action。
    /// 本入口必须是短时、非阻塞的只读检查，不新增资源、静态登记或外部副作用；Batch 仅检查观测组件。
    /// 同步执行不能被 timeout 抢占；Runner 在返回后复验截止时刻，panic 按 Ready 失败执行统一清理。
    fn validate_ready(&self) -> ApplicationResult<()> {
        Ok(())
    }

    /// 业务作用：取出 Ready 装配成功后需要由 Runner 暂存并监督的关键任务。
    ///
    /// 每个组件至多返回一次；先由监督器接管所有权，全部工厂与最终检查成功并发布 Ready 后才执行主体。
    /// Batch 仅放行观测任务而不发布 Service Ready；提前退出仍触发失败停机。
    ///
    /// 参数说明：无。
    /// 返回：尚未执行的终端任务及固定名称；默认或已经移交时为 None。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        None
    }
}

/// active stack 中可以严格反向执行的一类已激活步骤。
pub(crate) enum ActiveStep {
    Action {
        component: ComponentId,
        action: ShutdownActionCleanup<Box<dyn ShutdownAction>>,
    },
    InitializerAction {
        initializer: std::sync::Arc<str>,
        action: ShutdownActionCleanup<Box<dyn ShutdownAction>>,
    },
    ComponentResources(ComponentId),
    InitializerResources(std::sync::Arc<str>),
    BusinessResources,
    BusinessShutdownTasks,
    InitializerTasks,
    UserTasks,
}

/// 记录成功副作用及动态任务、资源步骤的唯一逆序清理栈。
pub(crate) struct ActiveStack {
    steps: Vec<ActiveStep>,
    component_resources: HashSet<ComponentId>,
    initializer_resources: HashSet<std::sync::Arc<str>>,
}

impl ActiveStack {
    /// 业务作用：创建尚未激活任何副作用的空栈。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Runner 在每次应用生命周期中只创建一个实例。
    pub(crate) fn new() -> Self {
        Self {
            steps: Vec::new(),
            component_resources: HashSet::new(),
            initializer_resources: HashSet::new(),
        }
    }

    /// 业务作用：在 poll UserHook 前压入动态业务资源清理步骤。
    ///
    /// # 参数
    ///
    /// 本方法无参数；提前压栈保证部分登记也能回滚。
    pub(crate) fn push_business_resources(&mut self) {
        self.steps.push(ActiveStep::BusinessResources);
    }

    /// 业务作用：在 UserHook 前压入业务停机任务清理步骤，保证部分登记也能进入统一收口链。
    ///
    /// 参数说明：无。
    ///
    /// 返回：无返回值；该步骤位于 UserTasks 之下、BusinessResources 之上，停机时先收割受管任务，
    /// 再执行一次性业务停机任务，最后释放业务资源。
    pub(crate) fn push_business_shutdown_tasks(&mut self) {
        self.steps.push(ActiveStep::BusinessShutdownTasks);
    }

    /// 业务作用：在 poll UserHook 前压入动态用户任务清理步骤。
    ///
    /// # 参数
    ///
    /// 本方法无参数；该步骤位于业务资源之上，因此停机先结束任务。
    pub(crate) fn push_user_tasks(&mut self) {
        self.steps.push(ActiveStep::UserTasks);
    }

    /// 业务作用：在 Ready action 之前压入受管任务清理门。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无返回值；该步骤位于 initializer action/资源之上、Ready action 之下，
    /// 因此停机先关入口，再停任务，最后释放 initializer 所有权。
    pub(crate) fn push_initializer_tasks(&mut self) {
        self.steps.push(ActiveStep::InitializerTasks);
    }

    /// 业务作用：弹出最后成功激活的步骤。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回顺序天然实现副作用的严格反向撤销。
    pub(crate) fn pop(&mut self) -> Option<ActiveStep> {
        self.steps.pop()
    }

    /// 业务作用：返回尚待逆序处置的 active step 数量，供停机摘要区分已尝试与被 deadline 放弃的步骤。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前栈内仍持有所有权的步骤数量，不改变清理顺序。
    pub(crate) fn len(&self) -> usize {
        self.steps.len()
    }

    /// 业务作用：把组件已经成功形成的可逆副作用压栈。
    ///
    /// 参数说明：
    ///
    /// - `component`：产生该副作用的组件身份。
    /// - `action`：拥有清理所需句柄的 object-safe action。
    ///
    /// 返回：无返回值；从入栈起隔离 action 析构，覆盖尚未执行就被放弃的路径。
    fn activate(&mut self, component: ComponentId, action: Box<dyn ShutdownAction>) {
        self.steps.push(ActiveStep::Action {
            component,
            action: ShutdownActionCleanup::new(action, component, "releasing its action"),
        });
    }

    /// 业务作用：为组件首次资源登记建立唯一清理步骤。
    ///
    /// # 参数
    ///
    /// - `component`：拥有随后登记资源的组件身份。
    fn ensure_component_resources(&mut self, component: ComponentId) {
        if self.component_resources.insert(component) {
            self.steps.push(ActiveStep::ComponentResources(component));
        }
    }

    /// 业务作用：把 initializer 已完整生效的可逆副作用立即压入统一清理栈。
    ///
    /// 参数说明：
    /// - `initializer`：产生副作用的 canonical initializer 身份。
    /// - `action`：持有撤销所需句柄的清理动作。
    ///
    /// 返回：无返回值；后续失败会严格逆序撤销。
    pub(crate) fn activate_initializer(
        &mut self,
        initializer: std::sync::Arc<str>,
        action: Box<dyn ShutdownAction>,
    ) {
        self.steps.push(ActiveStep::InitializerAction {
            initializer,
            action: ShutdownActionCleanup::new(
                action,
                ComponentId::Application,
                "releasing its action",
            ),
        });
    }

    /// 业务作用：为 initializer 首次资源登记建立唯一清理步骤。
    ///
    /// 参数说明：
    /// - `initializer`：拥有随后登记资源的 canonical 身份。
    ///
    /// 返回：无返回值；同一 initializer 仅压栈一次。
    pub(crate) fn ensure_initializer_resources(&mut self, initializer: std::sync::Arc<str>) {
        if self.initializer_resources.insert(initializer.clone()) {
            self.steps
                .push(ActiveStep::InitializerResources(initializer));
        }
    }
}

macro_rules! lifecycle_context {
    ($name:ident) => {
        #[doc = concat!(stringify!($name), " 为单个组件阶段提供受控 Application、资源登记和 action 激活能力。")]
        pub struct $name<'a> {
            application: &'a Application,
            component: ComponentId,
            active: &'a mut ActiveStack,
            deadline: Instant,
        }

        impl<'a> $name<'a> {
            /// 业务作用：创建只在当前组件阶段 future 内有效的上下文。
            ///
            /// # 参数
            ///
            /// - `application`：组件可以读取配置和资源的共享应用上下文。
            /// - `component`：当前执行阶段所属组件的稳定身份。
            /// - `active`：接收成功副作用和组件资源步骤的唯一清理栈。
            /// - `deadline`：Runner 为完整启动流程创建的共享绝对截止时刻。
            pub(crate) fn new(
                application: &'a Application,
                component: ComponentId,
                active: &'a mut ActiveStack,
                deadline: Instant,
            ) -> Self {
                Self {
                    application,
                    component,
                    active,
                    deadline,
                }
            }

            /// 业务作用：返回当前阶段共享的 Application。
            ///
            /// # 参数
            ///
            /// 本方法无参数；返回借用不会越过阶段上下文。
            pub fn application(&self) -> &Application {
                self.application
            }

            /// 业务作用：返回完整启动流程共享的绝对截止时刻。
            ///
            /// # 参数
            ///
            /// 本方法无参数；组件只能派生更短子预算，不能延长该时刻。
            pub fn deadline(&self) -> Instant {
                self.deadline
            }

            /// 业务作用：返回共享启动截止时刻的当前剩余预算。
            ///
            /// # 参数
            ///
            /// 本方法无参数；截止时刻已经到达时返回零时长。
            pub fn remaining(&self) -> Duration {
                self.deadline.saturating_duration_since(Instant::now())
            }

            /// 业务作用：在对应副作用完整成功后把 action 压入清理栈。
            ///
            /// # 参数
            ///
            /// - `action`：拥有撤销该副作用所需句柄的清理动作。
            pub fn activate(&mut self, action: Box<dyn ShutdownAction>) {
                self.active.activate(self.component, action);
            }

            /// 业务作用：登记一个由当前组件拥有的普通资源。
            ///
            /// # 参数
            ///
            /// - `qualifier`：同类型多实例使用的可选非空名称。
            /// - `value`：所有权交给组件资源容器的线程安全值。
            pub fn register_resource<T>(
                &mut self,
                qualifier: Option<&str>,
                value: T,
            ) -> ApplicationResult<()>
            where
                T: Send + Sync + 'static,
            {
                self.active.ensure_component_resources(self.component);
                self.application
                    .resources()
                    .register_component(self.component, qualifier, value)
            }

            /// 业务作用：在单次资源表写锁内原子登记一组异类型组件资源。
            ///
            /// 参数说明：`build` 只向本地暂存批次加入资源，不得执行外部副作用。
            ///
            /// 返回：构造与全部 key 预检成功后整体发布；冲突时资源表保持原样。
            #[allow(dead_code)]
            pub(crate) fn register_resource_batch(
                &mut self,
                build: impl FnOnce(
                    &mut crate::resources::StagedResourceBatch,
                ) -> ApplicationResult<()>,
            ) -> ApplicationResult<()> {
                let mut batch = crate::resources::StagedResourceBatch::new();
                build(&mut batch)?;
                self.application
                    .resources()
                    .register_component_batch(self.component, batch)?;
                self.active.ensure_component_resources(self.component);
                Ok(())
            }

            /// 业务作用：登记一个由当前组件拥有并需要显式异步 shutdown 的资源。
            ///
            /// # 参数
            ///
            /// - `qualifier`：同类型多实例使用的可选非空名称。
            /// - `value`：实现 ManagedResource 且 Drop 非阻塞的资源值。
            pub fn register_managed_resource<T>(
                &mut self,
                qualifier: Option<&str>,
                value: T,
            ) -> ApplicationResult<()>
            where
                T: ManagedResource,
            {
                self.active.ensure_component_resources(self.component);
                self.application.resources().register_component_managed(
                    self.component,
                    qualifier,
                    value,
                )
            }
        }
    };
}

lifecycle_context!(BootstrapContext);
lifecycle_context!(StartContext);
lifecycle_context!(PrepareContext);
lifecycle_context!(ReadyContext);
