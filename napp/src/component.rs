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
    /// 尚未释放的唯一对象，取出后重复清理不会再次析构同一值。
    value: Option<T>,
    /// 释放异常所属组件，避免析构期间再次调用业务元数据。
    component: ComponentId,
    /// 稳定的释放阶段摘要，用于停机错误归因。
    operation: &'static str,
}

impl<T> ShutdownActionCleanup<T> {
    /// 业务作用：接管 action 或 future，使尚未执行、外层取消和正常结束共用析构隔离边界。
    ///
    /// 参数说明：
    /// - `value`：由守卫独占释放责任的动作或 future，其析构可能发生在取消和停机期间。
    /// - `component`：析构异常应归属的组件身份，不在释放时再次调用组件元数据方法。
    /// - `operation`：框架固定的释放阶段摘要，不含业务值或异常正文。
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
/// 参数说明：
/// - `component`：启动时冻结的归属身份，错误处理不再依赖可能抛出异常的业务回调。
/// - `operation`：发生展开的固定操作名称，用于区分动作、future 等释放位置。
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
    /// 业务作用：提供在本次生命周期中保持不变的组件身份。
    ///
    /// 参数说明：无。
    ///
    /// 返回：Runner 在任何组件阶段开始前读取一次并冻结，用于唯一性、上下文和错误归因；展开时启动失败。
    /// 本方法只提供元数据，不得建立需要异步撤销的副作用，必须有限时间返回。
    fn id(&self) -> ComponentId;

    /// 业务作用：声明必须在当前组件之前出现的静态依赖，阻止未满足依赖的阶段执行。
    ///
    /// 参数说明：无。
    ///
    /// 返回：Runner 在任何组件阶段开始前读取一次并冻结，只校验声明顺序，不自动重排；展开时启动失败。
    /// 本方法只提供元数据，不得建立需要异步撤销的副作用，必须有限时间返回。
    fn dependencies(&self) -> &'static [ComponentId] {
        &[]
    }

    /// 业务作用：执行只依赖本地引导配置的早期初始化。
    ///
    /// 参数说明：
    ///
    /// - `_context`：提供 Application、组件资源登记和 action 激活能力的阶段上下文。
    ///
    /// 返回：默认立即成功且无副作用；实现方失败时返回阶段错误，Runner 停止启动并清理此前已登记步骤。
    /// 已建立的可逆副作用必须在失败或取消之前交给上下文托管。
    fn bootstrap<'a>(
        &'a mut self,
        _context: &'a mut BootstrapContext<'_>,
    ) -> ApplicationFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    /// 业务作用：使用最终初始配置创建组件运行资源。
    ///
    /// 参数说明：
    ///
    /// - `_context`：提供 Application、组件资源登记和 action 激活能力的阶段上下文。
    ///
    /// 返回：默认立即成功，不创建资源；实现方返回错误或被启动期限取消时，由 Runner 反向清理已托管步骤。
    /// 成功仅表示本阶段完成，尚未允许业务接流。
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
    ///
    /// 参数说明：
    ///
    /// - `_context`：提供 Application、组件资源登记和 action 激活能力的阶段上下文。
    ///
    /// 返回：装配成功时等待全表复验；失败由 Runner 反向清理，暂存终端任务尚未执行。
    fn ready<'a>(&'a mut self, _context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    /// 业务作用：在所有 Ready 装配及 initializer 任务工厂构造后，核对启动终端所需的最终静态条件。
    ///
    /// 参数说明：无。
    ///
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
    ///
    /// 返回：尚未执行的终端任务及固定名称；默认或已经移交时为 None。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        None
    }
}

/// active stack 中可以严格反向执行的一类已激活步骤。
pub(crate) enum ActiveStep {
    /// 组件已产生副作用的清理动作，按其激活位置反向执行。
    Action {
        /// 副作用的稳定组件身份。
        component: ComponentId,
        /// 对动作及其析构承担唯一清理责任的守卫。
        action: ShutdownActionCleanup<Box<dyn ShutdownAction>>,
    },
    /// initializer 已产生副作用的清理动作。
    InitializerAction {
        /// 初始化计划中冻结的 initializer 名称。
        initializer: std::sync::Arc<str>,
        /// 尚未执行或放弃时仍需安全释放的动作所有权。
        action: ShutdownActionCleanup<Box<dyn ShutdownAction>>,
    },
    /// 字段 `0` 标识本步应释放的组件资源集合。
    ComponentResources(ComponentId),
    /// 字段 `0` 标识本步应释放的 initializer 资源集合。
    InitializerResources(std::sync::Arc<str>),
    /// 清理由 UserHook 登记的业务资源。
    BusinessResources,
    /// 执行一次性业务停机任务，其依赖的业务资源此时仍须存在。
    BusinessShutdownTasks,
    /// 等待受管 initializer 任务结束，再释放其资源。
    InitializerTasks,
    /// 等待 UserHook 任务结束，再处理业务停机任务和资源。
    UserTasks,
}

/// 记录成功副作用及动态任务、资源步骤的唯一逆序清理栈。
pub(crate) struct ActiveStack {
    /// 已登记步骤的清理所有权，尾部是下一项待执行步骤。
    steps: Vec<ActiveStep>,
    /// 本生命周期已压入资源清理步骤的组件，弹栈后仍保留标记，避免重复压入该类步骤。
    component_resources: HashSet<ComponentId>,
    /// 本生命周期已建立资源清理步骤的 initializer 身份。
    initializer_resources: HashSet<std::sync::Arc<str>>,
}

impl ActiveStack {
    /// 业务作用：创建尚未激活任何副作用的空栈。
    ///
    /// 参数说明：无。
    ///
    /// 返回：没有清理步骤和资源登记标记的新栈，供一次应用生命周期独占使用，不执行任何组件动作。
    pub(crate) fn new() -> Self {
        Self {
            steps: Vec::new(),
            component_resources: HashSet::new(),
            initializer_resources: HashSet::new(),
        }
    }

    /// 业务作用：在 poll UserHook 前压入动态业务资源清理步骤。
    ///
    /// 参数说明：无。
    ///
    /// 返回：追加业务资源清理责任，使 UserHook 部分登记后失败也有释放入口；不立即关闭资源，不去重重复调用。
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
    /// 参数说明：无。
    ///
    /// 返回：追加用户任务清理责任，不停止任务或检查它们是否存在；Runner 在业务资源步骤之后调用，保证反向清理先处理任务。
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
    /// 参数说明：无。
    ///
    /// 返回：栈非空时移交最后一项的清理所有权，否则为 None；不执行撤销，资源首次登记标记也不会随弹出删除。
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
    /// 参数说明：
    ///
    /// - `component`：拥有随后登记资源的组件身份。
    ///
    /// 返回：该组件首次登记时压入资源清理步骤，后续调用保持栈不变；不代表任何资源已经登记成功。
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
            /// 当前应用的共享配置与资源入口，生命周期由 Runner 持有。
            application: &'a Application,
            /// 本阶段冻结的组件身份，用于资源归属和清理记录。
            component: ComponentId,
            /// 当前阶段独占借用的逆序清理栈，登记后由 Runner 负责执行。
            active: &'a mut ActiveStack,
            /// 启动流程共用的固定截止点，阶段切换不重新起算。
            deadline: Instant,
        }

        impl<'a> $name<'a> {
            /// 业务作用：创建只在当前组件阶段 future 内有效的上下文。
            ///
            /// 参数说明：
            ///
            /// - `application`：组件可以读取配置和资源的共享应用上下文。
            /// - `component`：当前执行阶段所属组件的稳定身份。
            /// - `active`：接收成功副作用和组件资源步骤的唯一清理栈。
            /// - `deadline`：Runner 为完整启动流程创建的共享绝对截止时刻。
            ///
            /// 返回：绑定组件身份、借用应用及清理栈的阶段上下文，不延长启动截止点，也不自行创建资源。
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
            /// 参数说明：无。
            /// 返回：绑定当前生命周期的应用借用，供读取配置与资源；不发布 Ready 或扩大资源登记窗口。
            pub fn application(&self) -> &Application {
                self.application
            }

            /// 业务作用：返回完整启动流程共享的绝对截止时刻。
            /// 参数说明：无。
            /// 返回：创建上下文时固定的单调时钟截止点；派生子预算只能收紧，不随组件切换重新起算。
            pub fn deadline(&self) -> Instant {
                self.deadline
            }

            /// 业务作用：返回共享启动截止时刻的当前剩余预算。
            /// 参数说明：无。
            /// 返回：当前单调时间到固定截止点的非负时长，已过期时为零；只提供瞬时预算，不保留或延长执行时间。
            pub fn remaining(&self) -> Duration {
                self.deadline.saturating_duration_since(Instant::now())
            }

            /// 业务作用：在对应副作用完整成功后把 action 压入清理栈。
            ///
            /// 参数说明：
            ///
            /// - `action`：拥有撤销该副作用所需句柄的清理动作。
            ///
            /// 返回：立即把动作与当前组件身份压入清理栈，交接其析构责任；不执行动作，也不验证外部副作用是否完整。
            pub fn activate(&mut self, action: Box<dyn ShutdownAction>) {
                self.active.activate(self.component, action);
            }

            /// 业务作用：登记一个由当前组件拥有的普通资源。
            ///
            /// 参数说明：
            ///
            /// - `qualifier`：同类型多实例使用的可选非空名称。
            /// - `value`：所有权交给组件资源容器的线程安全值。
            ///
            /// 返回：资源表开放且类型与名称键未占用时登记成功；名称非法、冲突或窗口关闭时返回错误并释放传入值。
            /// 先登记组件清理步骤再尝试发布，失败时仍可能保留空的清理步骤；本入口不登记异步 shutdown。
            pub fn register_resource<T>(
                &mut self,
                qualifier: Option<&str>,
                value: T,
            ) -> ApplicationResult<()>
            where
                T: Send + Sync + 'static,
            {
                // 先建立释放入口，资源部分登记后发生启动失败仍有统一清理责任。
                self.active.ensure_component_resources(self.component);
                self.application
                    .resources()
                    .register_component(self.component, qualifier, value)
            }

            /// 业务作用：在单次资源表写锁内原子登记一组异类型组件资源。
            ///
            /// 参数说明：
            /// - `build`：同步构造尚未发布的异类型资源批次；应只暂存资源，不执行不可撤销的外部副作用。
            ///
            /// 返回：构造成功且资源表开放、全部 key 唯一时整体发布，随后确保组件清理步骤存在；构造或预检失败不发布任何条目。
            /// 不能撤销构造闭包自行执行的外部副作用，闭包不得阻塞阶段执行。
            #[allow(dead_code)]
            pub(crate) fn register_resource_batch(
                &mut self,
                build: impl FnOnce(
                    &mut crate::resources::StagedResourceBatch,
                ) -> ApplicationResult<()>,
            ) -> ApplicationResult<()> {
                let mut batch = crate::resources::StagedResourceBatch::new();
                build(&mut batch)?;
                // 全部键预检在资源表内部完成，整批发布后再登记清理步骤，不留下半张可见资源表。
                self.application
                    .resources()
                    .register_component_batch(self.component, batch)?;
                self.active.ensure_component_resources(self.component);
                Ok(())
            }

            /// 业务作用：登记一个由当前组件拥有并需要显式异步 shutdown 的资源。
            ///
            /// 参数说明：
            ///
            /// - `qualifier`：同类型多实例使用的可选非空名称。
            /// - `value`：实现 ManagedResource 且 Drop 非阻塞的资源值。
            ///
            /// 返回：成功时资源及其异步关闭函数一同登记到当前组件名下；名称非法、冲突或资源表已封口时失败，传入值被释放。
            /// 清理步骤先于发布登记，不因本次失败撤销已有步骤；后续停机按宿主共享预算执行异步关闭。
            pub fn register_managed_resource<T>(
                &mut self,
                qualifier: Option<&str>,
                value: T,
            ) -> ApplicationResult<()>
            where
                T: ManagedResource,
            {
                // 异步资源关闭依赖清理栈；即使资源登记失败，也保留本组件先前已登记资源的清理入口。
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

#[cfg(feature = "hystrix")]
impl PrepareContext<'_> {
    /// 业务作用：让隔离命令在业务停机任务结束后才关闭，同时早于基础组件释放。
    ///
    /// 参数说明：
    /// - `action`：持有当前应用隔离命令目录的清理所有权，业务停机任务结束前必须保持命令可用。
    ///
    /// 返回：业务资源步骤存在时紧随其后插入，否则追加到栈顶；反向清理会先完成业务停机任务，再撤销命令目录。
    /// 只调整已完成副作用的清理位置，不执行动作或改变共享停机期限。
    pub(crate) fn activate_after_business(&mut self, action: Box<dyn ShutdownAction>) {
        let step = ActiveStep::Action {
            component: self.component,
            action: ShutdownActionCleanup::new(
                action,
                self.component,
                "releasing business dependency",
            ),
        };
        // Service 的业务停机任务需要继续调用 Command，不能由后准备的 owner 提前撤销准入。
        if let Some(index) = self
            .active
            .steps
            .iter()
            .position(|step| matches!(step, ActiveStep::BusinessResources))
        {
            self.active.steps.insert(index + 1, step);
        } else {
            self.active.steps.push(step);
        }
    }
}
