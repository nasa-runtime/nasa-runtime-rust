use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use crate::{
    Application, ApplicationFuture, ApplicationResult, ComponentId, ManagedResource,
    ShutdownContext,
};

/// 一个已成功产生副作用的可逆步骤。实现者的 `Drop` 必须非阻塞。
pub trait ShutdownAction: Send {
    /// 返回用于清理报告的稳定 action 名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；名称不得包含配置值或业务秘密。
    fn label(&self) -> &'static str;

    /// 在共享全局 deadline 内撤销该 action 已成功产生的副作用。
    ///
    /// # 参数
    ///
    /// - `context`：携带首次停机原因和剩余预算的统一清理上下文。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a>;
}

/// 内置组件的三阶段生命周期协议；boxed future 保持 trait object-safe。
pub trait ApplicationComponent: Send {
    /// 返回组件的稳定身份。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Runner 使用结果校验唯一性和依赖顺序。
    fn id(&self) -> ComponentId;

    /// 返回必须在当前组件之前声明的静态依赖。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Runner 只校验而不会自动重排组件。
    fn dependencies(&self) -> &'static [ComponentId] {
        &[]
    }

    /// 执行只依赖本地引导配置的早期初始化。
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

    /// 使用最终初始配置创建组件运行资源。
    ///
    /// # 参数
    ///
    /// - `_context`：提供 Application、组件资源登记和 action 激活能力的阶段上下文。
    fn start<'a>(&'a mut self, _context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    /// 在 UserHook 成功后激活对外服务或调度终端。
    ///
    /// # 参数
    ///
    /// - `_context`：提供 Application、组件资源登记和 action 激活能力的阶段上下文。
    fn ready<'a>(&'a mut self, _context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    /// 取出 Ready 成功后必须由 Runner 监督的关键任务。
    ///
    /// 每个组件至多返回一次；任务在 active action 已压栈后加入监督集合，提前退出会触发失败停机。
    ///
    /// # 参数
    ///
    /// 本方法无参数；默认组件没有需要持续运行的终端任务。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        None
    }
}

/// active stack 中可以严格反向执行的一类已激活步骤。
pub(crate) enum ActiveStep {
    Action {
        component: ComponentId,
        action: Box<dyn ShutdownAction>,
    },
    ComponentResources(ComponentId),
    BusinessResources,
    UserTasks,
}

/// 记录成功副作用及动态任务、资源步骤的唯一逆序清理栈。
pub(crate) struct ActiveStack {
    steps: Vec<ActiveStep>,
    component_resources: HashSet<ComponentId>,
}

impl ActiveStack {
    /// 创建尚未激活任何副作用的空栈。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Runner 在每次应用生命周期中只创建一个实例。
    pub(crate) fn new() -> Self {
        Self {
            steps: Vec::new(),
            component_resources: HashSet::new(),
        }
    }

    /// 在 poll UserHook 前压入动态业务资源清理步骤。
    ///
    /// # 参数
    ///
    /// 本方法无参数；提前压栈保证部分登记也能回滚。
    pub(crate) fn push_business_resources(&mut self) {
        self.steps.push(ActiveStep::BusinessResources);
    }

    /// 在 poll UserHook 前压入动态用户任务清理步骤。
    ///
    /// # 参数
    ///
    /// 本方法无参数；该步骤位于业务资源之上，因此停机先结束任务。
    pub(crate) fn push_user_tasks(&mut self) {
        self.steps.push(ActiveStep::UserTasks);
    }

    /// 弹出最后成功激活的步骤。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回顺序天然实现副作用的严格反向撤销。
    pub(crate) fn pop(&mut self) -> Option<ActiveStep> {
        self.steps.pop()
    }

    /// 把组件已经成功形成的可逆副作用压栈。
    ///
    /// # 参数
    ///
    /// - `component`：产生该副作用的组件身份。
    /// - `action`：拥有清理所需句柄的 object-safe action。
    fn activate(&mut self, component: ComponentId, action: Box<dyn ShutdownAction>) {
        self.steps.push(ActiveStep::Action { component, action });
    }

    /// 为组件首次资源登记建立唯一清理步骤。
    ///
    /// # 参数
    ///
    /// - `component`：拥有随后登记资源的组件身份。
    fn ensure_component_resources(&mut self, component: ComponentId) {
        if self.component_resources.insert(component) {
            self.steps.push(ActiveStep::ComponentResources(component));
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
            /// 创建只在当前组件阶段 future 内有效的上下文。
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

            /// 返回当前阶段共享的 Application。
            ///
            /// # 参数
            ///
            /// 本方法无参数；返回借用不会越过阶段上下文。
            pub fn application(&self) -> &Application {
                self.application
            }

            /// 返回完整启动流程共享的绝对截止时刻。
            ///
            /// # 参数
            ///
            /// 本方法无参数；组件只能派生更短子预算，不能延长该时刻。
            pub fn deadline(&self) -> Instant {
                self.deadline
            }

            /// 返回共享启动截止时刻的当前剩余预算。
            ///
            /// # 参数
            ///
            /// 本方法无参数；截止时刻已经到达时返回零时长。
            pub fn remaining(&self) -> Duration {
                self.deadline.saturating_duration_since(Instant::now())
            }

            /// 在对应副作用完整成功后把 action 压入清理栈。
            ///
            /// # 参数
            ///
            /// - `action`：拥有撤销该副作用所需句柄的清理动作。
            pub fn activate(&mut self, action: Box<dyn ShutdownAction>) {
                self.active.activate(self.component, action);
            }

            /// 登记一个由当前组件拥有的普通资源。
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

            /// 登记一个由当前组件拥有并需要显式异步 shutdown 的资源。
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
lifecycle_context!(ReadyContext);
