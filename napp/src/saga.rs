//! Saga Application 生命周期组件。
//!
//! 业务在 UserHook 构造 definition、Orchestrator 与参与方运行时并提交计划；组件在 Ready 前完成
//! 本地步骤合同和历史非终态实例校验，成功后才发布只读能力并启动 durable timer。应用停机时先
//! 关闭能力入口，再由数据库等更早启动的依赖执行反向清理。

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use naoutbox_core::OutboxPublisher;
use nasaga_runtime::{DefinitionRegistry, Orchestrator, ParticipantRuntime};
use serde::Deserialize;

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, ReadyContext, ShutdownAction,
    ShutdownContext, StartContext,
};

const DEFAULT_TIMER_POLL_INTERVAL_MS: u64 = 500;
const DEFAULT_TIMER_ERROR_BACKOFF_MS: u64 = 1_000;
const DEFAULT_TIMER_OPERATION_TIMEOUT_MS: u64 = 5_000;
const MAX_TIMER_INTERVAL_MS: u64 = 60_000;
const MAX_TIMER_FAILURE_THRESHOLD: u32 = 100;

/// 业务作用：选择 Saga 默认数据源在 Application 生命周期中的建立时机。
///
/// 常规服务使用 `Application`，由 DB 组件在 Start 阶段按统一配置建池；需要先创建隔离库的工具型
/// 进程使用 `UserHook`，由业务启动钩子注入默认池后，DB 组件在 Ready 前接管探针、监督和关闭。
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SagaDatabaseBootstrap {
    /// DB 组件依据 `database` 或 `datasources` 配置建立默认池。
    #[default]
    Application,
    /// 业务启动钩子先通过事务运行时注入默认池，DB 组件随后接管生命周期。
    UserHook,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SagaSettings {
    database_bootstrap: SagaDatabaseBootstrap,
    timer_poll_interval_ms: u64,
    timer_error_backoff_ms: u64,
    timer_operation_timeout_ms: u64,
    timer_failure_threshold: u32,
}

impl Default for SagaSettings {
    /// 业务作用：提供有界、偏保守的 timer 轮询与故障摘流默认值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：500ms 正常轮询、1s 故障退避、5s 单轮上限、连续三次失败后摘流的设置。
    fn default() -> Self {
        Self {
            database_bootstrap: SagaDatabaseBootstrap::Application,
            timer_poll_interval_ms: DEFAULT_TIMER_POLL_INTERVAL_MS,
            timer_error_backoff_ms: DEFAULT_TIMER_ERROR_BACKOFF_MS,
            timer_operation_timeout_ms: DEFAULT_TIMER_OPERATION_TIMEOUT_MS,
            timer_failure_threshold: 3,
        }
    }
}

/// 业务作用：读取 Saga 数据库引导策略，供隐式 DB 组件选择正常建池或延后接管。
///
/// 参数说明：
/// - `application`：提供已经完成合并与校验的不可变配置快照。
/// - `phase`：读取失败应归属的生命周期阶段。
///
/// 返回：配置合法时返回显式或默认策略；结构错误返回 Saga 组件错误并阻止含混启动。
pub(crate) fn database_bootstrap(
    application: &Application,
    phase: ApplicationPhase,
) -> ApplicationResult<SagaDatabaseBootstrap> {
    Ok(read_saga_settings(application, phase)?.database_bootstrap)
}

struct OrchestratorPlan {
    runtime: Arc<Orchestrator>,
    timer_owner: String,
}

/// 业务作用：描述一个进程在 Saga 生命周期中托管的 Orchestrator 与参与方运行时。
///
/// 计划只能在 Application UserHook 内提交一次。Orchestrator 可选；参与方按稳定名称索引，允许一个
/// 服务同时承载多个独立参与方适配器，但不允许同名覆盖。
pub struct SagaApplicationPlan {
    orchestrator: Option<OrchestratorPlan>,
    participants: BTreeMap<String, Arc<ParticipantRuntime>>,
    outbox: Option<crate::outbox::OutboxApplicationPlan>,
}

impl SagaApplicationPlan {
    /// 业务作用：创建尚未包含任何运行角色的 Saga 计划。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：必须继续加入 Orchestrator 或至少一个参与方后才能提交的空计划。
    pub fn new() -> Self {
        Self {
            orchestrator: None,
            participants: BTreeMap::new(),
            outbox: None,
        }
    }

    /// 业务作用：创建只包含一个 Orchestrator 的受管计划。
    ///
    /// 参数说明：
    /// - `runtime`：已经冻结 definition 注册表和推进预算的 Orchestrator。
    /// - `timer_owner`：当前副本稳定且唯一的 timer 租约身份，重启后保持不变、不同副本不得共享。
    ///
    /// 返回：owner 满足 canonical 标识合同则返回计划，否则拒绝把含混身份用于 fencing。
    pub fn orchestrator(
        runtime: Arc<Orchestrator>,
        timer_owner: impl Into<String>,
    ) -> ApplicationResult<Self> {
        Self::new().with_orchestrator(runtime, timer_owner)
    }

    /// 业务作用：创建只包含一个命名参与方的受管计划。
    ///
    /// 参数说明：
    /// - `name`：应用内唯一、只用于能力查找和低基数诊断的 canonical 名称。
    /// - `runtime`：已冻结 Inbox consumer 与受信 command 投影的参与方运行时。
    ///
    /// 返回：名称合法时返回计划；空白、超长或非 canonical 名称返回 UserHook 错误。
    pub fn participant(
        name: impl Into<String>,
        runtime: Arc<ParticipantRuntime>,
    ) -> ApplicationResult<Self> {
        Self::new().with_participant(name, runtime)
    }

    /// 业务作用：为计划设置唯一 Orchestrator 及其逐副本 timer fencing 身份。
    ///
    /// 参数说明：
    /// - `runtime`：已经完成构造、尚未对外发布的 Orchestrator。
    /// - `timer_owner`：用于 durable timer claim 的逐副本稳定身份。
    ///
    /// 返回：首次设置且 owner 合法时返回更新后的计划；重复设置或身份非法时返回错误。
    pub fn with_orchestrator(
        mut self,
        runtime: Arc<Orchestrator>,
        timer_owner: impl Into<String>,
    ) -> ApplicationResult<Self> {
        if self.orchestrator.is_some() {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga plan declares more than one orchestrator",
            ));
        }
        let timer_owner = timer_owner.into();
        validate_runtime_name(&timer_owner, "timer owner")?;
        self.orchestrator = Some(OrchestratorPlan {
            runtime,
            timer_owner,
        });
        Ok(self)
    }

    /// 业务作用：向计划加入一个具名参与方，避免多个 adapter 在能力入口发生身份覆盖。
    ///
    /// 参数说明：
    /// - `name`：应用内唯一的 canonical 参与方名称。
    /// - `runtime`：已经冻结信任投影的参与方运行时。
    ///
    /// 返回：名称合法且未重复时返回更新后的计划；否则返回 UserHook 错误。
    pub fn with_participant(
        mut self,
        name: impl Into<String>,
        runtime: Arc<ParticipantRuntime>,
    ) -> ApplicationResult<Self> {
        let name = name.into();
        validate_runtime_name(&name, "participant name")?;
        if self.participants.insert(name, runtime).is_some() {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga plan contains a duplicate participant name",
            ));
        }
        Ok(self)
    }

    /// 业务作用：为 Saga 内部必需的 command/result Outbox 绑定唯一受管发布端。
    ///
    /// 业务只声明 `saga`，不再重复声明或配置独立 Outbox 组件；Application 会把本计划中的发布端
    /// 移交给隐式 Outbox 生命周期。发布端可使用 Kafka、Redis Streams、HTTP 或其它可靠 transport。
    ///
    /// 参数说明：
    /// - `publisher`：下游确认成功后才返回成功的线程安全发布端。
    ///
    /// 返回：首次绑定返回更新后的 Saga 计划；重复绑定返回 UserHook 配置错误。
    pub fn with_event_publisher<P>(mut self, publisher: Arc<P>) -> ApplicationResult<Self>
    where
        P: OutboxPublisher + Send + Sync + 'static,
    {
        if self.outbox.is_some() {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga event publisher can be configured only once",
            ));
        }
        self.outbox = Some(crate::outbox::OutboxApplicationPlan::new(publisher));
        Ok(self)
    }

    /// 业务作用：确认计划至少托管一个可对外提供的 Saga 角色。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：含 Orchestrator 或参与方时成功；空计划返回错误并阻止无行为组件进入 Ready。
    pub(crate) fn validate(&self) -> ApplicationResult<()> {
        if self.orchestrator.is_none() && self.participants.is_empty() {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga plan must contain an orchestrator or at least one participant",
            ));
        }
        Ok(())
    }

    /// 业务作用：把 Saga 组合声明内的发布计划移交给隐式 Outbox 组件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已绑定发布端时返回唯一计划；缺失时返回 UserHook 配置错误。
    pub(crate) fn take_outbox_plan(
        &mut self,
    ) -> ApplicationResult<crate::outbox::OutboxApplicationPlan> {
        self.outbox.take().ok_or_else(|| {
            saga_error(
                ApplicationPhase::UserHook,
                "saga requires an event publisher for its managed Outbox",
            )
        })
    }
}

impl Default for SagaApplicationPlan {
    /// 业务作用：提供便于按角色逐步装配的空 Saga 计划。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与 [`SagaApplicationPlan::new`] 相同的空计划。
    fn default() -> Self {
        Self::new()
    }
}

/// 业务作用：提供 Ready 后的 Saga 只读能力，不暴露停机、替换 definition 或重置 fencing 权限。
#[derive(Clone)]
pub struct SagaHandle {
    pub(crate) state: Arc<SagaRuntimeState>,
}

impl SagaHandle {
    /// 业务作用：取得已经通过 Ready 门禁的 Orchestrator，用于业务入口推进 Saga。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前计划含 Orchestrator 且组件仍 Ready 时返回共享句柄；角色缺失或停机后返回错误。
    pub fn orchestrator(&self) -> ApplicationResult<Arc<Orchestrator>> {
        self.state.ensure_ready()?;
        self.state.orchestrator.get().cloned().ok_or_else(|| {
            saga_error(
                ApplicationPhase::Running,
                "this application does not host a saga orchestrator",
            )
        })
    }

    /// 业务作用：按稳定名称取得已经通过 Ready 门禁的参与方运行时。
    ///
    /// 参数说明：
    /// - `name`：提交计划时使用的参与方名称。
    ///
    /// 返回：名称存在且组件仍 Ready 时返回共享句柄；未知名称或停机后返回错误。
    pub fn participant(&self, name: &str) -> ApplicationResult<Arc<ParticipantRuntime>> {
        self.state.ensure_ready()?;
        self.state
            .participants
            .get()
            .and_then(|participants| participants.get(name))
            .cloned()
            .ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Running,
                    "requested saga participant is not hosted by this application",
                )
            })
    }
}

pub(crate) struct SagaRuntimeState {
    pending: Mutex<Option<SagaApplicationPlan>>,
    sealed: AtomicBool,
    lifecycle: AtomicU8,
    orchestrator: OnceLock<Arc<Orchestrator>>,
    participants: OnceLock<Arc<BTreeMap<String, Arc<ParticipantRuntime>>>>,
}

impl SagaRuntimeState {
    /// 业务作用：创建开放 UserHook 计划入口、尚未发布任何 Saga 权限的状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：生命周期处于 configuring 的新状态。
    pub(crate) fn new() -> Self {
        Self {
            pending: Mutex::new(None),
            sealed: AtomicBool::new(false),
            lifecycle: AtomicU8::new(0),
            orchestrator: OnceLock::new(),
            participants: OnceLock::new(),
        }
    }

    /// 业务作用：线性化接收唯一 Saga 计划，禁止晚到或重复装配静默覆盖运行角色。
    ///
    /// 参数说明：
    /// - `plan`：已经通过角色与名称校验的完整计划。
    ///
    /// 返回：首次提交成功；Ready 已封口或已有计划时返回 UserHook 错误。
    pub(crate) fn configure(&self, plan: SagaApplicationPlan) -> ApplicationResult<()> {
        plan.validate()?;
        if self.sealed.load(Ordering::Acquire) {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga configuration is sealed before Ready",
            ));
        }
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.sealed.load(Ordering::Acquire) || pending.is_some() {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga plan can be configured only once",
            ));
        }
        *pending = Some(plan);
        Ok(())
    }

    /// 业务作用：在 Ready 入口永久封口计划并移交唯一所有权，后续调用不能改变运行拓扑。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：UserHook 已提交计划时返回该计划；缺失时返回 Ready 错误。
    fn take_plan(&self) -> ApplicationResult<SagaApplicationPlan> {
        self.sealed.store(true, Ordering::Release);
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "saga component requires configure_saga during the application startup hook",
                )
            })
    }

    /// 业务作用：只在全部启动门禁通过后一次性发布运行角色与 Ready 权限。
    ///
    /// 参数说明：
    /// - `plan`：已完成 descriptor 与历史实例校验的封口计划。
    ///
    /// 返回：首次发布成功；内部重复发布返回 Ready 错误。
    fn publish(
        &self,
        mut plan: SagaApplicationPlan,
    ) -> ApplicationResult<Option<OrchestratorPlan>> {
        let orchestrator = plan.orchestrator.take();
        if let Some(runtime) = orchestrator
            .as_ref()
            .map(|entry| Arc::clone(&entry.runtime))
        {
            self.orchestrator.set(runtime).map_err(|_| {
                saga_error(
                    ApplicationPhase::Ready,
                    "saga orchestrator was already published",
                )
            })?;
        }
        self.participants
            .set(Arc::new(plan.participants))
            .map_err(|_| {
                saga_error(
                    ApplicationPhase::Ready,
                    "saga participants were already published",
                )
            })?;
        self.lifecycle.store(1, Ordering::Release);
        Ok(orchestrator)
    }

    /// 业务作用：确认 Saga 能力仍处于 Ready，避免停机排空后继续接收新推进请求。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Ready 时成功；尚未发布或已停机时返回对应阶段错误。
    pub(crate) fn ensure_ready(&self) -> ApplicationResult<()> {
        match self.lifecycle.load(Ordering::Acquire) {
            1 => Ok(()),
            2 => Err(saga_error(
                ApplicationPhase::Stopping,
                "saga runtime is stopping and rejects new work",
            )),
            _ => Err(saga_error(
                ApplicationPhase::Ready,
                "saga runtime has not passed its Ready gate",
            )),
        }
    }

    /// 业务作用：进入停机保护态并永久关闭新的 Saga 能力访问。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；Release 发布保证后续能力读取观察到停机态。
    fn stop(&self) {
        self.lifecycle.store(2, Ordering::Release);
    }
}

pub(crate) struct SagaComponent {
    settings: Option<SagaSettings>,
    contributor: Option<ReadinessContributor>,
    critical_task: Option<ApplicationFuture<'static>>,
}

impl SagaComponent {
    /// 业务作用：创建尚未读取配置、未接收计划的 Saga 生命周期组件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：由 Runner 独占并依次推进 Start、Ready 与停机阶段的组件。
    pub(crate) fn new() -> Self {
        Self {
            settings: None,
            contributor: None,
            critical_task: None,
        }
    }
}

impl ApplicationComponent for SagaComponent {
    /// 业务作用：返回 Saga 生命周期的稳定组件身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`ComponentId::Saga`。
    fn id(&self) -> ComponentId {
        ComponentId::Saga
    }

    /// 业务作用：声明 Saga 持久化门禁必须建立在受管数据库组件之上。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只包含 `ComponentId::Db` 的静态依赖；Outbox 位于 Saga 之后，由组合规范另行强制。
    fn dependencies(&self) -> &'static [ComponentId] {
        &[ComponentId::Db]
    }

    /// 业务作用：校验 timer 预算并注册初始未就绪的关键贡献项。
    ///
    /// 参数说明：
    /// - `context`：提供最终初始配置和动态就绪注册入口的 Start 上下文。
    ///
    /// 返回：配置与贡献项注册成功时完成；非法预算或重名时阻止进入 UserHook。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let settings = read_saga_settings(context.application(), ApplicationPhase::Start)?;
            let contributor = context.application().register_readiness(
                ComponentId::Saga,
                Arc::<str>::from("saga:runtime"),
                ReadinessPolicy {
                    affects_ready: true,
                    failure_threshold: settings.timer_failure_threshold,
                    recovery_threshold: 1,
                    stale_after: None,
                },
            )?;
            self.settings = Some(settings);
            self.contributor = Some(contributor);
            Ok(())
        })
    }

    /// 业务作用：封口计划、执行合同与历史实例门禁，最后发布能力并启动 durable timer。
    ///
    /// 参数说明：
    /// - `context`：提供 Application 共享状态与反向停机 action 登记入口的 Ready 上下文。
    ///
    /// 返回：全部门禁通过后发布 Ready；任何校验或持久化读取失败都保持零对外能力并拒绝启动。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let application = context.application().clone();
            let state = application.saga_runtime();
            let plan = state.take_plan()?;

            if let Some(orchestrator) = plan.orchestrator.as_ref() {
                // definition/descriptor 与历史非终态实例必须在能力发布前同时通过；否则旧实例可能被
                // 新合同驱动，或同一步骤存在多个 handler，均不得进入可接流状态。
                orchestrator
                    .runtime
                    .verify_startup()
                    .await
                    .map_err(|error| {
                        saga_source_error(
                            ApplicationPhase::Ready,
                            "saga orchestrator startup verification failed",
                            error,
                        )
                    })?;
            } else {
                // 纯参与方没有全局 definition 注册表，仍用空注册表校验本 binary 内 descriptor 的
                // 唯一性与字段合法性；远端 definition 投影由 ParticipantRuntime 信任合同约束。
                nasaga_runtime::verify_descriptors(&DefinitionRegistry::new()).map_err(
                    |error| {
                        saga_source_error(
                            ApplicationPhase::Ready,
                            "saga participant descriptor verification failed",
                            error,
                        )
                    },
                )?;
            }

            // 停机 action 必须先入栈，再发布能力；这样后续任一组件 Ready 失败时反向清理一定能先
            // 关闭 Saga 新请求入口，不会留下数据库尚可用但 Saga 权限游离的半完成状态。
            context.activate(Box::new(SagaShutdown {
                state: Arc::clone(&state),
            }));
            let orchestrator = state.publish(plan)?;
            let contributor = self.contributor.as_ref().cloned().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "saga readiness contributor is missing",
                )
            })?;
            contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());

            if let Some(orchestrator) = orchestrator {
                let settings = self.settings.clone().ok_or_else(|| {
                    saga_error(ApplicationPhase::Ready, "saga settings are missing")
                })?;
                self.critical_task = Some(Box::pin(run_timer_loop(
                    application,
                    orchestrator.runtime,
                    orchestrator.timer_owner,
                    settings,
                    contributor,
                )));
            }
            Ok(())
        })
    }

    /// 业务作用：把唯一 durable timer 轮询任务移交 Runner 监督。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：托管 Orchestrator 时首次调用返回任务；纯参与方或重复调用返回 `None`。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("saga-timer-poller", task))
    }
}

struct SagaShutdown {
    state: Arc<SagaRuntimeState>,
}

impl ShutdownAction for SagaShutdown {
    /// 业务作用：返回不含业务身份和配置值的稳定清理动作名。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：固定 Saga 能力关闭标签。
    fn label(&self) -> &'static str {
        "saga-runtime"
    }

    /// 业务作用：先关闭新的 Saga 能力访问，再允许反向清理继续释放数据库和 transport。
    ///
    /// 参数说明：
    /// - `_context`：Runner 提供的共享停机预算；本动作只做原子发布，不消耗外部等待。
    ///
    /// 返回：保护态发布完成后立即成功。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            self.state.stop();
            Ok(())
        })
    }
}

/// 业务作用：持续领取并裁决到期 timer，以 readiness 表达持久化依赖的连续故障与恢复。
///
/// 参数说明：
/// - `application`：用于观察统一停机状态，确保失权后立即退出轮询。
/// - `orchestrator`：唯一通过 Ready 门禁的推进运行时。
/// - `timer_owner`：当前副本的 fencing 身份。
/// - `settings`：正常轮询、错误退避和摘流阈值。
/// - `contributor`：Saga 独占的动态就绪贡献项。
///
/// 返回：应用进入停机态时正常退出；系统时钟不可表示时返回关键任务错误并触发失败停机。
async fn run_timer_loop(
    application: Application,
    orchestrator: Arc<Orchestrator>,
    timer_owner: String,
    settings: SagaSettings,
    contributor: ReadinessContributor,
) -> ApplicationResult<()> {
    loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                return Ok(());
            }
            ApplicationState::Starting => {
                tokio::time::sleep(Duration::from_millis(settings.timer_poll_interval_ms)).await;
                continue;
            }
            ApplicationState::Ready => {}
        }

        let now_ms = epoch_millis()?;
        let operation = tokio::time::timeout(
            Duration::from_millis(settings.timer_operation_timeout_ms),
            orchestrator.run_due_timers(&timer_owner, now_ms),
        )
        .await;
        let delay = match operation {
            Ok(Ok(_)) => {
                contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
                settings.timer_poll_interval_ms
            }
            Ok(Err(_)) | Err(_) => {
                // timer 读取或提交失败时保留持久化事实并退避；阈值由 contributor 统一裁决摘流，
                // 绝不把未提交轮次当成成功，也不因瞬时存储故障主动伪造业务超时结论。
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                settings.timer_error_backoff_ms
            }
        };
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
}

/// 业务作用：读取并校验 Saga 配置段，缺失时使用保守默认预算。
///
/// 参数说明：
/// - `application`：提供同版本不可变配置快照。
/// - `phase`：错误发生的真实生命周期阶段。
///
/// 返回：预算均在安全范围内时返回设置；未知字段、零值或过大预算返回阶段错误。
fn read_saga_settings(
    application: &Application,
    phase: ApplicationPhase,
) -> ApplicationResult<SagaSettings> {
    let snapshot = application.config();
    let settings = match snapshot.value().get("saga") {
        Some(value) => serde_json::from_value(value.clone()).map_err(|error| {
            saga_source_error(phase, "invalid saga configuration section", error)
        })?,
        None => SagaSettings::default(),
    };
    validate_settings(&settings, phase)?;
    Ok(settings)
}

/// 业务作用：在配置发布前验证 Saga 段，确保热刷新候选不会携带不可执行预算。
///
/// 参数说明：
/// - `tree`：合并完成但尚未发布的候选配置树。
/// - `phase`：启动或运行期配置校验阶段。
///
/// 返回：段缺失或设置合法时成功；反序列化和预算错误拒绝整帧配置。
pub(crate) fn validate_saga_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let settings = match tree.get("saga") {
        Some(value) => serde_json::from_value(value.clone()).map_err(|error| {
            saga_source_error(phase, "invalid saga configuration section", error)
        })?,
        None => SagaSettings::default(),
    };
    validate_settings(&settings, phase)
}

/// 业务作用：约束 timer 周期和失败阈值，防止忙循环或超长失联窗口。
///
/// 参数说明：
/// - `settings`：待校验的 Saga 运行设置。
/// - `phase`：形成错误时的真实生命周期阶段。
///
/// 返回：所有值位于封闭范围时成功，否则返回不含配置原值的错误。
fn validate_settings(settings: &SagaSettings, phase: ApplicationPhase) -> ApplicationResult<()> {
    if !(10..=MAX_TIMER_INTERVAL_MS).contains(&settings.timer_poll_interval_ms) {
        return Err(saga_error(
            phase,
            "saga timer poll interval is outside the supported range",
        ));
    }
    if !(10..=MAX_TIMER_INTERVAL_MS).contains(&settings.timer_error_backoff_ms) {
        return Err(saga_error(
            phase,
            "saga timer error backoff is outside the supported range",
        ));
    }
    if !(10..=MAX_TIMER_INTERVAL_MS).contains(&settings.timer_operation_timeout_ms) {
        return Err(saga_error(
            phase,
            "saga timer operation timeout is outside the supported range",
        ));
    }
    if !(1..=MAX_TIMER_FAILURE_THRESHOLD).contains(&settings.timer_failure_threshold) {
        return Err(saga_error(
            phase,
            "saga timer failure threshold is outside the supported range",
        ));
    }
    Ok(())
}

/// 业务作用：校验参与方名称与 timer owner 的稳定低基数身份合同。
///
/// 参数说明：
/// - `value`：未经信任的候选名称。
/// - `kind`：固定字段类别，只用于脱敏错误摘要。
///
/// 返回：1 至 128 字节的小写 canonical 名称成功；其它输入返回 UserHook 错误。
fn validate_runtime_name(value: &str, kind: &'static str) -> ApplicationResult<()> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value.trim() == value
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'.')
        });
    if valid {
        Ok(())
    } else {
        Err(saga_error(
            ApplicationPhase::UserHook,
            format!("saga {kind} must satisfy the canonical identity contract"),
        ))
    }
}

/// 业务作用：读取可用于持久化 Saga 裁决的当前 epoch 毫秒。
///
/// 参数说明: 无。
///
/// 返回：系统时间可表示为 `i64` 毫秒时成功；回拨到 epoch 前或溢出时返回关键任务错误。
fn epoch_millis() -> ApplicationResult<i64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Running,
                "saga timer clock is before the Unix epoch",
                error,
            )
        })?
        .as_millis();
    i64::try_from(millis).map_err(|error| {
        saga_source_error(
            ApplicationPhase::Running,
            "saga timer clock exceeds the supported range",
            error,
        )
    })
}

/// 业务作用：创建不带底层错误正文的 Saga 生命周期错误。
///
/// 参数说明：
/// - `phase`：失败所属生命周期阶段。
/// - `message`：不含业务输入的稳定摘要。
///
/// 返回：归因到 Saga 组件的统一错误。
fn saga_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Saga, phase, message)
}

/// 业务作用：创建保留诊断链、公开摘要保持脱敏的 Saga 生命周期错误。
///
/// 参数说明：
/// - `phase`：失败所属生命周期阶段。
/// - `message`：不含业务输入的稳定摘要。
/// - `source`：只进入统一诊断链的底层错误。
///
/// 返回：归因到 Saga 组件的统一错误。
fn saga_source_error(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Saga, phase, message, source)
}
