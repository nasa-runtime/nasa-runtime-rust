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
#[cfg(feature = "saga-redis-stream")]
use nasaga_runtime::SagaStreamPoller;
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
    #[cfg(feature = "saga-redis-stream")]
    redis_transport: Option<SagaRedisTransportPlan>,
}

/// 业务作用：Saga 的 Redis Streams 受管消费子计划——把已构造的 result/command 消费者
/// 交给 Application 生命周期:Ready 前统一探测拓扑/group/ACL/route owner,运行期由
/// Runner 监督消费循环,停机先关领取、排空在途轮次,Redis 连接由更早启动的 Redis
/// 组件在其后释放(`DB -> Redis -> Saga -> Outbox` 的逆序)。
///
/// 发布端不在本计划内:command/result 事件仍经由受管 Outbox 的发布端合同投递,
/// 本计划只托管消费侧。
#[cfg(feature = "saga-redis-stream")]
pub struct SagaRedisTransportPlan {
    client_name: String,
    pollers: Vec<Arc<dyn SagaStreamPoller>>,
    poll_idle_ms: u64,
    error_backoff_ms: u64,
}

#[cfg(feature = "saga-redis-stream")]
impl SagaRedisTransportPlan {
    /// 业务作用：创建绑定某个受管 Redis 客户端、尚无消费者的传输子计划。
    ///
    /// 参数说明：
    /// - `client_name`: 受管 Redis 实例 qualifier(单实例配置固定 `default`)。
    ///
    /// 返回：必须继续加入至少一个消费者后才能提交的空子计划。
    pub fn new(client_name: impl Into<String>) -> Self {
        Self {
            client_name: client_name.into(),
            pollers: Vec::new(),
            poll_idle_ms: 10,
            error_backoff_ms: 1_000,
        }
    }

    /// 业务作用：加入一个已构造的消费者(result 或 command)。
    ///
    /// 消费身份 `(stream, group, consumer)` 在计划内必须唯一:同一身份重复轮询会把
    /// 同一份 PEL 交给两个循环,重领与确认互相踩踏。
    ///
    /// 参数说明：
    /// - `poller`: 已通过构造期配置校验的消费者。
    ///
    /// 返回：身份唯一时返回自身;重复身份返回 UserHook 配置错误。
    pub fn with_poller(mut self, poller: Arc<dyn SagaStreamPoller>) -> ApplicationResult<Self> {
        let config = poller.config();
        let identity = (
            config.stream.clone(),
            config.group.clone(),
            config.consumer.clone(),
        );
        if self.pollers.iter().any(|existing| {
            let existing = existing.config();
            (
                existing.stream.as_str(),
                existing.group.as_str(),
                existing.consumer.as_str(),
            ) == (
                identity.0.as_str(),
                identity.1.as_str(),
                identity.2.as_str(),
            )
        }) {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga redis transport pollers must have unique (stream, group, consumer)",
            ));
        }
        self.pollers.push(poller);
        Ok(self)
    }

    /// 业务作用：调整轮询间歇与故障退避(默认 10ms/1s)。
    ///
    /// 间歇只是让位调度的下限——XREADGROUP 的 BLOCK 预算才是等待新消息的主体;
    /// 退避防止 Redis 故障期忙循环。两者都必须有界。
    ///
    /// 参数说明：
    /// - `poll_idle_ms`: 相邻两轮之间的间歇毫秒(1..=60_000)。
    /// - `error_backoff_ms`: 单轮失败后的退避毫秒(1..=60_000)。
    ///
    /// 返回：预算有界时返回自身;越界返回 UserHook 配置错误。
    pub fn with_budgets(
        mut self,
        poll_idle_ms: u64,
        error_backoff_ms: u64,
    ) -> ApplicationResult<Self> {
        if !(1..=60_000).contains(&poll_idle_ms) || !(1..=60_000).contains(&error_backoff_ms) {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga redis transport budgets must be within 1ms..=60s",
            ));
        }
        self.poll_idle_ms = poll_idle_ms;
        self.error_backoff_ms = error_backoff_ms;
        Ok(self)
    }

    /// 业务作用：校验子计划完整性——空消费者集合的传输计划没有业务意义。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：客户端名与消费者集合合法返回 `Ok`。
    fn validate(&self) -> ApplicationResult<()> {
        if self.client_name.is_empty() || self.client_name.len() > 128 {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga redis transport requires a bounded client name",
            ));
        }
        if self.pollers.is_empty() {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga redis transport requires at least one poller",
            ));
        }
        Ok(())
    }
}

/// 业务作用：单条受管 stream 消费的进程级观测状态——区分"某条流停摆"与"整个
/// 消费任务退出";标签值来自 Ready 时冻结的 (stream, group),基数有界。
#[cfg(feature = "saga-redis-stream")]
pub(crate) struct StreamRuntime {
    stream: String,
    group: String,
    consumer: String,
    acked: std::sync::atomic::AtomicU64,
    dead_lettered: std::sync::atomic::AtomicU64,
    retained: std::sync::atomic::AtomicU64,
    reclaimed: std::sync::atomic::AtomicU64,
    deleted_pending: std::sync::atomic::AtomicU64,
    auth_rejected: std::sync::atomic::AtomicU64,
    failed_rounds: std::sync::atomic::AtomicU64,
    handled: std::sync::atomic::AtomicU64,
    handler_micros_sum: std::sync::atomic::AtomicU64,
    pending: std::sync::atomic::AtomicU64,
    oldest_pel_age_ms: std::sync::atomic::AtomicU64,
    healthy: AtomicBool,
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
            #[cfg(feature = "saga-redis-stream")]
            redis_transport: None,
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

    /// 业务作用：为 Saga 内部必需的 Outbox 绑定一份**已完整配置**的发布计划——发布端之外
    /// 还要保留清理、多通道分片等能力时使用本入口。
    ///
    /// [`with_event_publisher`](Self::with_event_publisher) 只绑定发布端，是最简形式；隐式
    /// Outbox 的其余能力（`with_retention`、`with_channel_lanes` 等）都构建在
    /// [`OutboxApplicationPlan`](crate::OutboxApplicationPlan) 上，因此本入口直接接收整份
    /// 计划，避免每新增一个 Outbox 能力就要在 Saga 侧复制一个透传方法、也不会让受管 Saga
    /// 的业务够不到已有能力。两个入口互斥，只能选其一且只能调用一次。
    ///
    /// 参数说明：
    /// - `plan`：已绑定发布端并按需附加保留清理、通道分片的 Outbox 计划。
    ///
    /// 返回：首次绑定返回更新后的 Saga 计划；重复绑定返回 UserHook 配置错误。
    pub fn with_event_publisher_plan(
        mut self,
        plan: crate::outbox::OutboxApplicationPlan,
    ) -> ApplicationResult<Self> {
        if self.outbox.is_some() {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga event publisher can be configured only once",
            ));
        }
        self.outbox = Some(plan);
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

    /// 业务作用：提交 Redis Streams 受管消费子计划——消费循环交给 Application 监督。
    ///
    /// 需要组合声明包含受管 Redis 组件(`redis` 角色);Ready 前用真实客户端统一探测
    /// 拓扑、group 幂等创建与 ACL,失败拒绝 Ready。发布端不受影响,仍走受管 Outbox。
    ///
    /// 参数说明：
    /// - `transport`: 已装配消费者的传输子计划。
    ///
    /// 返回：首次提交且子计划自洽时返回自身;重复提交或计划不完整返回 UserHook 错误。
    #[cfg(feature = "saga-redis-stream")]
    pub fn with_redis_stream_transport(
        mut self,
        transport: SagaRedisTransportPlan,
    ) -> ApplicationResult<Self> {
        if self.redis_transport.is_some() {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga redis transport can be configured only once",
            ));
        }
        transport.validate()?;
        self.redis_transport = Some(transport);
        Ok(self)
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
    #[cfg(feature = "saga-redis-stream")]
    streams: OnceLock<Arc<Vec<Arc<StreamRuntime>>>>,
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
            #[cfg(feature = "saga-redis-stream")]
            streams: OnceLock::new(),
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
    #[cfg(feature = "saga-redis-stream")]
    stream_contributor: Option<ReadinessContributor>,
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
            #[cfg(feature = "saga-redis-stream")]
            stream_contributor: None,
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
            // 就绪注册表在 UserHook 完成时封口,而计划要到 UserHook 才提交:此处必须
            // 先注册 stream 贡献项占位;Ready 阶段若计划不含 Redis transport,占位被
            // 一次性置绿中和,不影响未启用者。
            #[cfg(feature = "saga-redis-stream")]
            {
                self.stream_contributor = Some(context.application().register_readiness(
                    ComponentId::Saga,
                    Arc::<str>::from("saga:redis-stream"),
                    ReadinessPolicy {
                        affects_ready: true,
                        failure_threshold: settings.timer_failure_threshold,
                        recovery_threshold: 1,
                        stale_after: None,
                    },
                )?);
            }
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
            #[cfg_attr(not(feature = "saga-redis-stream"), allow(unused_mut))]
            let mut plan = state.take_plan()?;
            // Redis transport 属组件生命周期资产,不随计划进入只读能力发布;必须在
            // publish 前取走。
            #[cfg(feature = "saga-redis-stream")]
            let redis_transport = plan.redis_transport.take();

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

            let timer_task: Option<ApplicationFuture<'static>> =
                if let Some(orchestrator) = orchestrator {
                    let settings = self.settings.clone().ok_or_else(|| {
                        saga_error(ApplicationPhase::Ready, "saga settings are missing")
                    })?;
                    Some(Box::pin(run_timer_loop(
                        application.clone(),
                        orchestrator.runtime,
                        orchestrator.timer_owner,
                        settings,
                        contributor,
                    )))
                } else {
                    None
                };

            #[cfg(feature = "saga-redis-stream")]
            let stream_task: Option<ApplicationFuture<'static>> = {
                let stream_contributor =
                    self.stream_contributor.as_ref().cloned().ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "saga stream readiness contributor is missing",
                        )
                    })?;
                match redis_transport {
                    Some(transport) => {
                        // Ready 前用真实客户端统一探测:PING、配置合同、group 幂等创建
                        // (兼 ACL 探测)。任何失败都拒绝 Ready——不能带着无法领取消息的
                        // 消费拓扑对外宣布可用。
                        let client = application.redis(&transport.client_name).await?;
                        let configs: Vec<&nasaga_runtime::SagaStreamConsumerConfig> = transport
                            .pollers
                            .iter()
                            .map(|poller| poller.config())
                            .collect();
                        nasaga_runtime::verify_stream_transport_ready(&client, &configs)
                            .await
                            .map_err(|error| {
                                saga_source_error(
                                    ApplicationPhase::Ready,
                                    "saga redis stream transport readiness probe failed",
                                    error,
                                )
                            })?;
                        let runtimes: Vec<Arc<StreamRuntime>> = transport
                            .pollers
                            .iter()
                            .map(|poller| {
                                let config = poller.config();
                                Arc::new(StreamRuntime::new(
                                    &config.stream,
                                    &config.group,
                                    &config.consumer,
                                ))
                            })
                            .collect();
                        state.publish_streams(runtimes.clone())?;
                        stream_contributor.observe(
                            DependencyState::Ready,
                            reason::HEALTHY,
                            Instant::now(),
                        );
                        Some(Box::pin(run_stream_poll_loop(
                            application.clone(),
                            client,
                            transport.pollers,
                            runtimes,
                            transport.poll_idle_ms,
                            transport.error_backoff_ms,
                            stream_contributor,
                        )) as ApplicationFuture<'static>)
                    }
                    None => {
                        // 计划不含 Redis transport:Start 注册的占位贡献项一次性置绿,
                        // 不影响未启用者的 readiness。
                        stream_contributor.observe(
                            DependencyState::Ready,
                            reason::HEALTHY,
                            Instant::now(),
                        );
                        None
                    }
                }
            };
            #[cfg(not(feature = "saga-redis-stream"))]
            let stream_task: Option<ApplicationFuture<'static>> = None;

            self.critical_task = match (timer_task, stream_task) {
                (None, None) => None,
                (Some(timer), None) => Some(timer),
                (timer, Some(stream)) => {
                    Some(Box::pin(run_saga_supervised_loops(timer, Some(stream))))
                }
            };
            Ok(())
        })
    }

    /// 业务作用：把唯一 durable timer 轮询任务移交 Runner 监督。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：托管 Orchestrator 时首次调用返回任务；纯参与方或重复调用返回 `None`。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        // 标签固定:任务内容(timer/stream 消费)由 Ready 阶段组装,Runner 只看单一
        // 受监督入口;任一内部循环异常退出都会以本任务失败触发统一停机。
        self.critical_task
            .take()
            .map(|task| ("saga-runtime-loops", task))
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

#[cfg(feature = "saga-redis-stream")]
impl StreamRuntime {
    /// 业务作用：创建单条受管 stream 的零值观测状态。
    ///
    /// 参数说明：
    /// - `stream`: 源 stream 名(冻结标签值)。
    /// - `group`: consumer group 名(冻结标签值)。
    ///
    /// 返回：全零计数、初始健康的状态。
    fn new(stream: &str, group: &str, consumer: &str) -> Self {
        Self {
            stream: stream.to_string(),
            group: group.to_string(),
            consumer: consumer.to_string(),
            acked: std::sync::atomic::AtomicU64::new(0),
            dead_lettered: std::sync::atomic::AtomicU64::new(0),
            retained: std::sync::atomic::AtomicU64::new(0),
            reclaimed: std::sync::atomic::AtomicU64::new(0),
            deleted_pending: std::sync::atomic::AtomicU64::new(0),
            auth_rejected: std::sync::atomic::AtomicU64::new(0),
            failed_rounds: std::sync::atomic::AtomicU64::new(0),
            handled: std::sync::atomic::AtomicU64::new(0),
            handler_micros_sum: std::sync::atomic::AtomicU64::new(0),
            pending: std::sync::atomic::AtomicU64::new(0),
            oldest_pel_age_ms: std::sync::atomic::AtomicU64::new(0),
            healthy: AtomicBool::new(true),
        }
    }

    /// 业务作用：吸收一轮消费报告到累计计数。
    ///
    /// 参数说明：
    /// - `report`: 单轮 poll 报告。
    ///
    /// 返回：无返回值。
    fn absorb(&self, report: &nasaga_runtime::StreamPollReport) {
        self.acked.fetch_add(report.acked, Ordering::Relaxed);
        self.dead_lettered
            .fetch_add(report.dead_lettered, Ordering::Relaxed);
        self.retained.fetch_add(report.retained, Ordering::Relaxed);
        self.reclaimed
            .fetch_add(report.reclaimed, Ordering::Relaxed);
        self.deleted_pending
            .fetch_add(report.deleted_pending, Ordering::Relaxed);
        self.auth_rejected
            .fetch_add(report.auth_rejected, Ordering::Relaxed);
        self.handled.fetch_add(report.handled, Ordering::Relaxed);
        self.handler_micros_sum
            .fetch_add(report.handler_micros_sum, Ordering::Relaxed);
    }

    /// 业务作用：刷新本流的积压 gauge——pending 数与最老 PEL 年龄。
    ///
    /// 参数说明：
    /// - `pending`: 当前 PEL 数。
    /// - `oldest_age_ms`: 最老 pending entry 年龄;PEL 为空时归零。
    ///
    /// 返回：无返回值。
    fn set_backlog(&self, pending: u64, oldest_age_ms: Option<u64>) {
        self.pending.store(pending, Ordering::Relaxed);
        self.oldest_pel_age_ms
            .store(oldest_age_ms.unwrap_or(0), Ordering::Relaxed);
    }
}

#[cfg(feature = "saga-redis-stream")]
impl SagaRuntimeState {
    /// 业务作用：Ready 时一次性发布冻结的受管 stream 观测集合。
    ///
    /// 参数说明：
    /// - `runtimes`: 与消费者一一对应的观测状态。
    ///
    /// 返回：首次发布成功;重复发布返回 Ready 错误。
    fn publish_streams(&self, runtimes: Vec<Arc<StreamRuntime>>) -> ApplicationResult<()> {
        self.streams.set(Arc::new(runtimes)).map_err(|_| {
            saga_error(
                ApplicationPhase::Ready,
                "saga stream runtimes were already published",
            )
        })
    }

    /// 业务作用：读取冻结的受管 stream 观测集合,供低基数指标渲染。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Ready 前或未启用 transport 时为空集合。
    pub(crate) fn stream_runtimes(&self) -> Arc<Vec<Arc<StreamRuntime>>> {
        self.streams
            .get()
            .cloned()
            .unwrap_or_else(|| Arc::new(Vec::new()))
    }
}

/// 业务作用：渲染受管 stream 消费的低基数 Prometheus 文本——标签值来自 Ready 冻结的
/// (stream, group) 集合;`deleted_pending` 非零表示 entry 在确认前被外部删除,必须告警。
///
/// 参数说明：
/// - `state`: Saga 运行时状态。
///
/// 返回：按 stream 分组的指标文本;未启用 transport 时为空串。
#[cfg(feature = "saga-redis-stream")]
pub(crate) fn render_stream_metrics(state: &SagaRuntimeState) -> String {
    /// Prometheus label 值转义:合法配置里的反斜线、引号与换行不允许破坏 exposition。
    fn escape_label(value: &str) -> String {
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
    }
    let mut output = String::new();
    for runtime in state.stream_runtimes().iter() {
        // 标签含 consumer 维度:同一 (stream, group) 允许多个消费身份,缺它会导出
        // 多条完全相同 label set 的 series。三个标签值都来自 Ready 冻结集合,基数有界。
        let labels = format!(
            "{{stream=\"{}\",group=\"{}\",consumer=\"{}\"}}",
            escape_label(&runtime.stream),
            escape_label(&runtime.group),
            escape_label(&runtime.consumer)
        );
        output.push_str(&format!(
            "napp_saga_stream_acked_total{labels} {}\n\
             napp_saga_stream_dead_lettered_total{labels} {}\n\
             napp_saga_stream_retained_total{labels} {}\n\
             napp_saga_stream_reclaimed_total{labels} {}\n\
             napp_saga_stream_deleted_pending_total{labels} {}\n\
             napp_saga_stream_auth_rejected_total{labels} {}\n\
             napp_saga_stream_failed_rounds_total{labels} {}\n\
             napp_saga_stream_handled_total{labels} {}\n\
             napp_saga_stream_handler_micros_sum{labels} {}\n\
             napp_saga_stream_pending{labels} {}\n\
             napp_saga_stream_oldest_pel_age_ms{labels} {}\n\
             napp_saga_stream_healthy{labels} {}\n",
            runtime.acked.load(Ordering::Relaxed),
            runtime.dead_lettered.load(Ordering::Relaxed),
            runtime.retained.load(Ordering::Relaxed),
            runtime.reclaimed.load(Ordering::Relaxed),
            runtime.deleted_pending.load(Ordering::Relaxed),
            runtime.auth_rejected.load(Ordering::Relaxed),
            runtime.failed_rounds.load(Ordering::Relaxed),
            runtime.handled.load(Ordering::Relaxed),
            runtime.handler_micros_sum.load(Ordering::Relaxed),
            runtime.pending.load(Ordering::Relaxed),
            runtime.oldest_pel_age_ms.load(Ordering::Relaxed),
            u8::from(runtime.healthy.load(Ordering::Relaxed)),
        ));
    }
    if !output.is_empty() {
        // 发布端重复提示是进程级计数(publisher 不绑定单一 stream 标签),随流指标一并导出。
        output.push_str(&format!(
            "napp_saga_stream_publisher_duplicate_hints_total {}\n",
            nasaga_runtime::publisher_duplicate_hints_total()
        ));
    }
    output
}

/// 业务作用：把 timer 轮询与 stream 消费收敛为单一受监督入口——任一循环异常退出都
/// 视为关键任务失败,由 Runner 触发统一停机;正常停机时两循环各自观察应用状态退出。
///
/// 参数说明：
/// - `timer`: 可选的 durable timer 循环(托管 Orchestrator 时存在)。
/// - `stream`: 可选的 Redis Streams 消费循环。
///
/// 返回：全部循环正常退出返回 `Ok`;任一循环错误或 panic 返回关键任务错误。
async fn run_saga_supervised_loops(
    timer: Option<ApplicationFuture<'static>>,
    stream: Option<ApplicationFuture<'static>>,
) -> ApplicationResult<()> {
    let mut set = tokio::task::JoinSet::new();
    if let Some(task) = timer {
        set.spawn(task);
    }
    if let Some(task) = stream {
        set.spawn(task);
    }
    while let Some(joined) = set.join_next().await {
        joined.map_err(|_| {
            saga_error(
                ApplicationPhase::Running,
                "saga supervised loop terminated abnormally",
            )
        })??;
    }
    Ok(())
}

/// 业务作用：持续轮询受管 stream 消费者,按封闭裁决推进并以 readiness 表达连续故障。
///
/// 停机语义固定:观察到应用停止即**先关领取**——不再发起新的 XREADGROUP/XAUTOCLAIM;
/// 在途轮次内已接管的消息由 `poll_once` 自身排空(handler 完成或超时留 PEL),未确认
/// 消息留在 PEL 交由重启后重领;Redis 连接由更早启动的 Redis 组件在本组件之后释放。
///
/// 参数说明：
/// - `application`: 观察统一停机状态。
/// - `client`: 受管 Redis 客户端。
/// - `pollers`: 冻结的消费者集合。
/// - `runtimes`: 与消费者一一对应的观测状态。
/// - `poll_idle_ms`: 轮间让位间歇。
/// - `error_backoff_ms`: 故障退避。
/// - `contributor`: stream 消费独占的动态就绪贡献项。
///
/// 返回：应用进入停机态时正常退出;系统时钟不可表示时返回关键任务错误。
#[cfg(feature = "saga-redis-stream")]
async fn run_stream_poll_loop(
    application: Application,
    client: Arc<nadis::RedisClient>,
    pollers: Vec<Arc<dyn SagaStreamPoller>>,
    runtimes: Vec<Arc<StreamRuntime>>,
    poll_idle_ms: u64,
    error_backoff_ms: u64,
    contributor: ReadinessContributor,
) -> ApplicationResult<()> {
    loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                return Ok(());
            }
            ApplicationState::Starting => {
                sleep_observing_state(&application, poll_idle_ms).await;
                continue;
            }
            ApplicationState::Ready => {}
        }
        let now_ms = epoch_millis()?;
        let mut round_healthy = true;
        for (poller, runtime) in pollers.iter().zip(runtimes.iter()) {
            // 停机信号在流与流之间复查:先关领取,不把停机窗口拖长到整轮结束。
            if !matches!(application.state(), ApplicationState::Ready) {
                break;
            }
            match poller.poll_once(&client, now_ms).await {
                Ok(report) => {
                    runtime.absorb(&report);
                    // 积压 gauge 与消费同轮刷新:pending 与最老 PEL 年龄是"消费是否
                    // 追得上"的直接证据;探测失败与消费失败同等计入轮失败,不导出
                    // 陈旧假数据。
                    let config = poller.config();
                    match nasaga_runtime::stream_group_backlog(
                        &client,
                        &config.stream,
                        &config.group,
                        now_ms,
                    )
                    .await
                    {
                        Ok((pending, oldest_age_ms)) => {
                            runtime.set_backlog(pending, oldest_age_ms);
                            runtime.healthy.store(true, Ordering::Relaxed);
                        }
                        Err(_) => {
                            round_healthy = false;
                            runtime.failed_rounds.fetch_add(1, Ordering::Relaxed);
                            runtime.healthy.store(false, Ordering::Relaxed);
                        }
                    }
                }
                Err(_) => {
                    // Redis 往返失败:消息原位保留(PEL/stream 不动),只退避重试;
                    // 摘流由 contributor 阈值统一裁决,不在单轮内武断退出。
                    round_healthy = false;
                    runtime.failed_rounds.fetch_add(1, Ordering::Relaxed);
                    runtime.healthy.store(false, Ordering::Relaxed);
                }
            }
        }
        let delay = if round_healthy {
            contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
            poll_idle_ms
        } else {
            contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
            error_backoff_ms
        };
        // 停机不等退避:分片睡眠把响应上界固定在一个分片内,停机信号落在退避中途
        // 也立即收口;循环顶部据状态退出,未确认消息留在 PEL 交给重启后重领。
        sleep_observing_state(&application, delay).await;
    }
}

/// 业务作用：可被停机打断的分片睡眠——把任意长的退避/轮询间歇切成 ≤200ms 片,
/// 每片后复查应用状态。停机信号无论落在睡眠的哪个时刻,响应上界都固定在一个分片,
/// 不会被 60 秒级故障退避拖满。
///
/// 参数说明：
/// - `application`: 观察统一停机状态。
/// - `total_ms`: 期望睡眠总时长(毫秒)。
///
/// 返回：睡满或状态离开 Ready 提前返回;由调用方循环顶部统一裁决去留。
async fn sleep_observing_state(application: &Application, total_ms: u64) {
    let mut remaining = total_ms;
    while remaining > 0 {
        let slice = remaining.min(200);
        tokio::time::sleep(Duration::from_millis(slice)).await;
        remaining -= slice;
        if !matches!(application.state(), ApplicationState::Ready) {
            return;
        }
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
                sleep_observing_state(&application, settings.timer_poll_interval_ms).await;
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
        // 退避同样必须可被停机打断:分片睡眠保证失权后一个分片内退出轮询。
        sleep_observing_state(&application, delay).await;
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
