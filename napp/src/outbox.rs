//! 事务型 Outbox 的 Application 生命周期组件。
//!
//! 业务在 UserHook 只提交发布端和明确的毒丸策略；组件在 Ready 前验证数据库可达，随后持续执行
//! MySQL dispatcher，并把连续失败映射到统一 readiness。停机顺序固定为先停止投递，再释放 transport
//! 和数据库，避免发布中途失去下游或持久化连接。

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use naoutbox_core::OutboxPublisher;
use naoutbox_mysql::MySqlOutbox;
use serde::Deserialize;
use tokio::sync::watch;

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, ReadyContext, ShutdownAction,
    ShutdownContext, StartContext,
};

const DEFAULT_POLL_INTERVAL_MS: u64 = 500;
const DEFAULT_ERROR_BACKOFF_MS: u64 = 1_000;
const DEFAULT_OPERATION_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_BATCH_SIZE: u32 = 100;
const MAX_INTERVAL_MS: u64 = 60_000;
const MAX_BATCH_SIZE: u32 = 10_000;
const MAX_FAILURE_THRESHOLD: u32 = 100;

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct OutboxSettings {
    poll_interval_ms: u64,
    error_backoff_ms: u64,
    operation_timeout_ms: u64,
    batch_size: u32,
    failure_threshold: u32,
}

impl Default for OutboxSettings {
    /// 业务作用：提供不会忙循环且单轮工作量有界的 Outbox 运行参数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：500ms 轮询、1s 故障退避、5s 单轮预算、100 行批次和三次失败摘流。
    fn default() -> Self {
        Self {
            poll_interval_ms: DEFAULT_POLL_INTERVAL_MS,
            error_backoff_ms: DEFAULT_ERROR_BACKOFF_MS,
            operation_timeout_ms: DEFAULT_OPERATION_TIMEOUT_MS,
            batch_size: DEFAULT_BATCH_SIZE,
            failure_threshold: 3,
        }
    }
}

/// 业务作用：明确 Outbox 首个毒丸是否允许让出有序投递通道。
///
/// 默认 `Block` 保持严格顺序并等待人工处置；`DeadLetter` 是显式可用性让步，达到预算后把毒丸留在
/// 数据库死信集合，并允许后续事件继续投递。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxPoisonPolicy {
    /// 首个失败事件持续阻塞后续事件，不自动改变持久化事实。
    Block,
    /// 同一事件达到正数失败预算后进入数据库死信集合。
    DeadLetter {
        /// 标记死信前允许的失败轮次。
        max_attempts: u32,
    },
}

/// 业务作用：描述一个进程唯一的受管 Outbox 发布计划。
///
/// 发布端负责把事件映射到 Kafka、Redis Streams 或其它下游；组件只负责持久化轮询和生命周期，
/// 因而 Outbox 可以脱离 Saga 独立使用。
pub struct OutboxApplicationPlan {
    publisher: Arc<dyn OutboxPublisher + Send + Sync>,
    poison_policy: OutboxPoisonPolicy,
}

impl OutboxApplicationPlan {
    /// 业务作用：创建默认严格保序的受管发布计划。
    ///
    /// 参数说明：
    /// - `publisher`：线程安全的下游发布端；成功必须代表下游已经确认接收。
    ///
    /// 返回：使用 `Block` 毒丸策略的计划。
    pub fn new<P>(publisher: Arc<P>) -> Self
    where
        P: OutboxPublisher + Send + Sync + 'static,
    {
        Self {
            publisher,
            poison_policy: OutboxPoisonPolicy::Block,
        }
    }

    /// 业务作用：显式允许毒丸达到预算后进入死信集合，避免永久阻塞后续事件。
    ///
    /// 参数说明：
    /// - `max_attempts`：同一首个失败事件进入死信前的正数失败轮次。
    ///
    /// 返回：预算有效时返回更新后的计划；零预算返回 UserHook 配置错误。
    pub fn dead_letter_after(mut self, max_attempts: u32) -> ApplicationResult<Self> {
        if max_attempts == 0 {
            return Err(outbox_error(
                ApplicationPhase::UserHook,
                "outbox dead-letter attempts must be greater than zero",
            ));
        }
        self.poison_policy = OutboxPoisonPolicy::DeadLetter { max_attempts };
        Ok(self)
    }
}

/// 业务作用：提供 Outbox 当前累计投递状态的低基数快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxSnapshot {
    rounds: u64,
    published: u64,
    failed_rounds: u64,
}

impl OutboxSnapshot {
    /// 业务作用：读取当前进程已完成的 dispatcher 轮次。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：包含空轮次与失败轮次的累计值。
    pub fn rounds(self) -> u64 {
        self.rounds
    }

    /// 业务作用：读取当前进程已获得下游确认的事件累计数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不包含仍待投递或已进入死信集合的事件数。
    pub fn published(self) -> u64 {
        self.published
    }

    /// 业务作用：读取发生存储错误、超时或发布失败的 dispatcher 轮次。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：进程启动以来的累计失败轮次。
    pub fn failed_rounds(self) -> u64 {
        self.failed_rounds
    }
}

/// 业务作用：提供 Ready 后的 Outbox 只读能力，不授予替换 publisher 或停止 dispatcher 的权限。
#[derive(Clone)]
pub struct OutboxHandle {
    pub(crate) state: Arc<OutboxRuntimeState>,
}

impl OutboxHandle {
    /// 业务作用：读取当前数据库仍等待发布的事件数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：组件 Ready 时返回持久化积压；停机或数据库失败返回统一错误。
    pub async fn pending_count(&self) -> ApplicationResult<u64> {
        self.state.ensure_ready()?;
        MySqlOutbox::new().pending_count().await.map_err(|error| {
            outbox_source_error(
                ApplicationPhase::Running,
                "outbox pending count failed",
                error,
            )
        })
    }

    /// 业务作用：读取数据库中保留的 Outbox 死信事件数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：组件 Ready 时返回死信累计值；停机或数据库失败返回统一错误。
    pub async fn dead_count(&self) -> ApplicationResult<u64> {
        self.state.ensure_ready()?;
        MySqlOutbox::new().dead_count().await.map_err(|error| {
            outbox_source_error(ApplicationPhase::Running, "outbox dead count failed", error)
        })
    }

    /// 业务作用：读取不含事件正文和业务身份的进程级投递计数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：组件 Ready 时返回原子快照；停机或尚未发布时返回阶段错误。
    pub fn snapshot(&self) -> ApplicationResult<OutboxSnapshot> {
        self.state.ensure_ready()?;
        Ok(self.state.snapshot())
    }

    /// 业务作用：导出 Outbox 串行通道的积压、死信和 dispatcher 进程计数，区分空闲、下游故障与毒丸停摆。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：组件 Ready 且数据库可读时返回无业务标签的 Prometheus 文本；停机或查询失败时返回统一错误，
    /// 禁止以缺失指标伪装健康。
    pub async fn render_prometheus(&self) -> ApplicationResult<String> {
        let snapshot = self.snapshot()?;
        let pending = self.pending_count().await?;
        let dead = self.dead_count().await?;
        Ok(format!(
            "# HELP napp_outbox_rounds_total Dispatcher completed rounds.\n\
             # TYPE napp_outbox_rounds_total counter\n\
             napp_outbox_rounds_total {}\n\
             # HELP napp_outbox_published_total Events confirmed by the downstream publisher.\n\
             # TYPE napp_outbox_published_total counter\n\
             napp_outbox_published_total {}\n\
             # HELP napp_outbox_failed_rounds_total Dispatcher rounds ending in storage, timeout, or publish failure.\n\
             # TYPE napp_outbox_failed_rounds_total counter\n\
             napp_outbox_failed_rounds_total {}\n\
             # HELP napp_outbox_pending Durable events waiting for downstream confirmation.\n\
             # TYPE napp_outbox_pending gauge\n\
             napp_outbox_pending {pending}\n\
             # HELP napp_outbox_dead Durable events retained in the local dead-letter set.\n\
             # TYPE napp_outbox_dead gauge\n\
             napp_outbox_dead {dead}\n",
            snapshot.rounds, snapshot.published, snapshot.failed_rounds
        ))
    }
}

pub(crate) struct OutboxRuntimeState {
    pending: Mutex<Option<OutboxApplicationPlan>>,
    sealed: AtomicBool,
    lifecycle: AtomicU8,
    rounds: AtomicU64,
    published: AtomicU64,
    failed_rounds: AtomicU64,
}

impl OutboxRuntimeState {
    /// 业务作用：创建开放 UserHook 计划入口、尚未授予 dispatcher 权限的状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：空的 configuring 状态。
    pub(crate) fn new() -> Self {
        Self {
            pending: Mutex::new(None),
            sealed: AtomicBool::new(false),
            lifecycle: AtomicU8::new(0),
            rounds: AtomicU64::new(0),
            published: AtomicU64::new(0),
            failed_rounds: AtomicU64::new(0),
        }
    }

    /// 业务作用：线性化接收唯一发布计划，禁止多个 publisher 竞争同一数据库顺序通道。
    ///
    /// 参数说明：
    /// - `plan`：已冻结发布端和毒丸策略的完整计划。
    ///
    /// 返回：首次提交成功；Ready 已封口或重复提交返回 UserHook 错误。
    pub(crate) fn configure(&self, plan: OutboxApplicationPlan) -> ApplicationResult<()> {
        if self.sealed.load(Ordering::Acquire) {
            return Err(outbox_error(
                ApplicationPhase::UserHook,
                "outbox configuration is sealed before Ready",
            ));
        }
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.sealed.load(Ordering::Acquire) || pending.is_some() {
            return Err(outbox_error(
                ApplicationPhase::UserHook,
                "outbox plan can be configured only once",
            ));
        }
        *pending = Some(plan);
        Ok(())
    }

    /// 业务作用：在 Ready 入口永久封口并移交发布计划唯一所有权。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已提交计划；缺失时拒绝启动空 dispatcher。
    fn take_plan(&self) -> ApplicationResult<OutboxApplicationPlan> {
        self.sealed.store(true, Ordering::Release);
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| {
                outbox_error(
                    ApplicationPhase::Ready,
                    "outbox component requires a publishing plan during the application startup hook",
                )
            })
    }

    /// 业务作用：在数据库探针成功且停机动作已压栈后发布运行权限。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次发布成功；内部重复发布返回 Ready 错误。
    fn publish_ready(&self) -> ApplicationResult<()> {
        self.lifecycle
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| {
                outbox_error(
                    ApplicationPhase::Ready,
                    "outbox runtime was already published",
                )
            })
    }

    /// 业务作用：确认 Outbox 能力仍处于 Ready，阻止停机后继续发起管理读取。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Ready 时成功；尚未发布或已停止时返回阶段错误。
    pub(crate) fn ensure_ready(&self) -> ApplicationResult<()> {
        match self.lifecycle.load(Ordering::Acquire) {
            1 => Ok(()),
            2 => Err(outbox_error(
                ApplicationPhase::Stopping,
                "outbox runtime is stopping",
            )),
            _ => Err(outbox_error(
                ApplicationPhase::Ready,
                "outbox runtime has not passed its Ready gate",
            )),
        }
    }

    /// 业务作用：发布停机保护态，使能力入口与 dispatcher 在下一轮边界拒绝新投递。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；Release 发布保证任务随后观察到停止状态，应用生命周期通知负责唤醒等待任务。
    fn stop(&self) {
        self.lifecycle.store(2, Ordering::Release);
    }

    /// 业务作用：生成进程级投递计数的一致近似快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：各字段独立单调，不携带事件或发布端身份。
    fn snapshot(&self) -> OutboxSnapshot {
        OutboxSnapshot {
            rounds: self.rounds.load(Ordering::Relaxed),
            published: self.published.load(Ordering::Relaxed),
            failed_rounds: self.failed_rounds.load(Ordering::Relaxed),
        }
    }
}

pub(crate) struct OutboxComponent {
    settings: Option<OutboxSettings>,
    contributor: Option<ReadinessContributor>,
    critical_task: Option<ApplicationFuture<'static>>,
}

impl OutboxComponent {
    /// 业务作用：创建尚未读取配置和发布计划的 Outbox 生命周期组件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：由 Runner 独占的空组件。
    pub(crate) fn new() -> Self {
        Self {
            settings: None,
            contributor: None,
            critical_task: None,
        }
    }
}

impl ApplicationComponent for OutboxComponent {
    /// 业务作用：返回 Outbox 生命周期的稳定组件身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`ComponentId::Outbox`。
    fn id(&self) -> ComponentId {
        ComponentId::Outbox
    }

    /// 业务作用：声明 MySQL dispatcher 必须晚于数据库组件启动。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只包含 `ComponentId::Db` 的静态依赖。
    fn dependencies(&self) -> &'static [ComponentId] {
        &[ComponentId::Db]
    }

    /// 业务作用：校验轮询预算并登记关键 readiness 贡献项。
    ///
    /// 参数说明：
    /// - `context`：提供最终配置与 readiness 注册入口。
    ///
    /// 返回：配置合法且贡献项登记成功时完成；否则阻止进入 UserHook。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let settings = read_outbox_settings(context.application(), ApplicationPhase::Start)?;
            let contributor = context.application().register_readiness(
                ComponentId::Outbox,
                Arc::<str>::from("outbox:dispatcher"),
                ReadinessPolicy {
                    affects_ready: true,
                    failure_threshold: settings.failure_threshold,
                    recovery_threshold: 1,
                    stale_after: None,
                },
            )?;
            self.settings = Some(settings);
            self.contributor = Some(contributor);
            Ok(())
        })
    }

    /// 业务作用：封口发布计划、验证数据库通道并把持续 dispatcher 移交监督器。
    ///
    /// 参数说明：
    /// - `context`：提供 Application、启动预算和反向停机 action 登记入口。
    ///
    /// 返回：计划和数据库均可用时发布 Ready；缺失计划、探针失败或超时拒绝启动。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let application = context.application().clone();
            let state = application.outbox_runtime();
            let plan = state.take_plan()?;
            let settings = self.settings.clone().ok_or_else(|| {
                outbox_error(ApplicationPhase::Ready, "outbox settings are missing")
            })?;
            let probe_budget = context
                .remaining()
                .min(Duration::from_millis(settings.operation_timeout_ms));
            tokio::time::timeout(probe_budget, MySqlOutbox::new().pending_count())
                .await
                .map_err(|error| {
                    outbox_source_error(
                        ApplicationPhase::Ready,
                        "outbox database probe timed out",
                        error,
                    )
                })?
                .map_err(|error| {
                    outbox_source_error(
                        ApplicationPhase::Ready,
                        "outbox database probe failed",
                        error,
                    )
                })?;

            // 停机保护必须先于权限发布入栈；否则后续 Ready 失败时 dispatcher 状态可能游离于
            // Application 反向清理之外，并在 transport 或数据库开始释放后继续投递。
            context.activate(Box::new(OutboxShutdown {
                state: Arc::clone(&state),
            }));
            state.publish_ready()?;
            let contributor = self.contributor.as_ref().cloned().ok_or_else(|| {
                outbox_error(
                    ApplicationPhase::Ready,
                    "outbox readiness contributor is missing",
                )
            })?;
            contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
            self.critical_task = Some(Box::pin(run_dispatch_loop(
                application,
                state,
                plan,
                settings,
                contributor,
            )));
            Ok(())
        })
    }

    /// 业务作用：把唯一 Outbox dispatcher 移交 Runner 作为关键任务监督。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次调用返回任务；重复调用返回 `None`。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("outbox-dispatcher", task))
    }
}

struct OutboxShutdown {
    state: Arc<OutboxRuntimeState>,
}

impl ShutdownAction for OutboxShutdown {
    /// 业务作用：返回不含 publisher 或数据库身份的稳定清理动作名。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：固定 Outbox 停机标签。
    fn label(&self) -> &'static str {
        "outbox-runtime"
    }

    /// 业务作用：先关闭新投递轮次，再允许 transport 和数据库按反向顺序释放。
    ///
    /// 参数说明：
    /// - `_context`：Runner 共享停机预算；本动作只发布内存保护态。
    ///
    /// 返回：保护态发布后立即成功。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            self.state.stop();
            Ok(())
        })
    }
}

/// 业务作用：持续执行有界 Outbox 投递轮次，并把发布和存储健康映射到 readiness。
///
/// 参数说明：
/// - `application`：提供统一进程停机状态。
/// - `state`：提供 Outbox 权限状态和低基数计数。
/// - `plan`：冻结的 publisher 与毒丸策略。
/// - `settings`：轮询、退避、单轮预算和批次上限。
/// - `contributor`：Outbox 独占的 readiness 贡献项。
///
/// 返回：应用停机时正常退出；循环本身不因瞬时下游或数据库故障丢失持久化事件。
async fn run_dispatch_loop(
    application: Application,
    state: Arc<OutboxRuntimeState>,
    plan: OutboxApplicationPlan,
    settings: OutboxSettings,
    contributor: ReadinessContributor,
) -> ApplicationResult<()> {
    let outbox = MySqlOutbox::new();
    let mut application_states = application.subscribe_state();
    let mut committed_appends = MySqlOutbox::subscribe_committed_appends();
    loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                return Ok(());
            }
            ApplicationState::Starting => {
                wait_for_next_round(
                    &mut application_states,
                    &mut committed_appends,
                    settings.poll_interval_ms,
                    false,
                )
                .await;
                continue;
            }
            ApplicationState::Ready => {}
        }

        let operation = async {
            match plan.poison_policy {
                OutboxPoisonPolicy::Block => {
                    outbox
                        .dispatch_batch(plan.publisher.as_ref(), settings.batch_size)
                        .await
                }
                OutboxPoisonPolicy::DeadLetter { max_attempts } => {
                    outbox
                        .dispatch_batch_with_dlt(
                            plan.publisher.as_ref(),
                            settings.batch_size,
                            max_attempts,
                        )
                        .await
                }
            }
        };
        let outcome = tokio::time::timeout(
            Duration::from_millis(settings.operation_timeout_ms),
            operation,
        )
        .await;
        state.rounds.fetch_add(1, Ordering::Relaxed);
        let healthy = match outcome {
            Ok(Ok(report)) => {
                state
                    .published
                    .fetch_add(report.published as u64, Ordering::Relaxed);
                report.first_error.is_none()
            }
            Ok(Err(_)) | Err(_) => false,
        };
        let delay = if healthy {
            contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
            settings.poll_interval_ms
        } else {
            state.failed_rounds.fetch_add(1, Ordering::Relaxed);
            // 未确认的发布必须留在数据库并退避；禁止为保持 Ready 而跳过首个失败事件。
            contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
            settings.error_backoff_ms
        };
        // 健康轮次允许提交信号提前结束等待，使步骤预算不再承担固定轮询相位；失败轮次保持完整
        // 退避，避免持续业务写在下游故障时绕过重试预算。数据库轮询仍覆盖跨进程与崩溃恢复。
        wait_for_next_round(
            &mut application_states,
            &mut committed_appends,
            delay,
            healthy,
        )
        .await;
    }
}

/// 业务作用：等待轮询兜底、已提交事件或停机信号中的最早者，控制下一轮 Outbox 投递时机。
///
/// 参数说明：
/// - `application_states`：提供不会丢失最新值的 Ready 与停机边界通知。
/// - `committed_appends`：当前进程数据库提交代际订阅。
/// - `delay_ms`：没有新提交时的轮询或故障退避上限。
/// - `accept_commit_wake`：健康路径允许提交打断等待；故障路径关闭以保持退避。
///
/// 返回：任一条件先满足即返回；不解释数据库事实，调用方下一轮仍以持久化待投递行为准。
async fn wait_for_next_round(
    application_states: &mut watch::Receiver<ApplicationState>,
    committed_appends: &mut watch::Receiver<u64>,
    delay_ms: u64,
    accept_commit_wake: bool,
) {
    let delay = tokio::time::sleep(Duration::from_millis(delay_ms));
    tokio::pin!(delay);
    if accept_commit_wake {
        tokio::select! {
            biased;
            _ = application_states.changed() => {}
            _ = committed_appends.changed() => {}
            _ = &mut delay => {}
        }
    } else {
        tokio::select! {
            biased;
            _ = application_states.changed() => {}
            _ = &mut delay => {}
        }
    }
}

/// 业务作用：读取并校验 Outbox 配置段，缺失时使用保守默认预算。
///
/// 参数说明：
/// - `application`：提供当前不可变配置快照。
/// - `phase`：错误发生的真实生命周期阶段。
///
/// 返回：设置合法时返回强类型值；未知字段或越界预算返回阶段错误。
fn read_outbox_settings(
    application: &Application,
    phase: ApplicationPhase,
) -> ApplicationResult<OutboxSettings> {
    let snapshot = application.config();
    let settings = match snapshot.value().get("outbox") {
        Some(value) => serde_json::from_value(value.clone()).map_err(|error| {
            outbox_source_error(phase, "invalid outbox configuration section", error)
        })?,
        None => OutboxSettings::default(),
    };
    validate_settings(&settings, phase)?;
    Ok(settings)
}

/// 业务作用：在配置发布前验证 Outbox 段，阻止不可执行预算进入运行快照。
///
/// 参数说明：
/// - `tree`：合并完成但尚未发布的候选配置树。
/// - `phase`：启动或运行期配置校验阶段。
///
/// 返回：段缺失或设置合法时成功；反序列化和预算错误拒绝整帧配置。
pub(crate) fn validate_outbox_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let settings = match tree.get("outbox") {
        Some(value) => serde_json::from_value(value.clone()).map_err(|error| {
            outbox_source_error(phase, "invalid outbox configuration section", error)
        })?,
        None => OutboxSettings::default(),
    };
    validate_settings(&settings, phase)
}

/// 业务作用：约束轮询周期、单轮预算、批次和失败阈值，防止忙循环与无界占用。
///
/// 参数说明：
/// - `settings`：待校验的 Outbox 设置。
/// - `phase`：形成错误时的真实生命周期阶段。
///
/// 返回：全部字段处于封闭范围时成功，否则返回脱敏配置错误。
fn validate_settings(settings: &OutboxSettings, phase: ApplicationPhase) -> ApplicationResult<()> {
    for value in [
        settings.poll_interval_ms,
        settings.error_backoff_ms,
        settings.operation_timeout_ms,
    ] {
        if !(10..=MAX_INTERVAL_MS).contains(&value) {
            return Err(outbox_error(
                phase,
                "outbox interval is outside the supported range",
            ));
        }
    }
    if !(1..=MAX_BATCH_SIZE).contains(&settings.batch_size) {
        return Err(outbox_error(
            phase,
            "outbox batch size is outside the supported range",
        ));
    }
    if !(1..=MAX_FAILURE_THRESHOLD).contains(&settings.failure_threshold) {
        return Err(outbox_error(
            phase,
            "outbox failure threshold is outside the supported range",
        ));
    }
    Ok(())
}

/// 业务作用：创建不含配置值、事件内容或下游身份的 Outbox 生命周期错误。
///
/// 参数说明：
/// - `phase`：失败所属生命周期阶段。
/// - `message`：稳定脱敏摘要。
///
/// 返回：归因到 Outbox 组件的统一错误。
fn outbox_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Outbox, phase, message)
}

/// 业务作用：创建保留诊断链、公开摘要保持脱敏的 Outbox 生命周期错误。
///
/// 参数说明：
/// - `phase`：失败所属生命周期阶段。
/// - `message`：稳定脱敏摘要。
/// - `source`：只进入统一诊断链的底层错误。
///
/// 返回：归因到 Outbox 组件的统一错误。
fn outbox_source_error(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Outbox, phase, message, source)
}
