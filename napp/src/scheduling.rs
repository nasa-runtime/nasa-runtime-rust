use serde::Deserialize;

use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ComponentId, ReadyContext, ShutdownAction, ShutdownContext,
};

/// `scheduling` 段声明的集群执行模式。
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum ClusterMode {
    /// 每个实例都独立触发自己的任务。
    #[default]
    Local,
    /// 只有 leader 实例触发 `cluster="leader"` 任务，需要 Redis 选主。
    Leader,
}

/// 完整配置树中调度组件负责读取的顶层投影。
#[derive(Default, Deserialize)]
#[serde(default)]
struct SchedulingConfigRoot {
    scheduling: SchedulingConfig,
}

/// 调度器启动参数。
///
/// 只描述"如何启动调度器"；具体任务由 `#[scheduled]` 在编译期收集，不在配置里重复声明。
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SchedulingConfig {
    /// 集群执行模式；`leader` 需要同时声明 redis 组件。
    cluster: ClusterMode,
    /// leader 选举使用的业务锁 key；同一组需要互斥的实例必须填相同值。
    leader_key: Option<String>,
    /// 竞选与在任检测周期，毫秒；必须显著小于锁租约以便及时改选。
    leader_period_ms: u64,
    /// 写入运行记录的节点标识；留空表示不区分节点。
    node_id: String,
}

impl Default for SchedulingConfig {
    /// 业务作用：返回不依赖任何外部组件的本地调度缺省配置。
    ///
    /// # 参数
    ///
    /// 本方法无参数；缺省即 local 模式，配置段整体缺失时等价该值。
    fn default() -> Self {
        Self {
            cluster: ClusterMode::Local,
            leader_key: None,
            leader_period_ms: 1_000,
            node_id: String::new(),
        }
    }
}

impl SchedulingConfig {
    /// 业务作用：校验会影响选主时序与启动指纹的取值。
    ///
    /// # 参数
    ///
    /// - `phase`：配置被校验时所属的生命周期阶段。
    fn validate(&self, phase: ApplicationPhase) -> ApplicationResult<()> {
        if self.leader_period_ms == 0 {
            return Err(scheduling_error(
                phase,
                "scheduling.leader_period_ms must be greater than zero",
            ));
        }
        if self.cluster == ClusterMode::Leader
            && self
                .leader_key
                .as_deref()
                .is_none_or(|key| key.trim().is_empty())
        {
            return Err(scheduling_error(
                phase,
                "scheduling.cluster is `leader` but scheduling.leader_key is missing or empty",
            ));
        }
        Ok(())
    }
}

/// 调度组件：在 Ready 段末尾启动 `#[scheduled]` 任务，并在停机时显式关闭调度器。
///
/// 启动放在 Ready 的最后一步是有意的：此时配置、数据源、Redis 和 Web 监听都已就绪，
/// 第一次触发不会打到还没准备好的依赖上。
pub(crate) struct SchedulingComponent {
    #[cfg(feature = "scheduling-cluster")]
    leader: Option<std::sync::Arc<nadis::leader::Leader>>,
}

impl SchedulingComponent {
    /// 业务作用：创建尚未启动调度器的组件。
    ///
    /// # 参数
    ///
    /// 本方法无参数；调度器在 Ready 阶段按最终配置启动。
    pub(crate) fn new() -> Self {
        Self {
            #[cfg(feature = "scheduling-cluster")]
            leader: None,
        }
    }
}

impl ApplicationComponent for SchedulingComponent {
    /// 业务作用：返回调度组件稳定身份。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Runner 用它归类调度相关错误。
    fn id(&self) -> ComponentId {
        ComponentId::Scheduling
    }

    /// 业务作用：按配置启动调度器，并把关闭动作压入逆序清理栈。
    ///
    /// # 参数
    ///
    /// - `context`：提供最终配置、Redis 资源与 active stack 写入口的 Ready 上下文。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let config = read_scheduling_config(context.application())?;
            config.validate(ApplicationPhase::Ready)?;
            let options = self.build_options(context.application(), &config).await?;
            if let Err(error) = nasched::start_scheduled_with(options).await {
                // 集群选主在调度器启动前建立。若调度器因启动指纹或任务定义失败，尚未入栈的
                // leader 必须在返回错误前自行退出，不能等待一个永远不会存在的 ShutdownAction。
                #[cfg(feature = "scheduling-cluster")]
                if let Some(leader) = self.leader.take() {
                    leader.shutdown().await;
                }
                return Err(scheduling_error_src(
                    ApplicationPhase::Ready,
                    "cannot start the scheduled task runtime",
                    error,
                ));
            }
            let runtime = context.application().scheduling_runtime();
            #[cfg(feature = "scheduling-cluster")]
            if let Some(leader) = self.leader.as_ref() {
                if !runtime.publish_leader(leader) {
                    // 调度器关闭失败也不能跳过退位，否则没有进入 active stack 的选主任务会继续续租。
                    let scheduler_rollback = nasched::shutdown_scheduled().await;
                    leader.shutdown().await;
                    scheduler_rollback.map_err(|error| {
                        scheduling_error_src(
                            ApplicationPhase::Ready,
                            "cannot roll back the scheduled task runtime after duplicate capability publication",
                            error,
                        )
                    })?;
                    return Err(scheduling_error(
                        ApplicationPhase::Ready,
                        "scheduling capability state was already published",
                    ));
                }
            }
            runtime.publish_running();
            // 调度器已经开始触发任务后才压栈：这一步之后的任何失败都必须先把它显式停掉。
            context.activate(Box::new(SchedulingShutdown {
                #[cfg(feature = "scheduling-cluster")]
                leader: self.leader.take(),
                runtime,
            }));
            Ok(())
        })
    }
}

impl SchedulingComponent {
    /// 业务作用：把配置翻译成调度器启动选项，必要时先建立 Redis leader gate。
    ///
    /// # 参数
    ///
    /// - `application`：提供已注册 Redis 资源的共享上下文。
    /// - `config`：已校验的调度配置。
    #[cfg_attr(not(feature = "scheduling-cluster"), allow(unused_variables))]
    async fn build_options(
        &mut self,
        application: &Application,
        config: &SchedulingConfig,
    ) -> ApplicationResult<nasched::SchedulerOptions> {
        let mut options = match config.cluster {
            ClusterMode::Local => nasched::SchedulerOptions::local(),
            ClusterMode::Leader => self.build_clustered_options(application, config).await?,
        };
        options.node_id = config.node_id.clone();
        Ok(options)
    }

    /// 业务作用：构造依赖 Redis 选主的集群调度选项。
    ///
    /// # 参数
    ///
    /// - `application`：提供已注册 Redis 客户端资源的共享上下文。
    /// - `config`：已校验且 `cluster=leader` 的调度配置。
    #[cfg(feature = "scheduling-cluster")]
    async fn build_clustered_options(
        &mut self,
        application: &Application,
        config: &SchedulingConfig,
    ) -> ApplicationResult<nasched::SchedulerOptions> {
        use std::sync::Arc;

        let leader_key = config
            .leader_key
            .clone()
            .unwrap_or_else(|| "scheduled:leader".to_owned());
        // Redis 组件必须先声明：这里只取已注册资源，不自行建连，避免出现第二条 Redis 生命周期。
        let client = crate::redis::redis_handle(application, crate::redis::DEFAULT_REDIS)
            .await
            .map_err(|error| {
                scheduling_error_src(
                    ApplicationPhase::Ready,
                    "scheduling.cluster=`leader` requires a redis client; \
                     declare the `redis` component before `scheduling`, or set scheduling.cluster=`local`",
                    error,
                )
            })?;
        let lock = Arc::new(nadis::lock::DistributedLock::new(client));
        let leader = nadis::leader::Leader::elect(
            lock,
            leader_key.clone(),
            std::time::Duration::from_millis(config.leader_period_ms),
        );
        self.leader = Some(leader.clone());
        let gate: Arc<dyn nasched::LeaderGate> = Arc::new(nasched::NadisLeaderGate::new(leader));
        // gate_id 用 leader key：同进程二次启动换了 key 时启动指纹能 fail-fast。
        Ok(nasched::SchedulerOptions::clustered_with_id(
            leader_key, gate,
        ))
    }

    /// 业务作用：在未编入集群适配层时拒绝 `cluster=leader`。
    ///
    /// # 参数
    ///
    /// - `application`：未使用；保持与集群实现相同的调用形态。
    /// - `config`：已校验且 `cluster=leader` 的调度配置。
    #[cfg(not(feature = "scheduling-cluster"))]
    async fn build_clustered_options(
        &mut self,
        application: &Application,
        config: &SchedulingConfig,
    ) -> ApplicationResult<nasched::SchedulerOptions> {
        let _ = (application, config);
        Err(scheduling_error(
            ApplicationPhase::Ready,
            "scheduling.cluster=`leader` requires nasa feature `scheduling-cluster`; \
             enable it, or set scheduling.cluster=`local`",
        ))
    }
}

/// 停机时关闭调度器并退出 leader 选举的可逆 action。
struct SchedulingShutdown {
    #[cfg(feature = "scheduling-cluster")]
    leader: Option<std::sync::Arc<nadis::leader::Leader>>,
    /// 与公开调度句柄共享的运行标志；底层关闭完成后才撤销。
    runtime: std::sync::Arc<crate::capabilities::SchedulingRuntimeState>,
}

impl ShutdownAction for SchedulingShutdown {
    /// 业务作用：返回清理报告使用的稳定动作名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；名称不含 leader key 等配置值。
    fn label(&self) -> &'static str {
        "scheduling"
    }

    /// 业务作用：先停调度触发，再退出选举，避免退位后仍有任务在跑。
    ///
    /// # 参数
    ///
    /// - `_context`：共享停机预算；两步都只做取消与 join，不做无界等待。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let scheduler_result = nasched::shutdown_scheduled().await.map_err(|error| {
                scheduling_error_src(
                    ApplicationPhase::Stopping,
                    "cannot shut down the scheduled task runtime",
                    error,
                )
            });
            #[cfg(feature = "scheduling-cluster")]
            if let Some(leader) = self.leader.take() {
                // 即使调度器清理失败也必须主动退位并停掉续租，避免错误短路留下仍持锁的选主任务。
                leader.shutdown().await;
            }
            self.runtime.close();
            scheduler_result
        })
    }
}

/// 业务作用：从最终配置读取 `scheduling` 段；段缺失时使用 local 缺省配置。
///
/// # 参数
///
/// - `application`：提供当前不可变配置快照的共享上下文。
fn read_scheduling_config(application: &Application) -> ApplicationResult<SchedulingConfig> {
    let snapshot = application.config();
    let root: SchedulingConfigRoot =
        serde_json::from_value((*snapshot.value()).clone()).map_err(|error| {
            scheduling_error_src(
                ApplicationPhase::Ready,
                "invalid `scheduling` configuration section",
                error,
            )
        })?;
    Ok(root.scheduling)
}

/// 业务作用：在不启动调度器的前提下校验候选配置树中的 `scheduling` 段。
///
/// # 参数
///
/// - `tree`：合并、插值完成但尚未发布的候选配置树。
/// - `phase`：本次无副作用校验所属的生命周期阶段。
pub(crate) fn validate_scheduling_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let Some(section) = tree.get("scheduling") else {
        return Ok(());
    };
    let config: SchedulingConfig = serde_json::from_value(section.clone()).map_err(|error| {
        scheduling_error_src(phase, "invalid `scheduling` configuration section", error)
    })?;
    config.validate(phase)
}

/// 业务作用：创建调度组件的稳定生命周期错误。
///
/// # 参数
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：不含配置值的稳定摘要。
fn scheduling_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Scheduling, phase, message)
}

/// 业务作用：创建带底层错误链的调度错误。
///
/// # 参数
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：不含配置值的稳定摘要。
/// - `source`：只供诊断、输出前统一脱敏的底层错误。
fn scheduling_error_src(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Scheduling, phase, message, source)
}
