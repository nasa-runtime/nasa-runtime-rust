//! 固定隔离配置、命令目录与周期观测的应用所有权。

use crate::readiness::{reason, DependencyState, ReadinessPolicy};
use crate::{
    ApplicationError, ApplicationFuture, ApplicationMode, ApplicationPhase, ApplicationResult,
    ApplicationState, ComponentId, PrepareContext, ShutdownAction, ShutdownContext,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    #[serde(default)]
    enabled: bool,
    max_commands: Option<usize>,
    context_path: Option<String>,
    #[serde(default)]
    isolation: HashMap<String, hystrix::IsolationRule>,
    #[serde(default)]
    commands: BTreeMap<String, CommandPlan>,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandPlan {
    group: String,
    max_concurrent: usize,
    timeout_ms: u64,
    tps_weight: Option<u64>,
}

/// 业务作用：在工作负载前安装唯一命令 owner，预装配显式命令并接管周期任务。
/// 参数说明：`context` 为配置、资源发布及回滚责任上下文。
/// 返回：未启用或 Batch 不创建宿主监控；Service 返回受监督的健康任务。
pub(crate) async fn prepare(
    context: &mut PrepareContext<'_>,
) -> ApplicationResult<Option<ApplicationFuture<'static>>> {
    let app = context.application().clone();
    let config = app.config();
    let Some(value) = config.value().get("hystrix") else {
        return Ok(None);
    };
    if value.get("enabled").and_then(serde_json::Value::as_bool) != Some(true) {
        return Ok(None);
    }
    let plan: Plan = serde_json::from_value(value.clone())
        .map_err(|_| error("invalid managed hystrix declaration"))?;
    if !plan.enabled {
        return Ok(None);
    }
    let max = plan.max_commands.unwrap_or(256);
    if plan.commands.len() > max {
        return Err(error("hystrix command directory exceeds capacity"));
    }
    let runtime = Arc::new(
        hystrix::ManagedRuntime::start(
            &plan.isolation,
            plan.context_path.as_deref().unwrap_or(""),
            max,
        )
        .await
        .map_err(|_| error("hystrix owner or configuration rejected"))?,
    );
    // 全局安装立即归入清理栈，后续命令重名或资源发布失败仍能撤销本代。
    let commands = app.hystrix_commands();
    context.activate_after_business(Box::new(HystrixShutdown {
        runtime: runtime.clone(),
        commands: commands.clone(),
    }));
    for (name, command) in plan.commands {
        let rule = hystrix::IsolationRule {
            max_concurrent: command.max_concurrent,
            timeout_ms: command.timeout_ms,
            tps_weight: command.tps_weight,
        };
        let command = runtime
            .command(&name, &command.group, &rule)
            .map_err(|_| error("hystrix command registration rejected"))?;
        commands.lock().unwrap().insert(name, command);
    }
    if app.info().mode() == ApplicationMode::Batch {
        return Ok(None);
    }
    let health = app.register_readiness(
        ComponentId::Application,
        "hystrix",
        ReadinessPolicy {
            affects_ready: true,
            failure_threshold: 1,
            recovery_threshold: 1,
            stale_after: Some(Duration::from_secs(5)),
        },
    )?;
    health.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
    app.set_hystrix_owner(&runtime, health.clone())?;
    Ok(Some(Box::pin(async move {
        let mut state = app.subscribe_state();
        loop {
            if !matches!(
                *state.borrow_and_update(),
                ApplicationState::Starting | ApplicationState::Ready
            ) {
                return Ok(());
            }
            if !runtime.is_running() {
                health.observe(DependencyState::NotReady, reason::DEGRADED, Instant::now());
                return Err(ApplicationError::new(
                    ComponentId::Application,
                    ApplicationPhase::Running,
                    "hystrix observer exited",
                ));
            }
            health.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
            tokio::select! {biased;_=state.changed()=>{},_=tokio::time::sleep(Duration::from_secs(1))=>{}}
        }
    })))
}
struct HystrixShutdown {
    runtime: Arc<hystrix::ManagedRuntime>,
    commands: Arc<std::sync::Mutex<BTreeMap<String, Arc<hystrix::Command>>>>,
}
impl ShutdownAction for HystrixShutdown {
    /// 业务作用：标识当前应用的隔离命令关闭责任。
    /// 参数说明：无。
    /// 返回：固定清理名称。
    fn label(&self) -> &'static str {
        "hystrix"
    }
    /// 业务作用：业务收尾后关闭新调用，并在共享预算内等待全局引用撤销。
    /// 参数说明：`context` 为宿主最终关闭截止时间。
    /// 返回：全部在途调用和周期任务退出时成功，否则明确报告未完成。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        self.runtime.begin_shutdown();
        Box::pin(async move {
            tokio::time::timeout_at(context.deadline().into(), self.runtime.shutdown())
                .await
                .map_err(|_| {
                    ApplicationError::new(
                        ComponentId::Application,
                        ApplicationPhase::Stopping,
                        "hystrix shutdown incomplete",
                    )
                })?
                .map_err(|_| {
                    ApplicationError::new(
                        ComponentId::Application,
                        ApplicationPhase::Stopping,
                        "hystrix observer failed",
                    )
                })
        })
    }
}
impl Drop for HystrixShutdown {
    /// 业务作用：回滚或等待取消后保留独立收尾 owner，同时永久关闭准入。
    /// 参数说明：无。
    /// 返回：实际在途责任结束后才允许下一代安装。
    fn drop(&mut self) {
        self.runtime.begin_shutdown();
        self.commands.lock().unwrap().clear();
    }
}
/// 业务作用：输出不包含业务路由材料的固定装配失败。
/// 参数说明：`message` 为静态原因。
/// 返回：宿主准备错误。
fn error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ComponentId::Application, ApplicationPhase::Prepare, message)
}
