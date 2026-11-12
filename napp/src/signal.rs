use std::future::pending;

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    report::write_stderr, state::TerminalIntent, Application, ApplicationError, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, ShutdownSignal,
};

/// 信号控制面的启动方式；嵌入式 Runner 可关闭监听，进程入口显式选择真实监听。
pub(crate) enum SignalMode {
    /// 嵌入式使用时由宿主管理信号。
    Disabled,
    /// 应用进程建立独立监听任务。
    Process,
}

/// Runner 接收到的信号控制面事件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SignalEvent {
    /// 操作系统监听任务转交的终止信号。
    Received(ShutdownSignal),
    /// 监听发送侧已关闭且没有缓存信号。
    BrokerClosed,
}

/// 独立于普通任务组的信号控制面，保证 active stack 清理期间仍能观察强退信号。
pub(crate) struct SignalBroker {
    /// Runner 独占的信号接收端；禁用或停止后为空。
    receiver: Option<mpsc::UnboundedReceiver<ShutdownSignal>>,
    /// 最终退出路径通知监听任务结束的单次通道。
    stop: Option<oneshot::Sender<()>>,
    /// 停止时等待的监听任务，独立于普通受管任务组。
    task: Option<JoinHandle<()>>,
}

impl SignalBroker {
    /// 业务作用：按指定模式建立独立信号监听，等待监听任务确认启动。
    ///
    /// Unix 在两个 handler 注册后确认；其它平台确认仅表示任务已启动，ctrl_c 注册在随后轮询时发生。
    ///
    /// 参数说明：
    ///
    /// - `mode`：决定关闭监听或注册真实进程信号。
    /// - `application`：提供公开状态和首次终态，供停机中的强退判定使用。
    ///
    /// 返回：禁用模式返回无监听对象；进程模式在收到确认后返回控制面，注册失败或确认通道关闭则返回启动错误。
    pub(crate) async fn start(
        mode: SignalMode,
        application: Application,
    ) -> ApplicationResult<Self> {
        match mode {
            SignalMode::Disabled => Ok(Self {
                receiver: None,
                stop: None,
                task: None,
            }),
            SignalMode::Process => {
                let (events, receiver) = mpsc::unbounded_channel();
                let (stop, stopped) = oneshot::channel();
                let (ready, registered) = oneshot::channel();
                let task = tokio::spawn(run_process_broker(application, events, stopped, ready));
                let registration = registered.await.map_err(|_| {
                    signal_error("signal broker exited before registration acknowledgement")
                })?;
                registration?;
                Ok(Self {
                    receiver: Some(receiver),
                    stop: Some(stop),
                    task: Some(task),
                })
            }
        }
    }

    /// 业务作用：等待下一次已注册信号；关闭模式下永久 pending，不制造虚假事件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：取得信号时返回 Received；发送侧关闭且缓存耗尽时返回 BrokerClosed；禁用或已停止时永久等待。
    pub(crate) async fn next(&mut self) -> SignalEvent {
        match self.receiver.as_mut() {
            Some(receiver) => receiver
                .recv()
                .await
                .map(SignalEvent::Received)
                .unwrap_or(SignalEvent::BrokerClosed),
            None => pending::<SignalEvent>().await,
        }
    }

    /// 业务作用：在最终状态和退出码已经确定后停止信号控制面。
    ///
    /// 该步骤必须位于异步退出路径最后，尽量缩短信号 handler 已注册但无人消费的同步尾窗。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：发送停止通知并等待持有的监听任务结束后丢弃接收端；忽略任务退出错误，完整执行后重复调用不再等待。
    pub(crate) async fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        self.receiver.take();
    }
}

/// 业务作用：注册平台信号并运行独立监听循环。
///
/// 参数说明：
///
/// - `application`：用于判断进程是否已经进入 Stopping 以及读取首次终态。
/// - `events`：把正常信号发送给 Runner 的无界控制通道；信号频率由操作系统限制，不承载业务数据。
/// - `stop`：Runner 最终退场时发送的单次停止通知。
/// - `ready`：平台 handler 注册结果的单次 ACK。
///
/// 返回：注册失败时向 ready 报错并结束；确认无法送达、停止通知或信号流结束时退出，Stopping 中再收到信号可直接终止进程。
#[cfg(unix)]
async fn run_process_broker(
    application: Application,
    events: mpsc::UnboundedSender<ShutdownSignal>,
    mut stop: oneshot::Receiver<()>,
    ready: oneshot::Sender<ApplicationResult<()>>,
) {
    use tokio::signal::unix::{signal, SignalKind};

    let mut interrupt = match signal(SignalKind::interrupt()) {
        Ok(signal) => signal,
        Err(error) => {
            let _ = ready.send(Err(ApplicationError::with_source(
                ComponentId::Application,
                ApplicationPhase::Bootstrap,
                "cannot register SIGINT handler",
                error,
            )));
            return;
        }
    };
    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(error) => {
            let _ = ready.send(Err(ApplicationError::with_source(
                ComponentId::Application,
                ApplicationPhase::Bootstrap,
                "cannot register SIGTERM handler",
                error,
            )));
            return;
        }
    };
    // 只有完整接管两个终止信号才允许 Runner 启动组件；确认接收者消失时不遗留监听循环。
    if ready.send(Ok(())).is_err() {
        return;
    }

    loop {
        tokio::select! {
            _ = &mut stop => break,
            received = interrupt.recv() => {
                if received.is_none() {
                    break;
                }
                dispatch_signal(&application, &events, ShutdownSignal::CtrlC);
            }
            received = terminate.recv() => {
                if received.is_none() {
                    break;
                }
                dispatch_signal(&application, &events, ShutdownSignal::Terminate);
            }
        }
    }
}

/// 业务作用：注册可移植的终止信号并运行独立监听循环。
///
/// 参数说明：
///
/// - `application`：用于判断进程是否已经进入 Stopping 以及读取首次终态。
/// - `events`：把正常信号发送给 Runner 的控制通道。
/// - `stop`：Runner 最终退场时发送的单次停止通知。
/// - `ready`：监听任务已启动的单次 ACK，不保证 ctrl_c 已被轮询注册。
///
/// 返回：确认失败、停止通知或 ctrl_c 返回错误时结束；成功接收信号则分发，停机期间可能直接终止进程。
#[cfg(not(unix))]
async fn run_process_broker(
    application: Application,
    events: mpsc::UnboundedSender<ShutdownSignal>,
    mut stop: oneshot::Receiver<()>,
    ready: oneshot::Sender<ApplicationResult<()>>,
) {
    if ready.send(Ok(())).is_err() {
        return;
    }
    loop {
        tokio::select! {
            _ = &mut stop => break,
            received = tokio::signal::ctrl_c() => {
                if received.is_err() {
                    break;
                }
                dispatch_signal(&application, &events, ShutdownSignal::CtrlC);
            }
        }
    }
}

/// 业务作用：根据公开状态把信号分流为正常 Runner 事件或停机中立即强退。
///
/// 参数说明：
///
/// - `application`：提供状态和首次终态的共享视图。
/// - `events`：正常阶段转交 Runner 的控制通道。
/// - `signal`：本次被平台 handler 观察到的信号。
///
/// 返回：非 Stopping 时尽力入队，接收侧关闭则忽略；Stopping 时同步退出进程，已完成批任务用零退出码，其它情况用信号码。
fn dispatch_signal(
    application: &Application,
    events: &mpsc::UnboundedSender<ShutdownSignal>,
    signal: ShutdownSignal,
) {
    // 清理期间再次收到信号代表强退意图，不再等待可能卡住的异步清理；已完成批任务保留成功退出语义。
    if application.state() == ApplicationState::Stopping {
        let intent = application.terminal_intent();
        let exit_code = if intent == TerminalIntent::BatchCompleted {
            0
        } else {
            signal.exit_code()
        };
        write_stderr(&format!("force exit on signal: {}\n", signal.name()));
        std::process::exit(i32::from(exit_code));
    }
    let _ = events.send(signal);
}

/// 业务作用：创建统一的信号控制面错误。
///
/// 参数说明：
///
/// - `message`：不包含业务配置值的稳定错误摘要。
///
/// 返回：归属 Application 的 Bootstrap 阶段错误，用于中止信号监听未就绪的启动流程。
fn signal_error(message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(
        ComponentId::Application,
        ApplicationPhase::Bootstrap,
        message,
    )
}
