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
    Disabled,
    Process,
}

/// Runner 接收到的信号控制面事件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SignalEvent {
    Received(ShutdownSignal),
    BrokerClosed,
}

/// 独立于普通任务组的信号控制面，保证 active stack 清理期间仍能观察强退信号。
pub(crate) struct SignalBroker {
    receiver: Option<mpsc::UnboundedReceiver<ShutdownSignal>>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl SignalBroker {
    /// 按指定模式建立信号控制面，并等待平台 handler 完成注册。
    ///
    /// ready ACK 先于任何组件启动，避免启动卡住时存在尚未接管终止信号的窗口。
    ///
    /// # 参数
    ///
    /// - `mode`：决定关闭监听或注册真实进程信号。
    /// - `application`：提供公开状态和首次终态，供停机中的强退判定使用。
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

    /// 等待下一次已注册信号；关闭模式下永久 pending，不制造虚假事件。
    ///
    /// # 参数
    ///
    /// 本方法无参数；调用期间需要持续持有 Broker 的可变借用以保持单消费者语义。
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

    /// 在最终状态和退出码已经确定后停止信号控制面。
    ///
    /// 该步骤必须位于异步退出路径最后，尽量缩短信号 handler 已注册但无人消费的同步尾窗。
    ///
    /// # 参数
    ///
    /// 本方法无参数；重复调用安全，后续调用不会再次等待任务。
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

/// 注册平台信号并运行独立监听循环。
///
/// # 参数
///
/// - `application`：用于判断进程是否已经进入 Stopping 以及读取首次终态。
/// - `events`：把正常信号发送给 Runner 的无界控制通道；信号频率由操作系统限制，不承载业务数据。
/// - `stop`：Runner 最终退场时发送的单次停止通知。
/// - `ready`：平台 handler 注册结果的单次 ACK。
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

/// 注册可移植的终止信号并运行独立监听循环。
///
/// # 参数
///
/// - `application`：用于判断进程是否已经进入 Stopping 以及读取首次终态。
/// - `events`：把正常信号发送给 Runner 的控制通道。
/// - `stop`：Runner 最终退场时发送的单次停止通知。
/// - `ready`：handler 注册完成后的单次 ACK。
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

/// 根据公开状态把信号分流为正常 Runner 事件或停机中立即强退。
///
/// # 参数
///
/// - `application`：提供状态和首次终态的共享视图。
/// - `events`：正常阶段转交 Runner 的控制通道。
/// - `signal`：本次被平台 handler 观察到的信号。
fn dispatch_signal(
    application: &Application,
    events: &mpsc::UnboundedSender<ShutdownSignal>,
    signal: ShutdownSignal,
) {
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

/// 创建统一的信号控制面错误。
///
/// # 参数
///
/// - `message`：不包含业务配置值的稳定错误摘要。
fn signal_error(message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(
        ComponentId::Application,
        ApplicationPhase::Bootstrap,
        message,
    )
}
