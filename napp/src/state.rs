use std::sync::atomic::{AtomicU8, Ordering};

use tokio::sync::watch;

use crate::{ApplicationError, ApplicationPhase, ApplicationResult, ComponentId};

/// 供健康检查和外部观察者读取的公开应用生命周期状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ApplicationState {
    /// 应用正在构造配置、资源和组件。
    Starting = 0,
    /// 应用已完成探针并可接收业务流量。
    Ready = 1,
    /// 应用正在摘流和释放资源。
    Stopping = 2,
    /// 应用已经完成清理。
    Stopped = 3,
    /// 启动或运行阶段发生不可恢复失败。
    Failed = 4,
}

impl ApplicationState {
    /// 业务作用：把原子存储值保守映射为公开状态。
    ///
    /// # 参数
    ///
    /// - `value`：StateCell 中读取的原始整数；未知值按 Failed 处理。
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Starting,
            1 => Self::Ready,
            2 => Self::Stopping,
            3 => Self::Stopped,
            _ => Self::Failed,
        }
    }
}

/// preflight 已固定的进程生命周期模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationMode {
    /// 持续运行并等待停机信号的服务进程。
    Service,
    /// 完成有限任务后主动退出的批处理进程。
    Batch,
}

/// 使用原子值发布公开生命周期状态的共享单元。
pub(crate) struct StateCell {
    value: AtomicU8,
    changes: watch::Sender<ApplicationState>,
}

impl StateCell {
    /// 业务作用：创建初始为 Starting 的状态单元，并建立不会丢失最新状态的进程内通知通道。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：状态只允许由 Runner 推进、观察者只能订阅的共享单元。
    pub(crate) fn new() -> Self {
        let (changes, _receiver) = watch::channel(ApplicationState::Starting);
        Self {
            value: AtomicU8::new(ApplicationState::Starting as u8),
            changes,
        }
    }

    /// 业务作用：以 Acquire 语义读取当前公开状态。
    ///
    /// # 参数
    ///
    /// 本方法无参数；该顺序与首次终态的 Release 提交配对。
    pub(crate) fn load(&self) -> ApplicationState {
        ApplicationState::from_u8(self.value.load(Ordering::Acquire))
    }

    /// 业务作用：以 CAS 执行唯一合法状态转换，并在成功后唤醒等待开放或停机边界的受管任务。
    ///
    /// 参数说明：
    /// - `expected`：调用方要求的唯一前置状态。
    /// - `next`：转换成功后发布的新状态。
    ///
    /// 返回：CAS 成功且最新状态已经发布时完成；前置状态不匹配返回统一生命周期错误。
    pub(crate) fn transition(
        &self,
        expected: ApplicationState,
        next: ApplicationState,
    ) -> ApplicationResult<()> {
        self.value
            .compare_exchange(
                expected as u8,
                next as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|actual| {
                ApplicationError::new(
                    ComponentId::Application,
                    ApplicationPhase::Running,
                    format!(
                        "invalid state transition: expected {expected:?}, actual {:?}, next {next:?}",
                        ApplicationState::from_u8(actual)
                    ),
                )
            })?;
        self.changes.send_replace(next);
        Ok(())
    }

    #[cfg(feature = "outbox")]
    /// 业务作用：订阅应用状态转换，供受管任务等待 Ready 或立即响应停机，而不是按固定周期猜测。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：初值等于调用时最新状态的接收端，后续转换以 watch 代际通知。
    pub(crate) fn subscribe(&self) -> watch::Receiver<ApplicationState> {
        self.changes.subscribe()
    }
}

/// Runner 首次提交的终止意图，后到事件只能进入次要报告。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum TerminalIntent {
    Undecided = 0,
    BatchCompleted = 1,
    ServiceStop = 2,
    BatchInterrupted = 3,
    Failure = 4,
}

/// 保存 first-trigger-wins 终止意图的原子单元。
pub(crate) struct TerminalCell {
    value: AtomicU8,
}

impl TerminalCell {
    /// 业务作用：创建尚未提交终止意图的单元。
    ///
    /// # 参数
    ///
    /// 本方法无参数；初值为 Undecided。
    pub(crate) fn new() -> Self {
        Self {
            value: AtomicU8::new(TerminalIntent::Undecided as u8),
        }
    }

    /// 业务作用：尝试提交首次终止意图；返回 false 表示已有事件获胜。
    ///
    /// # 参数
    ///
    /// - `intent`：Runner 已分类的非 Undecided 意图。
    pub(crate) fn try_set(&self, intent: TerminalIntent) -> bool {
        debug_assert_ne!(intent, TerminalIntent::Undecided);
        self.value
            .compare_exchange(
                TerminalIntent::Undecided as u8,
                intent as u8,
                Ordering::Release,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// 业务作用：以 Acquire 语义读取已经提交的终止意图。
    ///
    /// # 参数
    ///
    /// 本方法无参数；未知原始值按 Undecided 保守处理。
    pub(crate) fn load(&self) -> TerminalIntent {
        match self.value.load(Ordering::Acquire) {
            1 => TerminalIntent::BatchCompleted,
            2 => TerminalIntent::ServiceStop,
            3 => TerminalIntent::BatchInterrupted,
            4 => TerminalIntent::Failure,
            _ => TerminalIntent::Undecided,
        }
    }
}
