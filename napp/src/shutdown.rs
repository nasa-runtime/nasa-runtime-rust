use std::time::{Duration, Instant};

/// 运行期统一识别的进程终止信号。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownSignal {
    /// 终端 Ctrl-C 或等价中断信号。
    CtrlC,
    /// 进程终止信号。
    Terminate,
    /// 显式中断信号。
    Interrupt,
}

impl ShutdownSignal {
    /// 业务作用：返回信号对应的进程退出码。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回值遵循常用的 `128 + signal number` 约定。
    pub fn exit_code(self) -> u8 {
        match self {
            Self::CtrlC | Self::Interrupt => 130,
            Self::Terminate => 143,
        }
    }

    /// 业务作用：返回适合固定诊断标记使用的信号名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回值不包含业务输入。
    pub fn name(self) -> &'static str {
        match self {
            Self::CtrlC => "SIGINT",
            Self::Terminate => "SIGTERM",
            Self::Interrupt => "SIGINT",
        }
    }
}

/// 传递给资源和 action 的首次停机原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShutdownReason {
    /// 管理入口或内部控制面主动请求停机。
    Requested,
    /// 操作系统信号触发停机。
    Signal(ShutdownSignal),
    /// Batch 应用正常完成。
    BatchCompleted,
    /// 关键受管任务异常退出。
    CriticalTaskFailed,
    /// 应用启动过程失败。
    StartupFailed,
    /// 内置组件在运行或停机阶段失败。
    ComponentFailed,
}

/// 传给资源和组件的统一停机预算。子步骤只能消费剩余时间，不能重置 deadline。
#[derive(Debug, Clone)]
pub struct ShutdownContext {
    deadline: Instant,
    reason: ShutdownReason,
}

impl ShutdownContext {
    /// 业务作用：创建共享同一绝对截止时间的停机上下文。
    ///
    /// # 参数
    ///
    /// - `deadline`：所有清理步骤都不能突破的绝对时间点。
    /// - `reason`：由首次终止意图映射出的稳定停机原因。
    pub fn new(deadline: Instant, reason: ShutdownReason) -> Self {
        Self { deadline, reason }
    }

    /// 业务作用：返回全局停机截止时间。
    ///
    /// # 参数
    ///
    /// 本方法无参数；子步骤不得据此创建新的完整预算。
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// 业务作用：返回首次停机原因。
    ///
    /// # 参数
    ///
    /// 本方法无参数；后续清理错误不会改变该值。
    pub fn reason(&self) -> &ShutdownReason {
        &self.reason
    }

    /// 业务作用：计算当前步骤还能消费的剩余预算。
    ///
    /// # 参数
    ///
    /// 本方法无参数；过期后饱和为零而不会回绕。
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// 业务作用：判断全局清理预算是否已经耗尽。
    ///
    /// # 参数
    ///
    /// 本方法无参数；结果来自同一绝对 deadline。
    pub fn is_expired(&self) -> bool {
        self.remaining().is_zero()
    }
}
