//! Runner 停止结果与公开错误。

use std::fmt;

/// 停机损耗与退出证明摘要。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShutdownReport {
    /// 停机开始后仍正常执行完成的任务数。
    pub drained: u64,
    /// 停机阶段物理跨过的已取消任务数。
    pub cancelled: u64,
    /// 有损停止中止的执行中任务数。
    pub aborted: u64,
    /// 未执行且因无法继续安全推进而冻结的任务数。
    pub frozen: u64,
    /// 未在无损预算内排空的类型或兼容统计方向数。
    pub timed_out_lanes: u32,
    /// 未取得退出证明的严格类型数。
    pub unconverged_strict_types: u32,
    /// 未取得退出证明的 slot 数。
    pub unconverged_slots: u32,
    /// 未取得退出证明的 timer 数。
    pub unconverged_timers: u32,
}

/// Runner 启动失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StartError {
    /// 当前线程没有可用 Tokio runtime。
    RuntimeUnavailable,
    /// Runner 已经启动或正在启动。
    AlreadyStarted,
    /// 上一 generation 尚未取得完整停止证明。
    PreviousGenerationActive,
    /// worker、timer、observer 或 supervisor 无法建立唯一控制权。
    ControlPlaneUnavailable,
}

impl fmt::Display for StartError {
    /// 业务作用：把启动拒绝格式化为稳定文本，供配置面和 Application 错误链展示。
    ///
    /// 参数说明：
    /// - `f`: 接收稳定启动拒绝文本的格式化器。
    ///
    /// 返回：写入成功返回 Ok；底层写入失败返回格式错误。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::RuntimeUnavailable => "Tokio runtime is unavailable",
            Self::AlreadyStarted => "partition runner is already started",
            Self::PreviousGenerationActive => "previous partition runner generation is active",
            Self::ControlPlaneUnavailable => "partition runner control plane is unavailable",
        };
        f.write_str(text)
    }
}

impl std::error::Error for StartError {}

/// 无损停止没有在调用方期限内取得完整退出证明。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopError {
    /// operation 仍在后台持续收口；后续 `stop` 或 `force_stop` 会复用同一动作。
    NotConverged,
}

impl fmt::Display for StopError {
    /// 业务作用：输出无损停止缺少退出证明的稳定错误文本。
    ///
    /// 参数说明：
    /// - `f`: 接收无损停止错误文本的格式化器。
    ///
    /// 返回：写入成功返回 Ok；底层写入失败返回格式错误。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("partition runner did not converge before the deadline")
    }
}

impl std::error::Error for StopError {}

/// 显式有损停止没有在调用方期限内取得完整退出证明。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForceStopError {
    /// 中止请求已发布，但仍有 Future 或内部任务尚未真实退出。
    NotConverged,
}

impl fmt::Display for ForceStopError {
    /// 业务作用：输出有损停止缺少退出证明的稳定错误文本。
    ///
    /// 参数说明：
    /// - `f`: 接收有损停止错误文本的格式化器。
    ///
    /// 返回：写入成功返回 Ok；底层写入失败返回格式错误。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("forced partition runner stop did not converge before the deadline")
    }
}

impl std::error::Error for ForceStopError {}
