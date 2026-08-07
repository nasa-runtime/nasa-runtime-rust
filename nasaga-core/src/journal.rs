//! Orchestrator step journal 与 transition journal 的状态词汇。
//!
//! 这些枚举是持久化列、审计与管理接口共用的**稳定词汇表**：字符串一经发布不得修改，
//! 解析函数与 `as_str` 必须严格互逆。把词汇放在纯逻辑层而不是某个具体 store，
//! 是为了让 MySQL store、Runtime 与管理面共用同一份映射，杜绝两处映射各自漂移后
//! 把同一份持久化事实解读成两种状态。

/// 业务作用：表示步骤取消屏障阶段在 journal 中的裁决投影。
///
/// 分支说明：三个非 `None` 分支正是取消屏障协议的三种合法裁决——确认未执行、
/// 已有确定终态、结果未知不谎报取消成功。缺少 `ResolutionPending` 分支的实现
/// 只能把"取消了但结果未知"谎报成成功，从而让迟到生效的效果泄漏。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StepCancelStatus {
    /// 尚未发出取消或取消不适用。
    None,
    /// 参与方确认执行从未开始，已建立 admission fence，零正向效果。
    Confirmed,
    /// 执行已有确定终态，取消无从谈起；真实终态另由 forward 状态记账。
    AlreadyTerminal,
    /// 已有外部 intent 或调用在途，结果未知；必须进入解决通道而不是宣称取消成功。
    ResolutionPending,
}

impl StepCancelStatus {
    /// 业务作用：返回状态的稳定文本名，用于持久化列、审计与管理接口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：取消裁决稳定名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Confirmed => "CONFIRMED",
            Self::AlreadyTerminal => "ALREADY_TERMINAL",
            Self::ResolutionPending => "RESOLUTION_PENDING",
        }
    }

    /// 业务作用：把持久化列中的稳定文本解析回枚举，是 `as_str` 的严格逆映射。
    ///
    /// 参数说明：
    /// - `raw`: 持久化读出的状态文本。
    ///
    /// 返回：识别成功返回对应状态；文本不在词汇表内返回 `None`，
    /// 调用方必须按数据损坏处理而不是猜测语义。
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "NONE" => Some(Self::None),
            "CONFIRMED" => Some(Self::Confirmed),
            "ALREADY_TERMINAL" => Some(Self::AlreadyTerminal),
            "RESOLUTION_PENDING" => Some(Self::ResolutionPending),
            _ => None,
        }
    }
}

/// 业务作用：表示步骤补偿阶段在 journal 中的投影状态。
///
/// 分支说明：**故意没有 `Rejected` 分支**——补偿不允许被业务拒绝,只能成功、未知或冻结待人工,
/// 与 [`CompensationOutcome`](crate::CompensationOutcome) 的封闭结构保持一致。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StepCompensationStatus {
    /// 不需要补偿或尚未纳入冻结计划。
    None,
    /// 已纳入冻结计划，等待或正在执行补偿。
    Pending,
    /// 补偿已真实到达终态。
    Succeeded,
    /// 补偿的外部效果未知，等待解决通道裁决。
    Unknown,
    /// 合同违规或重试耗尽，已冻结等待人工裁决。
    Halted,
}

impl StepCompensationStatus {
    /// 业务作用：返回状态的稳定文本名，用于持久化列、审计与管理接口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：补偿状态稳定名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Pending => "PENDING",
            Self::Succeeded => "SUCCEEDED",
            Self::Unknown => "UNKNOWN",
            Self::Halted => "HALTED",
        }
    }

    /// 业务作用：把持久化列中的稳定文本解析回枚举，是 `as_str` 的严格逆映射。
    ///
    /// 参数说明：
    /// - `raw`: 持久化读出的状态文本。
    ///
    /// 返回：识别成功返回对应状态；文本不在词汇表内返回 `None`，调用方按数据损坏处理。
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "NONE" => Some(Self::None),
            "PENDING" => Some(Self::Pending),
            "SUCCEEDED" => Some(Self::Succeeded),
            "UNKNOWN" => Some(Self::Unknown),
            "HALTED" => Some(Self::Halted),
            _ => None,
        }
    }
}

/// 业务作用：表示步骤解决（resolve）阶段在 journal 中的投影状态。
///
/// 解决阶段的职责是把 `Unknown` 收敛成唯一裁决：查询、回调或对账确认真实结果。
/// `Succeeded`/`Rejected` 指被裁决的那个业务效果的真实终态，不是解决动作本身的成败。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StepResolutionStatus {
    /// 无需解决或尚未进入解决通道。
    None,
    /// 已发出查询/对账，等待唯一裁决。
    Pending,
    /// 裁决：效果已真实发生。
    Succeeded,
    /// 裁决：效果确定未发生，且已按供应商能力排除迟到生效。
    Rejected,
    /// 解决预算耗尽或裁决本身失败，已冻结等待人工。
    Halted,
}

impl StepResolutionStatus {
    /// 业务作用：返回状态的稳定文本名，用于持久化列、审计与管理接口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：解决状态稳定名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Pending => "PENDING",
            Self::Succeeded => "SUCCEEDED",
            Self::Rejected => "REJECTED",
            Self::Halted => "HALTED",
        }
    }

    /// 业务作用：把持久化列中的稳定文本解析回枚举，是 `as_str` 的严格逆映射。
    ///
    /// 参数说明：
    /// - `raw`: 持久化读出的状态文本。
    ///
    /// 返回：识别成功返回对应状态；文本不在词汇表内返回 `None`，调用方按数据损坏处理。
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "NONE" => Some(Self::None),
            "PENDING" => Some(Self::Pending),
            "SUCCEEDED" => Some(Self::Succeeded),
            "REJECTED" => Some(Self::Rejected),
            "HALTED" => Some(Self::Halted),
            _ => None,
        }
    }
}

/// 业务作用：表示 attempt journal 中单次尝试的生命周期状态。
///
/// attempt journal 是"每条投递命令全局去重"（`UNIQUE(command_id)`）与真实 outcome 留证的
/// 统一事实源，四个 phase 共用同一张 journal，因此状态词汇是四个 phase 终态的并集：
/// `Succeeded/Rejected/Unknown/Halted` 覆盖 execute/compensate/resolve，
/// `CancelConfirmed/AlreadyTerminal/ResolutionPending` 覆盖取消屏障的三种裁决。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StepAttemptStatus {
    /// 命令已写入 Outbox，等待参与方结果。
    Started,
    /// 参与方报告业务效果成功。
    Succeeded,
    /// 参与方报告确定性拒绝，无正向效果。
    Rejected,
    /// 参与方报告外部效果未知。
    Unknown,
    /// 参与方报告合同违规冻结。
    Halted,
    /// 取消屏障裁决：执行从未开始，已建立 admission fence。
    CancelConfirmed,
    /// 取消屏障裁决：执行已有确定终态。
    AlreadyTerminal,
    /// 取消屏障裁决：结果未知，需进入解决通道。
    ResolutionPending,
}

impl StepAttemptStatus {
    /// 业务作用：返回状态的稳定文本名，用于持久化列、审计与管理接口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尝试状态稳定名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Started => "STARTED",
            Self::Succeeded => "SUCCEEDED",
            Self::Rejected => "REJECTED",
            Self::Unknown => "UNKNOWN",
            Self::Halted => "HALTED",
            Self::CancelConfirmed => "CANCEL_CONFIRMED",
            Self::AlreadyTerminal => "ALREADY_TERMINAL",
            Self::ResolutionPending => "RESOLUTION_PENDING",
        }
    }

    /// 业务作用：把持久化列中的稳定文本解析回枚举，是 `as_str` 的严格逆映射。
    ///
    /// 参数说明：
    /// - `raw`: 持久化读出的状态文本。
    ///
    /// 返回：识别成功返回对应状态；文本不在词汇表内返回 `None`，调用方按数据损坏处理。
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "STARTED" => Some(Self::Started),
            "SUCCEEDED" => Some(Self::Succeeded),
            "REJECTED" => Some(Self::Rejected),
            "UNKNOWN" => Some(Self::Unknown),
            "HALTED" => Some(Self::Halted),
            "CANCEL_CONFIRMED" => Some(Self::CancelConfirmed),
            "ALREADY_TERMINAL" => Some(Self::AlreadyTerminal),
            "RESOLUTION_PENDING" => Some(Self::ResolutionPending),
            _ => None,
        }
    }

    /// 业务作用：判断该状态是否仍在等待结果，用于识别"在途命令"。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`Started` 返回真，其余均为已裁决状态。
    pub fn is_in_flight(self) -> bool {
        matches!(self, Self::Started)
    }
}

/// 业务作用：区分驱动状态迁移的触发来源，隔离不同 ID 命名空间。
///
/// 同一个字符串在 event、timer 与管理 operation 三个命名空间里可以合法重复；
/// 不区分来源就无法建立 `UNIQUE(saga_id, trigger_kind, trigger_id)` 的
/// "同一触发在每个 Saga 内只推进一次"约束。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TriggerKind {
    /// 由业务 result 事件触发，`trigger_id` 是 event id。
    Event,
    /// 由 durable timer 到期触发，`trigger_id` 是 timer id。
    Timer,
    /// 由管理面操作触发，`trigger_id` 是稳定 operation id。
    Admin,
}

impl TriggerKind {
    /// 业务作用：返回触发来源的稳定文本名，用于持久化列与审计。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：触发来源稳定名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Event => "EVENT",
            Self::Timer => "TIMER",
            Self::Admin => "ADMIN",
        }
    }

    /// 业务作用：把持久化列中的稳定文本解析回枚举，是 `as_str` 的严格逆映射。
    ///
    /// 参数说明：
    /// - `raw`: 持久化读出的触发来源文本。
    ///
    /// 返回：识别成功返回对应来源；文本不在词汇表内返回 `None`，调用方按数据损坏处理。
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "EVENT" => Some(Self::Event),
            "TIMER" => Some(Self::Timer),
            "ADMIN" => Some(Self::Admin),
            _ => None,
        }
    }
}
