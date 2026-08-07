//! Saga 实例的封闭状态机与正交控制状态。
//!
//! 状态集合与合法迁移在此**穷尽枚举**：持久化层的 CAS 语句只允许在该集合内推进，
//! 任何未列出的迁移都是不变量破坏。把状态机放在纯逻辑层，是为了让崩溃恢复、
//! 并发副本与人工介入三条路径共用同一套裁决，而不是各自实现一份近似规则。

use crate::error::{
    ContractResult, ContractViolation, CODE_TRANSITION_FORWARD_AFTER_COMPENSATION,
    CODE_TRANSITION_ILLEGAL,
};

/// 业务作用：表示 Saga 实例的业务状态；持久化 CAS 的 `AND status = ?` 只在本集合内迁移。
///
/// 分支说明：`Cancelling` 与 `WaitingResolution` 是安全性关键的中间态——前者表示“超时但结果未知，
/// 正在建立取消屏障”，后者表示“外部效果尚无确定结论”。缺少它们就只能在超时后直接补偿，
/// 从而让迟到生效的效果永久泄漏在已补偿的 Saga 之外。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SagaStatus {
    /// 正向执行中。
    Running,
    /// 步骤超时，正在通过取消屏障裁决真实结果；绝不在此状态直接补偿。
    Cancelling,
    /// 外部效果未知，正在通过查询、回调或对账取得唯一裁决。
    WaitingResolution,
    /// 已冻结补偿计划并按计划执行补偿。
    Compensating,
    /// 终态：全部正向步骤成功。
    Completed,
    /// 终态：冻结计划内全部补偿都已真实到达终态。
    Compensated,
    /// 停止自动推进，等待人工裁决；不伪造任何业务终态。
    ManualIntervention,
}

impl SagaStatus {
    /// 实例创建事务写入的唯一合法初始状态。
    pub const INITIAL: Self = Self::Running;

    /// 业务作用：返回状态的稳定文本名，用于持久化列、指标标签与管理接口。
    ///
    /// 该字符串在协议演进期间保持稳定，是运维查询与告警规则的契约。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：状态稳定名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Cancelling => "CANCELLING",
            Self::WaitingResolution => "WAITING_RESOLUTION",
            Self::Compensating => "COMPENSATING",
            Self::Completed => "COMPLETED",
            Self::Compensated => "COMPENSATED",
            Self::ManualIntervention => "MANUAL_INTERVENTION",
        }
    }

    /// 业务作用：把持久化列中的稳定文本解析回状态，是 `as_str` 的严格逆映射。
    ///
    /// 参数说明：
    /// - `raw`: 持久化读出的状态文本。
    ///
    /// 返回：识别成功返回对应状态；文本不在封闭集合内返回 `None`，
    /// 调用方必须按数据损坏处理而不是猜测语义。
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "RUNNING" => Some(Self::Running),
            "CANCELLING" => Some(Self::Cancelling),
            "WAITING_RESOLUTION" => Some(Self::WaitingResolution),
            "COMPENSATING" => Some(Self::Compensating),
            "COMPLETED" => Some(Self::Completed),
            "COMPENSATED" => Some(Self::Compensated),
            "MANUAL_INTERVENTION" => Some(Self::ManualIntervention),
            _ => None,
        }
    }

    /// 业务作用：判断状态是否为不可再迁移的终态，用于清理策略与管理面操作门禁。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`Completed` 与 `Compensated` 返回真。
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Compensated)
    }

    /// 业务作用：判断该状态下是否仍允许自动发出新的业务命令。
    ///
    /// `ManualIntervention` 停止发出新命令，但 result consumer 仍必须运行以接收在途迟到结果；
    /// 拒收迟到结果会丢失“退款已经发生”这类事实，并在恢复时造成重复补偿。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：允许自动发出新命令时返回真。
    pub fn allows_automatic_commands(self) -> bool {
        matches!(
            self,
            Self::Running | Self::Cancelling | Self::WaitingResolution | Self::Compensating
        )
    }

    /// 业务作用：判断从当前状态到目标状态的迁移是否属于封闭状态机允许的边。
    ///
    /// 本函数只裁决结构性合法性，不检查业务前置条件（如是否已发生补偿副作用），
    /// 后者由 [`check_transition`] 在同一处统一施加。
    ///
    /// 参数说明：
    /// - `to`: 目标状态。
    ///
    /// 返回：迁移在允许集合内时返回真。
    pub fn can_transition_to(self, to: Self) -> bool {
        match (self, to) {
            // 同态推进边：四个自动段内的"步骤推进"（下一正向步骤、下一补偿步骤、
            // 取消/解决的新 attempt）也必须以 CAS + transition 审计行的事务模式落库
            // （见 11.5.4"以相同事务模式推进下一补偿步骤"），因此 from == to 在这
            // 四个状态内是合法边；终态与人工介入没有自动推进，不开放同态边。
            (Self::Running, Self::Running)
            | (Self::Cancelling, Self::Cancelling)
            | (Self::WaitingResolution, Self::WaitingResolution)
            | (Self::Compensating, Self::Compensating) => true,

            // 正向段：成功收敛、确定性拒绝进补偿、超时先进取消屏障、未知进解决通道。
            (Self::Running, Self::Completed)
            | (Self::Running, Self::Compensating)
            | (Self::Running, Self::Cancelling)
            | (Self::Running, Self::WaitingResolution) => true,

            // 取消屏障段：裁决为未决则继续解决；确认成功可回正向或直接完成；
            // 确认取消或按策略补偿则进入补偿。
            (Self::Cancelling, Self::WaitingResolution)
            | (Self::Cancelling, Self::Running)
            | (Self::Cancelling, Self::Completed)
            | (Self::Cancelling, Self::Compensating) => true,

            // 解决段：查询确认成功可继续正向或完成；确认失败或按策略进补偿；预算耗尽转人工。
            (Self::WaitingResolution, Self::Running)
            | (Self::WaitingResolution, Self::Completed)
            | (Self::WaitingResolution, Self::Compensating) => true,

            // 补偿段：补偿自身的外部效果未知时同样进入解决通道，方向仍保持补偿。
            (Self::Compensating, Self::WaitingResolution)
            | (Self::Compensating, Self::Compensated) => true,

            // 任一自动段都可以在冻结、重试耗尽或不变量破坏时升级人工介入。
            (Self::Running, Self::ManualIntervention)
            | (Self::Cancelling, Self::ManualIntervention)
            | (Self::WaitingResolution, Self::ManualIntervention)
            | (Self::Compensating, Self::ManualIntervention) => true,

            // 人工裁决只能回到受控的自动段，且仍走同一套 CAS 与审计。
            (Self::ManualIntervention, Self::Compensating)
            | (Self::ManualIntervention, Self::Running)
            | (Self::ManualIntervention, Self::WaitingResolution) => true,

            // 终态不可再迁移；其余组合一律视为不变量破坏。
            _ => false,
        }
    }
}

/// 业务作用：表示实例当前处于正向推进还是补偿推进，与状态正交。
///
/// `WaitingResolution` 在两个方向上都可能出现——方向决定解决结果应当驱动“继续正向”
/// 还是“继续补偿”，因此不能从状态反推，必须独立持久化。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// 正向推进。
    Forward,
    /// 补偿推进。
    Compensating,
}

impl Direction {
    /// 业务作用：返回方向的稳定文本名，用于持久化列与指标标签。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：方向稳定名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Forward => "FORWARD",
            Self::Compensating => "COMPENSATING",
        }
    }

    /// 业务作用：把持久化列中的稳定文本解析回方向，是 `as_str` 的严格逆映射。
    ///
    /// 参数说明：
    /// - `raw`: 持久化读出的方向文本。
    ///
    /// 返回：识别成功返回对应方向；文本不在词汇表内返回 `None`，调用方按数据损坏处理。
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "FORWARD" => Some(Self::Forward),
            "COMPENSATING" => Some(Self::Compensating),
            _ => None,
        }
    }
}

/// 业务作用：表示管理面暂停开关，与业务状态正交，保证暂停不丢失暂停前的业务状态。
///
/// 把 `PAUSED` 混进业务状态机会让恢复时无法还原原状态，只能猜测；因此它必须是独立列。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ControlState {
    /// 正常自动推进。
    Active,
    /// 已暂停：只停止发出新的自动动作，不停止业务逻辑时钟。
    Paused,
}

impl ControlState {
    /// 业务作用：返回控制状态的稳定文本名，用于持久化列与管理接口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：控制状态稳定名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Paused => "PAUSED",
        }
    }

    /// 业务作用：把持久化列中的稳定文本解析回控制状态，是 `as_str` 的严格逆映射。
    ///
    /// 参数说明：
    /// - `raw`: 持久化读出的控制状态文本。
    ///
    /// 返回：识别成功返回对应控制状态；文本不在词汇表内返回 `None`，调用方按数据损坏处理。
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "ACTIVE" => Some(Self::Active),
            "PAUSED" => Some(Self::Paused),
            _ => None,
        }
    }

    /// 业务作用：判断当前是否允许发出新的自动动作。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：处于 `Active` 时返回真。
    pub fn allows_automatic_actions(self) -> bool {
        matches!(self, Self::Active)
    }
}

/// 业务作用：承载状态迁移的业务前置条件，使结构性合法与业务合法在同一处裁决。
///
/// 字段说明：`any_compensation_succeeded` 表示本实例是否已经真实发生过补偿副作用，
/// 它决定人工裁决能否把实例送回正向执行。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransitionGuard {
    /// 是否已有任一步骤补偿成功。
    pub any_compensation_succeeded: bool,
}

impl TransitionGuard {
    /// 业务作用：构造一次迁移的业务前置条件快照。
    ///
    /// 参数说明：
    /// - `any_compensation_succeeded`: 本实例是否已经发生过补偿副作用。
    ///
    /// 返回：可传入 [`check_transition`] 的前置条件值。
    pub fn new(any_compensation_succeeded: bool) -> Self {
        Self {
            any_compensation_succeeded,
        }
    }
}

/// 业务作用：在写入任何状态变更之前，统一裁决一次迁移是否同时满足结构性与业务前置条件。
///
/// 参数说明：
/// - `from`: 迁移前的已提交状态，必须来自持久化读取而不是内存快照。
/// - `to`: 目标状态。
/// - `guard`: 由已提交 journal 推导出的业务前置条件。
///
/// 返回：允许迁移时返回成功；迁移不在封闭集合内，或试图在已发生补偿副作用后恢复正向执行时，
/// 返回带稳定原因码的合同违规，调用方必须保持原状态并转人工或重新读取裁决。
pub fn check_transition(
    from: SagaStatus,
    to: SagaStatus,
    guard: TransitionGuard,
) -> ContractResult<()> {
    if !from.can_transition_to(to) {
        return Err(ContractViolation::new(
            CODE_TRANSITION_ILLEGAL,
            format!(
                "transition {} -> {} is not in the closed state machine",
                from.as_str(),
                to.as_str()
            ),
        ));
    }

    // 补偿一旦真实发生，上游效果已经被撤销；此时恢复正向执行会在“部分已回滚”的
    // 数据上继续推进，产生既非完成也非补偿的第三种终态。只允许重建计划继续补偿
    // 或保持人工介入。
    if from == SagaStatus::ManualIntervention
        && to == SagaStatus::Running
        && guard.any_compensation_succeeded
    {
        return Err(ContractViolation::new(
            CODE_TRANSITION_FORWARD_AFTER_COMPENSATION,
            "cannot resume forward execution after a compensation has already succeeded",
        ));
    }

    Ok(())
}
