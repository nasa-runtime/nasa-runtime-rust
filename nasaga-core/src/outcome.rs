//! Saga 结果分类。
//!
//! 这里区分的是**可提交的处理结论**与**必须回滚的执行故障**：
//! `Succeeded/Rejected/Unknown/Halted` 都是必须与 Inbox、结果事件、Audit 一起原子提交的事实；
//! 只有 `Retryable` 表示本次尝试没有得到可提交结果，必须回滚且不得 ACK。
//! 类型层面的这条分界，替代了“见 `Err` 即补偿”的错误语义。

/// 业务作用：表示正向步骤一次执行得到的可提交处理结论。
///
/// 分支说明：`Succeeded` 携带业务成功产物；`Rejected` 是确定性业务拒绝且承诺没有需要撤销的
/// 正向效果；`Unknown` 表示远端效果仍未确定，不是确定裁决；`Halted` 表示不变量破坏需冻结。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SagaOutcome<T> {
    /// 步骤成功，产物随结果事件提交并进入后续补偿计划的候选集合。
    Succeeded(T),
    /// 确定性业务拒绝；只允许提交拒绝状态、原因码与审计事实，不得留下待撤销的部分写。
    Rejected {
        /// 稳定拒绝原因码，可安全进入指标标签。
        code: &'static str,
    },
    /// 外部效果结果未知；必须进入查询、回调或对账通道，绝不能自动降级为拒绝。
    Unknown {
        /// 稳定未知原因码。
        code: &'static str,
    },
    /// 检测到不变量破坏，冻结自动推进并告警，是否补偿需要显式裁决。
    Halted {
        /// 稳定冻结原因码。
        code: &'static str,
    },
}

impl<T> SagaOutcome<T> {
    /// 业务作用：判断该结论是否为“可继续正向推进”的成功，决定 Orchestrator 是否走下一步。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅 `Succeeded` 返回真。
    pub fn is_succeeded(&self) -> bool {
        matches!(self, Self::Succeeded(_))
    }

    /// 业务作用：判断该结论是否已经得到确定终态，用于取消屏障裁决与补偿计划冻结前置检查。
    ///
    /// `Unknown` 不是终态：它只表明尚未获得裁决，必须继续解决而不是据此推进。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`Succeeded`、`Rejected`、`Halted` 返回真，`Unknown` 返回假。
    pub fn is_terminal(&self) -> bool {
        !matches!(self, Self::Unknown { .. })
    }

    /// 业务作用：读取稳定原因码，用于指标标签、告警路由与审计留证。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：非成功分支返回其原因码；`Succeeded` 没有原因码时返回空。
    pub fn code(&self) -> Option<&'static str> {
        match self {
            Self::Succeeded(_) => None,
            Self::Rejected { code } | Self::Unknown { code } | Self::Halted { code } => Some(code),
        }
    }
}

/// 业务作用：表示补偿步骤一次执行得到的可提交处理结论。
///
/// 这里**故意没有 `Rejected` 分支**：补偿被“拒绝”不能再触发一轮上游补偿，
/// 业务无法完成补偿时必须显式冻结并转人工，而不是把失败伪装成一次正常业务裁决。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompensationOutcome {
    /// 补偿完成，或此前已完成（幂等重入）。
    Succeeded,
    /// 补偿的外部效果未知，需要查询或对账后才能确认。
    Unknown {
        /// 稳定未知原因码。
        code: &'static str,
    },
    /// 补偿无法安全完成，冻结 Saga 并进入人工介入。
    Halted {
        /// 稳定冻结原因码。
        code: &'static str,
    },
}

impl CompensationOutcome {
    /// 业务作用：判断补偿是否已经确认完成，决定 Orchestrator 是否推进下一条补偿。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅 `Succeeded` 返回真。
    pub fn is_succeeded(&self) -> bool {
        matches!(self, Self::Succeeded)
    }

    /// 业务作用：读取稳定原因码，用于指标、告警与人工介入工单。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：非成功分支返回其原因码；`Succeeded` 返回空。
    pub fn code(&self) -> Option<&'static str> {
        match self {
            Self::Succeeded => None,
            Self::Unknown { code } | Self::Halted { code } => Some(code),
        }
    }
}

/// 业务作用：表示本次尝试没有得到任何可提交结论的执行故障。
///
/// 它必须导致本地事务回滚且不 ACK 输入消息；重试耗尽只进入 DLT 与告警，
/// **不得**被当作领域失败去驱动补偿——事务已经回滚，没有可证明的领域终态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SagaExecutionError {
    /// 临时故障，按 definition 的重试策略重投。
    Retryable {
        /// 稳定故障原因码。
        code: &'static str,
    },
}

impl SagaExecutionError {
    /// 业务作用：读取稳定故障原因码，用于重试指标与退避策略选择。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：故障原因码。
    pub fn code(&self) -> &'static str {
        match self {
            Self::Retryable { code } => code,
        }
    }
}

impl std::fmt::Display for SagaExecutionError {
    /// 业务作用：输出稳定原因码，保证错误链进入日志时不携带业务负载或外部错误原文。
    ///
    /// 参数说明：
    /// - `formatter`: 标准库格式化器。
    ///
    /// 返回：格式化成功返回 `Ok`；写入失败时透传格式化错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retryable { code } => write!(formatter, "saga retryable execution error: {code}"),
        }
    }
}

impl std::error::Error for SagaExecutionError {}

/// 业务作用：正向步骤 handler 的返回类型，把“可提交结论”与“必须回滚的故障”在类型层面分开。
pub type SagaResult<T> = Result<SagaOutcome<T>, SagaExecutionError>;

/// 业务作用：补偿 handler 的返回类型；`Ok` 侧不含拒绝分支，杜绝“拒绝补偿”这一非法语义。
pub type CompensationResult = Result<CompensationOutcome, SagaExecutionError>;

/// 业务作用：表示取消屏障裁决时，正向执行已经到达的确定终态。
///
/// 分支说明：与 [`SagaOutcome`] 的终态子集一一对应，但不携带成功产物——
/// 屏障只需要知道“已经发生了什么”，产物由此前提交的结果事件负责传递。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalForwardOutcome {
    /// 正向执行已成功，效果真实存在。
    Succeeded,
    /// 正向执行已被确定性拒绝，没有需要撤销的效果。
    Rejected {
        /// 稳定拒绝原因码。
        code: &'static str,
    },
    /// 正向执行已冻结，需人工裁决。
    Halted {
        /// 稳定冻结原因码。
        code: &'static str,
    },
}

/// 业务作用：表示参与方对一次 `CancelStep` 的裁决结果，是超时进入补偿前唯一合法的证据来源。
///
/// 分支说明：三个分支互斥且穷尽——要么本次取消成功建立准入屏障（此后 execute 零效果），
/// 要么正向执行已有确定终态，要么效果未知只能继续解决；框架**绝不允许**对外部步骤伪造
/// `CancelConfirmed`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// 取消成功：正向执行尚未开始且已建立准入屏障，该步骤不纳入补偿集合。
    CancelConfirmed,
    /// 正向执行已经有确定终态，按真实终态记账而不是按取消记账。
    AlreadyTerminal(TerminalForwardOutcome),
    /// 已提交外部 intent、调用在途或结果未知；不谎报取消成功，转入解决通道。
    ResolutionPending {
        /// 稳定未决原因码。
        code: &'static str,
    },
}

impl CancelOutcome {
    /// 业务作用：判断该裁决是否已经允许 Orchestrator 冻结补偿计划并进入补偿。
    ///
    /// `ResolutionPending` 必须返回假：此时远端效果可能仍会迟到生效，
    /// 直接补偿会让效果永久泄漏在“已补偿”的 Saga 之外。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：取消已确认或正向已有确定终态时返回真；仍需解决时返回假。
    pub fn is_resolved(&self) -> bool {
        !matches!(self, Self::ResolutionPending { .. })
    }
}

/// 业务作用：表示补偿 handler 的类型化返回值别名，供适配器在阶段间统一处理。
pub type CancelResult = Result<CancelOutcome, SagaExecutionError>;
