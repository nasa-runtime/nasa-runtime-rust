//! 补偿计划的冻结与查询。
//!
//! 进入补偿之前必须一次性**冻结**补偿计划：把当时已确定的事实固化为一个带摘要的有序集合，
//! 之后只允许幂等完成计划内步骤。缺少冻结语义时，补偿执行到一半的迟到事件会动态改变
//! 补偿顺序，可能颠倒依赖并造成“先退款后解冻库存”式的错序撤销。

use std::collections::BTreeSet;

use sha2::{Digest, Sha256};

use crate::definition::WorkflowDefinition;
use crate::error::{
    ContractResult, ContractViolation, CODE_PLAN_DUPLICATE_STEP, CODE_PLAN_MISSING_STEP,
    CODE_PLAN_SUCCEEDED_PIVOT, CODE_PLAN_UNKNOWN_STEP, CODE_PLAN_UNSETTLED_STEP,
};
use crate::identity::{canonical_bytes, StepName};

/// 业务作用：表示某个步骤正向阶段在 journal 中的已提交事实。
///
/// 分支说明：`Unknown`、`TimedOut` 表示结果尚未裁决，`Halted` 表示不变量破坏；
/// 三者都不允许直接参与补偿计划冻结，必须先由解决通道或人工裁决收敛。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepForwardStatus {
    /// 尚未发出或尚未产生任何效果。
    Pending,
    /// 已确认成功，效果真实存在。
    Succeeded,
    /// 已确定性拒绝，没有需要撤销的正向效果。
    Rejected,
    /// 已通过取消屏障确认未开始执行，零正向效果。
    Cancelled,
    /// 结果未知，等待查询、回调或对账裁决。
    Unknown,
    /// 已超时但取消屏障尚未给出裁决。
    TimedOut,
    /// 已冻结，等待人工裁决。
    Halted,
}

impl StepForwardStatus {
    /// 业务作用：返回状态的稳定文本名，用于持久化列、审计与管理接口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：正向状态稳定名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Succeeded => "SUCCEEDED",
            Self::Rejected => "REJECTED",
            Self::Cancelled => "CANCELLED",
            Self::Unknown => "UNKNOWN",
            Self::TimedOut => "TIMED_OUT",
            Self::Halted => "HALTED",
        }
    }

    /// 业务作用：把持久化列中的稳定文本解析回状态，是 `as_str` 的严格逆映射。
    ///
    /// 参数说明：
    /// - `raw`: 持久化读出的正向状态文本。
    ///
    /// 返回：识别成功返回对应状态；文本不在词汇表内返回 `None`，调用方按数据损坏处理。
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "PENDING" => Some(Self::Pending),
            "SUCCEEDED" => Some(Self::Succeeded),
            "REJECTED" => Some(Self::Rejected),
            "CANCELLED" => Some(Self::Cancelled),
            "UNKNOWN" => Some(Self::Unknown),
            "TIMED_OUT" => Some(Self::TimedOut),
            "HALTED" => Some(Self::Halted),
            _ => None,
        }
    }

    /// 业务作用：判断该状态是否已经确定到可以参与补偿计划冻结。
    ///
    /// 只有“确知发生了什么”的状态才算已定：`Unknown`/`TimedOut` 的效果可能仍会迟到生效，
    /// `Halted` 需要人工裁决；带着它们冻结计划等于对未知事实下结论。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`Pending`、`Succeeded`、`Rejected`、`Cancelled` 返回真。
    pub fn is_settled(self) -> bool {
        matches!(
            self,
            Self::Pending | Self::Succeeded | Self::Rejected | Self::Cancelled
        )
    }
}

/// 业务作用：表示 step journal 中一条已提交的正向事实，是冻结补偿计划的唯一输入来源。
///
/// 字段说明：计划只能由持久化事实推导，不允许依赖 Orchestrator 内存或正向命令原始 payload。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepJournalEntry {
    step: StepName,
    forward_status: StepForwardStatus,
}

impl StepJournalEntry {
    /// 业务作用：构造一条 journal 事实。
    ///
    /// 参数说明：
    /// - `step`: 步骤名称。
    /// - `forward_status`: 该步骤正向阶段的已提交状态。
    ///
    /// 返回：可参与计划冻结的 journal 条目。
    pub fn new(step: StepName, forward_status: StepForwardStatus) -> Self {
        Self {
            step,
            forward_status,
        }
    }

    /// 业务作用：读取步骤名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：步骤名称引用。
    pub fn step(&self) -> &StepName {
        &self.step
    }

    /// 业务作用：读取正向状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：该步骤的已提交正向状态。
    pub fn forward_status(&self) -> StepForwardStatus {
        self.forward_status
    }
}

/// 业务作用：表示补偿计划中的一项，绑定步骤与其在计划内的稳定执行位置。
///
/// 字段说明：`order` 是计划内的稳定序号，崩溃恢复后按同一序号继续，不重新推导顺序。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompensationEntry {
    step: StepName,
    order: u32,
}

impl CompensationEntry {
    /// 业务作用：读取需要补偿的步骤名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：步骤名称引用。
    pub fn step(&self) -> &StepName {
        &self.step
    }

    /// 业务作用：读取计划内稳定执行序号，用于持久化与恢复后继续。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：从 1 开始的计划内序号。
    pub fn order(&self) -> u32 {
        self.order
    }
}

/// 业务作用：表示一次性冻结的补偿计划，是 `COMPENSATED` 终态含义的唯一依据。
///
/// 字段说明：`version` 是覆盖 definition 摘要与全部计划项的内容摘要，写入实例后用于
/// 校验后续补偿推进仍在同一份计划上；`entries` 已按执行顺序排好，不允许运行期重排或插队。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompensationPlan {
    version: String,
    entries: Vec<CompensationEntry>,
}

impl CompensationPlan {
    /// 业务作用：读取计划内容摘要，写入实例的 `compensation_plan_version` 列。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：六十四位小写十六进制 SHA-256 摘要。
    pub fn version(&self) -> &str {
        &self.version
    }

    /// 业务作用：按执行顺序读取全部计划项。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已排序的计划项切片。
    pub fn entries(&self) -> &[CompensationEntry] {
        &self.entries
    }

    /// 业务作用：判断计划是否为空，用于“无需补偿即可直接收敛”的合法捷径。
    ///
    /// 首步即被拒绝时不存在任何已成功步骤，补偿计划为空，`COMPENSATING` 应立即收敛为
    /// `COMPENSATED`，不引入额外终态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无任何计划项时返回真。
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 业务作用：判断某步骤是否在冻结计划内，用于识别“计划外迟到成功”。
    ///
    /// 计划内的重复结果可以幂等忽略；计划外的迟到成功说明取消屏障或参与方状态机被破坏，
    /// 必须转人工介入并重建计划，绝不能动态插入正在执行的补偿序列。
    ///
    /// 参数说明：
    /// - `step`: 待判断的步骤名称。
    ///
    /// 返回：步骤在计划内时返回真。
    pub fn contains(&self, step: &StepName) -> bool {
        self.entries.iter().any(|entry| entry.step() == step)
    }
}

/// 业务作用：在进入补偿之前，从 definition 与已提交 step journal 一次性冻结补偿计划。
///
/// 纳入规则：定义序中 `forward_status = SUCCEEDED` 且可补偿的步骤全部纳入，按正向依赖逆序排列；
/// `REJECTED`/`CANCELLED`/`PENDING` 步骤自身不参与补偿——它们没有需要撤销的正向效果，
/// 对其执行补偿反而会撤销不存在的效果。
///
/// 参数说明：
/// - `definition`: 实例绑定版本的流程定义，提供步骤顺序与补偿能力。
/// - `journal`: 已提交的步骤正向事实；必须来自持久化读取。
///
/// 返回：全部前置条件成立时返回带摘要的冻结计划；journal 引用未知步骤、步骤重复、
/// 存在尚未裁决的步骤，或存在已成功的不可补偿步骤时返回合同违规，调用方必须转人工介入
/// 而不是带着破损事实开始补偿。
pub fn freeze_compensation_plan(
    definition: &WorkflowDefinition,
    journal: &[StepJournalEntry],
) -> ContractResult<CompensationPlan> {
    let mut seen = BTreeSet::new();
    for entry in journal {
        let step = definition.step(entry.step()).ok_or_else(|| {
            ContractViolation::new(
                CODE_PLAN_UNKNOWN_STEP,
                format!(
                    "journal references step `{}` absent from workflow `{}` v{}",
                    entry.step().as_str(),
                    definition.name().as_str(),
                    definition.version().get()
                ),
            )
        })?;

        // journal 内同一步骤重复会让“是否已成功”出现两个互相矛盾的事实来源，
        // 计划将取决于遍历顺序而不是持久化真相。
        if !seen.insert(entry.step().as_str()) {
            return Err(ContractViolation::new(
                CODE_PLAN_DUPLICATE_STEP,
                format!(
                    "journal contains duplicate step `{}`",
                    entry.step().as_str()
                ),
            ));
        }

        // 带着未裁决状态冻结计划，等于对“可能仍会迟到生效”的效果下结论；
        // 正常协议要求在进入补偿前解决全部已发出命令的确定终态。
        if !entry.forward_status().is_settled() {
            return Err(ContractViolation::new(
                CODE_PLAN_UNSETTLED_STEP,
                format!(
                    "step `{}` is `{}` and must be resolved before freezing a compensation plan",
                    entry.step().as_str(),
                    entry.forward_status().as_str()
                ),
            ));
        }

        // 已成功的 pivot 既无法补偿也不能静默跳过：跳过会让 COMPENSATED 终态
        // 谎称“全部已撤销”。definition 预检已把 pivot 限制在末步，出现该情形
        // 说明不变量已被破坏，只能转人工。
        if entry.forward_status() == StepForwardStatus::Succeeded && !step.is_compensable() {
            return Err(ContractViolation::new(
                CODE_PLAN_SUCCEEDED_PIVOT,
                format!(
                    "step `{}` succeeded but is non-compensable; automatic compensation must not proceed",
                    entry.step().as_str()
                ),
            ));
        }
    }

    // 缺少 definition 成员不能按“该步骤没成功”处理：持久化行可能被漏建或损坏，
    // 静默略过会让冻结计划不完整，并最终错误宣称整个 Saga 已补偿。
    if let Some(missing) = definition
        .steps()
        .iter()
        .find(|step| !seen.contains(step.name().as_str()))
    {
        return Err(ContractViolation::new(
            CODE_PLAN_MISSING_STEP,
            format!(
                "journal is missing definition step `{}` for workflow `{}` v{}",
                missing.name().as_str(),
                definition.name().as_str(),
                definition.version().get()
            ),
        ));
    }

    // 顺序由 definition 显式表达并逆序执行，绝不依赖 Kafka 偶然到达顺序；
    // 遍历 definition 而不是 journal，保证同一事实集合总是得到同一份计划。
    let mut entries = Vec::new();
    for step in definition.steps().iter().rev() {
        let succeeded = journal.iter().any(|entry| {
            entry.step() == step.name() && entry.forward_status() == StepForwardStatus::Succeeded
        });
        if succeeded && step.is_compensable() {
            entries.push(CompensationEntry {
                step: step.name().clone(),
                order: entries.len() as u32 + 1,
            });
        }
    }

    let mut hasher = Sha256::new();
    hasher.update(canonical_bytes(&[
        definition.digest().as_bytes(),
        &(entries.len() as u64).to_be_bytes(),
    ]));
    for entry in &entries {
        hasher.update(canonical_bytes(&[
            entry.step().as_str().as_bytes(),
            &entry.order().to_be_bytes(),
        ]));
    }
    let version = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();

    Ok(CompensationPlan { version, entries })
}
