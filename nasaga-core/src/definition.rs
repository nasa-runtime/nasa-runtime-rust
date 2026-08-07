//! 带不可变版本的 workflow definition 及其发布前校验。
//!
//! definition 是跨服务流程的**唯一事实源**：本地宏 descriptor 只能覆盖当前 binary，
//! 无法在编译期发现完整业务链。definition 一经发布内容不可变，任何步骤、顺序或补偿依赖的
//! 修改都必须递增定义版本，并由 [`WorkflowDefinition::digest`] 的内容摘要固定到实例，
//! 防止“同一版本静默改定义”后用新内容驱动旧实例。

use std::collections::BTreeSet;
use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::error::{
    ContractResult, ContractViolation, CODE_DEFINITION_BUDGET_ZERO, CODE_DEFINITION_DUPLICATE_STEP,
    CODE_DEFINITION_EMPTY, CODE_DEFINITION_MODE_CONFLICT, CODE_DEFINITION_PIVOT_NOT_LAST,
    CODE_DEFINITION_RESOLUTION_MISSING,
};
use crate::identity::{
    canonical_bytes, DefinitionVersion, ServiceIdentity, StepName, WorkflowName,
};

/// 业务作用：声明步骤是否具备显式业务补偿，决定它能否进入补偿计划。
///
/// 分支说明：`Compensable` 必须注册补偿 handler；`NonCompensable`（pivot）没有撤销手段，
/// 因此受 [`WorkflowDefinition::new`] 发布前校验的位置约束。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compensation {
    /// 步骤可补偿，成功后会被纳入冻结补偿计划。
    Compensable,
    /// 步骤不可补偿（pivot）；一旦成功，自动补偿路径永远不得穿越它向前回卷。
    NonCompensable,
}

impl Compensation {
    /// 业务作用：返回稳定文本名，进入 definition 摘要与管理面展示。
    ///
    /// 该字符串是摘要输入的一部分，一旦发布不得修改。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：补偿能力的稳定名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Compensable => "compensable",
            Self::NonCompensable => "non-compensable",
        }
    }
}

/// 业务作用：声明步骤在超时取消屏障中的可取消形态，决定框架能否代为建立准入屏障。
///
/// 分支说明：`LocalFenceable` 由宏生成本地 gate 与业务写同事务竞争；
/// `ExternallyCancellable` 必须额外注册取消 handler 并可靠报告结果；
/// `ResolveOnly` 无法取消，只能查询或对账直到确定。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelMode {
    /// 纯本地数据库步骤：取消与执行竞争同一条 participant step gate。
    LocalFenceable,
    /// 外部可取消：参与方注册取消 handler，把取消建模为带稳定幂等键的业务命令。
    ExternallyCancellable,
    /// 不可取消：只能通过查询、回调或对账得到确定结果。
    ResolveOnly,
}

impl CancelMode {
    /// 业务作用：返回稳定文本名，进入 definition 摘要、contract projection 与预检比对。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：取消形态的稳定名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalFenceable => "local-fenceable",
            Self::ExternallyCancellable => "externally-cancellable",
            Self::ResolveOnly => "resolve-only",
        }
    }

    /// 业务作用：判断该形态是否意味着步骤会产生框架无法回滚的外部副作用。
    ///
    /// 该判断同时决定步骤能否合法返回 `Unknown`：纯本地步骤的事务结果总是可知，
    /// 返回 `Unknown` 属于合同违规。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：外部交互形态返回真，纯本地形态返回假。
    pub fn is_external(self) -> bool {
        !matches!(self, Self::LocalFenceable)
    }
}

/// 业务作用：声明未知结果的解决通道，保证 `Unknown` 不会成为可以永久停留的状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionMode {
    /// 外部系统主动回调；必须声明可校验的回调身份与 Inbox 去重入口。
    Callback,
    /// 主动轮询查询；必须注册类型化查询 handler。
    Poll,
    /// 直接进入受审计的人工裁决队列。
    Manual,
}

impl ResolutionMode {
    /// 业务作用：返回稳定文本名，进入 definition 摘要与 descriptor 一致性预检。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：解决模式的稳定名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Callback => "callback",
            Self::Poll => "poll",
            Self::Manual => "manual",
        }
    }
}

/// 业务作用：声明步骤是否允许返回 `Unknown`，以及未知结果的解决通道与有界预算。
///
/// 字段说明：`allow_unknown` 与步骤的取消形态必须自洽；`mode` 与 `budget` 只在允许未知时存在，
/// 保证每个 `Unknown` 都带着自己的解决期限，超期升级人工介入而不是无限滞留。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionSpec {
    allow_unknown: bool,
    mode: Option<ResolutionMode>,
    budget: Duration,
}

impl ResolutionSpec {
    /// 业务作用：声明步骤不产生外部效果，因此不允许返回 `Unknown`。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：禁止未知结果的解决声明。
    pub fn forbidden() -> Self {
        Self {
            allow_unknown: false,
            mode: None,
            budget: Duration::ZERO,
        }
    }

    /// 业务作用：声明步骤允许返回 `Unknown`，并绑定解决通道与有界解决预算。
    ///
    /// 参数说明：
    /// - `mode`: 未知结果的解决通道。
    /// - `budget`: 解决期限；耗尽后升级人工介入，绝不自动降级为拒绝。
    ///
    /// 返回：允许未知结果的解决声明；预算合法性由 definition 校验统一裁决。
    pub fn allowed(mode: ResolutionMode, budget: Duration) -> Self {
        Self {
            allow_unknown: true,
            mode: Some(mode),
            budget,
        }
    }

    /// 业务作用：判断步骤是否被允许返回 `Unknown`，供 handler 合同校验与运行期违规检测。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：允许时返回真。
    pub fn allow_unknown(&self) -> bool {
        self.allow_unknown
    }

    /// 业务作用：读取解决通道，供启动预检与 descriptor 比对。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：允许未知时返回解决模式；禁止未知时返回空。
    pub fn mode(&self) -> Option<ResolutionMode> {
        self.mode
    }

    /// 业务作用：读取解决预算，供 durable timer 计算升级人工介入的时刻。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：解决期限；禁止未知时为零。
    pub fn budget(&self) -> Duration {
        self.budget
    }
}

/// 业务作用：声明步骤超时后，若取消屏障回报正向执行其实已经成功时的收敛策略。
///
/// 分支说明：`AcceptLateSuccess` 接受迟到成功并继续正向；`CompensateLateSuccess` 把该步骤
/// 纳入冻结补偿计划。后者只对可补偿步骤合法——对成功 pivot 补偿上游会把不可撤销的效果
/// 伪装成整体回滚。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutPolicy {
    /// 迟到成功仍按成功收敛，继续正向流程。
    AcceptLateSuccess,
    /// 迟到成功按业务期限失效处理，纳入补偿计划。
    CompensateLateSuccess,
}

impl TimeoutPolicy {
    /// 业务作用：返回稳定文本名，进入 definition 摘要与运维展示。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：超时策略的稳定名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AcceptLateSuccess => "accept-late-success",
            Self::CompensateLateSuccess => "compensate-late-success",
        }
    }
}

/// 业务作用：描述 workflow 中的单个步骤，绑定归属服务、补偿能力、取消形态与有界期限。
///
/// 字段说明：`owner` 是 result 事件 producer 绑定的登记身份；其余字段共同决定该步骤在
/// 超时、未知与补偿路径上的合法行为集合。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepDefinition {
    name: StepName,
    owner: ServiceIdentity,
    compensation: Compensation,
    cancel_mode: CancelMode,
    resolution: ResolutionSpec,
    timeout: Duration,
    timeout_policy: TimeoutPolicy,
}

impl StepDefinition {
    /// 业务作用：构造步骤定义；合法性由所属 definition 的整体校验统一裁决，
    /// 避免单步骤看似合法、组合起来却破坏 pivot 或补偿不变量。
    ///
    /// 参数说明：
    /// - `name`: 步骤名称。
    /// - `owner`: 该步骤唯一有权产出 result 事件的逻辑服务身份。
    /// - `compensation`: 步骤是否具备显式补偿。
    /// - `cancel_mode`: 超时取消屏障中的可取消形态。
    /// - `resolution`: 未知结果的解决声明。
    /// - `timeout`: 正向执行的有界期限，到期进入取消屏障。
    /// - `timeout_policy`: 迟到成功的收敛策略。
    ///
    /// 返回：未校验的步骤定义值，必须经 [`WorkflowDefinition::new`] 的发布前校验后才可使用。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: StepName,
        owner: ServiceIdentity,
        compensation: Compensation,
        cancel_mode: CancelMode,
        resolution: ResolutionSpec,
        timeout: Duration,
        timeout_policy: TimeoutPolicy,
    ) -> Self {
        Self {
            name,
            owner,
            compensation,
            cancel_mode,
            resolution,
            timeout,
            timeout_policy,
        }
    }

    /// 业务作用：读取步骤名称，用于 journal 关联、身份派生与计划构造。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：步骤名称引用。
    pub fn name(&self) -> &StepName {
        &self.name
    }

    /// 业务作用：读取步骤 owner，用于校验 result 事件的 producer 身份。
    ///
    /// 能写共享 result topic 不等于有权代表该步骤作证，因此该字段是安全校验输入。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：owner 逻辑服务身份引用。
    pub fn owner(&self) -> &ServiceIdentity {
        &self.owner
    }

    /// 业务作用：读取补偿能力，用于冻结补偿计划时决定是否纳入该步骤。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：补偿能力声明。
    pub fn compensation(&self) -> Compensation {
        self.compensation
    }

    /// 业务作用：读取取消形态，用于取消屏障选择本地 gate、外部取消命令或纯解决通道。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：取消形态声明。
    pub fn cancel_mode(&self) -> CancelMode {
        self.cancel_mode
    }

    /// 业务作用：读取未知结果解决声明，用于 handler 合同校验与解决预算计时。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：解决声明引用。
    pub fn resolution(&self) -> &ResolutionSpec {
        &self.resolution
    }

    /// 业务作用：读取正向执行期限，用于写入 durable timer。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：步骤超时时长。
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// 业务作用：读取迟到成功收敛策略，用于取消屏障回报 `AlreadyTerminal(Succeeded)` 时裁决。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：超时策略。
    pub fn timeout_policy(&self) -> TimeoutPolicy {
        self.timeout_policy
    }

    /// 业务作用：判断步骤成功后是否需要被撤销，用于补偿计划纳入判断。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可补偿步骤返回真，pivot 返回假。
    pub fn is_compensable(&self) -> bool {
        matches!(self.compensation, Compensation::Compensable)
    }
}

/// 业务作用：表示一个已命名、带不可变版本的完整跨服务流程定义。
///
/// 字段说明：`steps` 按正向执行顺序排列，补偿默认按其逆序执行；顺序本身是摘要输入，
/// 因此调整顺序必然改变摘要并要求递增定义版本。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowDefinition {
    name: WorkflowName,
    version: DefinitionVersion,
    steps: Vec<StepDefinition>,
}

impl WorkflowDefinition {
    /// 业务作用：构造并**立即校验**一个 workflow definition，把非法流程拦在发布之前。
    ///
    /// 校验在任何持久化和外部副作用之前完成；只有通过校验的 definition 才允许分发
    /// contract projection 或驱动实例。
    ///
    /// 参数说明：
    /// - `name`: workflow 名称。
    /// - `version`: definition 版本，固定到实例后不可更改。
    /// - `steps`: 按正向执行顺序排列的步骤定义。
    ///
    /// 返回：全部不变量成立时返回可发布的 definition；步骤为空、重名、pivot 位置非法、
    /// 解决声明与取消形态冲突或期限为零时返回合同违规。
    pub fn new(
        name: WorkflowName,
        version: DefinitionVersion,
        steps: Vec<StepDefinition>,
    ) -> ContractResult<Self> {
        let definition = Self {
            name,
            version,
            steps,
        };
        definition.validate()?;
        Ok(definition)
    }

    /// 业务作用：校验 definition 的全部结构性安全不变量。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部不变量成立时返回成功；任一不变量破坏时返回带稳定原因码的合同违规。
    fn validate(&self) -> ContractResult<()> {
        if self.steps.is_empty() {
            return Err(ContractViolation::new(
                CODE_DEFINITION_EMPTY,
                format!("workflow `{}` declares no step", self.name.as_str()),
            ));
        }

        let mut seen = BTreeSet::new();
        for step in &self.steps {
            // 步骤名是身份派生、journal 关联与补偿计划的共同锚点；重名会让两个步骤
            // 共享同一 effect_id，进而共用外部幂等键并互相顶替对方的执行证据。
            if !seen.insert(step.name.as_str()) {
                return Err(ContractViolation::new(
                    CODE_DEFINITION_DUPLICATE_STEP,
                    format!(
                        "workflow `{}` declares duplicate step `{}`",
                        self.name.as_str(),
                        step.name.as_str()
                    ),
                ));
            }
        }

        for (index, step) in self.steps.iter().enumerate() {
            self.validate_step(index, step)?;
        }

        Ok(())
    }

    /// 业务作用：校验单个步骤在流程中的位置与模式组合是否合法。
    ///
    /// 参数说明：
    /// - `index`: 步骤在正向序列中的下标，用于判断 pivot 是否位于末尾。
    /// - `step`: 待校验的步骤定义。
    ///
    /// 返回：合法返回成功；pivot 位置非法、模式冲突或期限为零时返回合同违规。
    fn validate_step(&self, index: usize, step: &StepDefinition) -> ContractResult<()> {
        // pivot 规则：不可补偿步骤成功后既不能执行补偿也不能静默跳过，否则 COMPENSATED
        // 终态会谎称“全部已撤销”。首版把它收紧为“只能是最后一步”，从而保证进入补偿时
        // 计划内永远不存在已成功的 pivot。
        if matches!(step.compensation, Compensation::NonCompensable)
            && index + 1 != self.steps.len()
        {
            return Err(ContractViolation::new(
                CODE_DEFINITION_PIVOT_NOT_LAST,
                format!(
                    "workflow `{}` step `{}` is non-compensable but is followed by further steps",
                    self.name.as_str(),
                    step.name.as_str()
                ),
            ));
        }

        // 成功 pivot 不得被配置成“迟到成功即补偿上游”：pivot 自身无法撤销，
        // 补偿上游只会制造“上游已回滚、pivot 效果仍在”的不一致终态。
        if matches!(step.compensation, Compensation::NonCompensable)
            && matches!(step.timeout_policy, TimeoutPolicy::CompensateLateSuccess)
        {
            return Err(ContractViolation::new(
                CODE_DEFINITION_MODE_CONFLICT,
                format!(
                    "workflow `{}` step `{}` is non-compensable and must not compensate on late success",
                    self.name.as_str(),
                    step.name.as_str()
                ),
            ));
        }

        // 步骤必须有有界执行期限，否则超时取消屏障永远不会触发，Saga 可能无限滞留在 RUNNING。
        if step.timeout.is_zero() {
            return Err(ContractViolation::new(
                CODE_DEFINITION_BUDGET_ZERO,
                format!(
                    "workflow `{}` step `{}` must declare a non-zero timeout",
                    self.name.as_str(),
                    step.name.as_str()
                ),
            ));
        }

        // 取消形态与未知声明必须自洽：纯本地步骤的事务结果总是可知，允许它返回 Unknown
        // 会让一个本可裁决的失败退化成需要人工对账的悬挂状态；反之外部交互步骤若禁止
        // Unknown，超时后就只剩“伪装成拒绝”这一条错误出路。
        if step.cancel_mode.is_external() != step.resolution.allow_unknown() {
            return Err(ContractViolation::new(
                CODE_DEFINITION_MODE_CONFLICT,
                format!(
                    "workflow `{}` step `{}` cancel mode `{}` conflicts with allow_unknown={}",
                    self.name.as_str(),
                    step.name.as_str(),
                    step.cancel_mode.as_str(),
                    step.resolution.allow_unknown()
                ),
            ));
        }

        if step.resolution.allow_unknown() {
            // 允许未知必须同时给出解决通道与有界预算，否则 Unknown 会成为永久停留状态。
            if step.resolution.mode().is_none() {
                return Err(ContractViolation::new(
                    CODE_DEFINITION_RESOLUTION_MISSING,
                    format!(
                        "workflow `{}` step `{}` allows unknown but declares no resolution mode",
                        self.name.as_str(),
                        step.name.as_str()
                    ),
                ));
            }
            if step.resolution.budget().is_zero() {
                return Err(ContractViolation::new(
                    CODE_DEFINITION_BUDGET_ZERO,
                    format!(
                        "workflow `{}` step `{}` allows unknown but declares no resolution budget",
                        self.name.as_str(),
                        step.name.as_str()
                    ),
                ));
            }
        } else if step.resolution.mode().is_some() {
            return Err(ContractViolation::new(
                CODE_DEFINITION_MODE_CONFLICT,
                format!(
                    "workflow `{}` step `{}` forbids unknown but declares a resolution mode",
                    self.name.as_str(),
                    step.name.as_str()
                ),
            ));
        }

        Ok(())
    }

    /// 业务作用：读取 workflow 名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：workflow 名称引用。
    pub fn name(&self) -> &WorkflowName {
        &self.name
    }

    /// 业务作用：读取 definition 版本，用于实例绑定与多版本并存判断。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：definition 版本。
    pub fn version(&self) -> DefinitionVersion {
        self.version
    }

    /// 业务作用：按正向执行顺序读取全部步骤。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：步骤定义切片，下标即正向序号。
    pub fn steps(&self) -> &[StepDefinition] {
        &self.steps
    }

    /// 业务作用：按名称定位步骤定义，用于校验 journal、命令与结果事件引用的步骤是否合法。
    ///
    /// 参数说明：
    /// - `name`: 待查找的步骤名称。
    ///
    /// 返回：存在时返回步骤定义引用；definition 未声明该步骤时返回空。
    pub fn step(&self, name: &StepName) -> Option<&StepDefinition> {
        self.steps.iter().find(|step| step.name() == name)
    }

    /// 业务作用：读取流程末尾的不可补偿步骤（pivot），用于补偿前的不变量检查与运维展示。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：末步不可补偿时返回该步骤；全部步骤可补偿时返回空。
    pub fn pivot(&self) -> Option<&StepDefinition> {
        self.steps.last().filter(|step| !step.is_compensable())
    }

    /// 业务作用：计算覆盖全部流程语义的 canonical 内容摘要，作为“同一版本不可静默改内容”的证据。
    ///
    /// 摘要以长度前缀编码逐字段写入，字段边界不可伪造；步骤顺序参与摘要，因此重排步骤
    /// 必然改变摘要。Orchestrator 启动时用它校验非终态实例引用的 definition 未发生漂移，
    /// 参与方用它校验 contract projection 与本地 descriptor 对齐。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：六十四位小写十六进制 SHA-256 摘要。
    pub fn digest(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(canonical_bytes(&[
            self.name.as_str().as_bytes(),
            &self.version.get().to_be_bytes(),
            &(self.steps.len() as u64).to_be_bytes(),
        ]));
        for step in &self.steps {
            // 每个步骤的全部语义字段都进入摘要：任何一项变化都必须表现为新的 definition 版本，
            // 否则跟随者可能用新内容解释旧实例的历史事实。
            hasher.update(canonical_bytes(&[
                step.name.as_str().as_bytes(),
                step.owner.as_str().as_bytes(),
                step.compensation.as_str().as_bytes(),
                step.cancel_mode.as_str().as_bytes(),
                &[u8::from(step.resolution.allow_unknown())],
                step.resolution
                    .mode()
                    .map(ResolutionMode::as_str)
                    .unwrap_or("")
                    .as_bytes(),
                &step.resolution.budget().as_millis().to_be_bytes(),
                &step.timeout.as_millis().to_be_bytes(),
                step.timeout_policy.as_str().as_bytes(),
            ]));
        }
        hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}
