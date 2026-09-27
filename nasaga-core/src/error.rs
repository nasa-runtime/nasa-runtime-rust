//! Saga 合同违规错误。
//!
//! 本 crate 的全部校验都在任何外部副作用之前完成，因此错误统一表达为“合同违规”，
//! 而不是运行期故障；调用方据此在启动预检或状态推进前 fail-closed。

/// 业务作用：表示一次在副作用发生前被拦截的 Saga 合同违规，携带可进入指标的稳定原因码
/// 与仅含结构性标识的脱敏细节。
///
/// 字段说明：`code` 是有界枚举式常量，可安全用作指标标签；`detail` 只允许包含 workflow、
/// step、状态名等结构性信息，禁止写入 payload、凭据、业务主键或外部错误原文。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractViolation {
    code: &'static str,
    detail: String,
}

impl ContractViolation {
    /// 业务作用：构造一次合同违规，绑定稳定原因码与脱敏细节。
    ///
    /// 参数说明：
    /// - `code`: 稳定原因码，必须来自本 crate 的常量集合，保证指标基数有界。
    /// - `detail`: 仅含结构性标识的说明文本，调用方不得传入业务负载或凭据。
    ///
    /// 返回：可直接向上传播的合同违规值；构造本身不产生任何副作用。
    pub fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }

    /// 业务作用：读取稳定原因码，供指标标签、告警路由与自动化裁决使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：有界常量字符串，在协议演进期间保持稳定。
    pub fn code(&self) -> &'static str {
        self.code
    }

    /// 业务作用：读取脱敏细节，供日志与管理面展示定位问题。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：结构性说明文本，不含敏感数据。
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl std::fmt::Display for ContractViolation {
    /// 业务作用：输出稳定原因码与脱敏细节，保证错误链进入日志和管理接口时不泄露敏感信息。
    ///
    /// 参数说明：
    /// - `formatter`: 标准库格式化器。
    ///
    /// 返回：格式化成功返回 `Ok`；写入失败时透传格式化错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "saga contract violation [{}]: {}",
            self.code, self.detail
        )
    }
}

impl std::error::Error for ContractViolation {}

/// 业务作用：统一本 crate 的校验结果类型，明确所有失败都是可在副作用前拦截的合同违规。
pub type ContractResult<T> = Result<T, ContractViolation>;

/// 标识符为空或只含空白。
pub const CODE_IDENTIFIER_EMPTY: &str = "identifier_empty";
/// 标识符带首尾空白，可能在跨系统传递时被静默改写。
pub const CODE_IDENTIFIER_UNTRIMMED: &str = "identifier_untrimmed";
/// 标识符超过长度上限。
pub const CODE_IDENTIFIER_TOO_LONG: &str = "identifier_too_long";
/// 标识符含控制字符或超出允许字符集。
pub const CODE_IDENTIFIER_CHARSET: &str = "identifier_charset";
/// 定义版本、attempt 等单调计数必须从 1 开始。
pub const CODE_COUNTER_ZERO: &str = "counter_must_start_at_one";
/// 单调计数溢出，必须停止自动推进而不是回绕。
pub const CODE_COUNTER_OVERFLOW: &str = "counter_overflow";
/// definition 未声明任何步骤。
pub const CODE_DEFINITION_EMPTY: &str = "definition_has_no_step";
/// definition 内步骤名重复。
pub const CODE_DEFINITION_DUPLICATE_STEP: &str = "definition_duplicate_step";
/// 不可补偿步骤之后仍存在可失败、可超时或可未知的步骤。
pub const CODE_DEFINITION_PIVOT_NOT_LAST: &str = "definition_pivot_not_last";
/// 步骤声明允许 `Unknown` 却没有给出解决模式或预算。
pub const CODE_DEFINITION_RESOLUTION_MISSING: &str = "definition_resolution_missing";
/// 步骤解决模式与取消模式不自洽。
pub const CODE_DEFINITION_MODE_CONFLICT: &str = "definition_mode_conflict";
/// 步骤超时或解决预算为零，无法形成有界等待。
pub const CODE_DEFINITION_BUDGET_ZERO: &str = "definition_budget_zero";
/// 实例状态迁移不在封闭状态机允许的集合内。
pub const CODE_TRANSITION_ILLEGAL: &str = "transition_illegal";
/// 已经发生补偿副作用后仍试图恢复正向执行。
pub const CODE_TRANSITION_FORWARD_AFTER_COMPENSATION: &str =
    "transition_forward_after_compensation";
/// 缺少同事务管理审计证据的人工关闭迁移，属于越权或伪造终态企图。
pub const CODE_TRANSITION_MANUAL_CLOSE_UNAUDITED: &str = "transition_manual_close_unaudited";
/// 冻结补偿计划时发现已成功的不可补偿步骤，属于不变量破坏。
pub const CODE_PLAN_SUCCEEDED_PIVOT: &str = "plan_contains_succeeded_pivot";
/// step journal 引用了 definition 中不存在的步骤。
pub const CODE_PLAN_UNKNOWN_STEP: &str = "plan_unknown_step";
/// step journal 中同一步骤重复出现。
pub const CODE_PLAN_DUPLICATE_STEP: &str = "plan_duplicate_step";
/// step journal 缺少 definition 中声明的步骤，无法证明补偿计划覆盖完整事实集合。
pub const CODE_PLAN_MISSING_STEP: &str = "plan_missing_step";
/// 冻结补偿计划时仍存在未裁决（未知、超时未决或已冻结）的步骤。
pub const CODE_PLAN_UNSETTLED_STEP: &str = "plan_unsettled_step";
