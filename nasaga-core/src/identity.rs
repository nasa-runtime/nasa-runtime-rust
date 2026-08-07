//! Saga 身份体系：名称校验、canonical 编码与确定性身份派生。
//!
//! 核心不变量是**业务效果身份与投递尝试身份必须分离**：
//! `effect_id` 跨全部 attempt 永久稳定，参与方 step gate、业务唯一键和外部系统幂等键都用它；
//! `command_id` 只标识某一次 attempt，同一 attempt 的传输重投复用，新 attempt 才递增。
//! 若用含 attempt 的 `command_id` 做外部幂等，新 attempt 会重复扣款、退款或释放资源。

use uuid::Uuid;

use crate::error::{
    ContractResult, ContractViolation, CODE_COUNTER_OVERFLOW, CODE_COUNTER_ZERO,
    CODE_IDENTIFIER_CHARSET, CODE_IDENTIFIER_EMPTY, CODE_IDENTIFIER_TOO_LONG,
    CODE_IDENTIFIER_UNTRIMMED,
};

/// 派生全部 Saga 稳定身份使用的固定命名空间（ASCII `nasasaga-v1-idns`）。
///
/// 该常量一旦发布**不得修改**：改动会让同一 Saga 在新旧进程算出不同的 effect/command id，
/// 使外部幂等键失效，并在滚动发布期间造成重复扣款或重复补偿。需要变更派生规则时，
/// 必须通过新的 definition version 与显式迁移，而不是原地改命名空间。
const SAGA_ID_NAMESPACE: Uuid = Uuid::from_bytes(*b"nasasaga-v1-idns");

/// 结构性标识符长度上限；同时约束指标标签与外部命令载荷体积。
const IDENTIFIER_MAX_LEN: usize = 128;

/// 不透明业务标识符长度上限；业务主键可能较长，但仍需有界以免污染索引与日志。
const OPAQUE_MAX_LEN: usize = 256;

/// 业务作用：按“进入指标标签与拓扑名称”的严格字符集校验结构性标识符，
/// 防止 workflow/step 名称把任意字符带进指标、topic 或管理接口。
///
/// 参数说明：
/// - `raw`: 待校验的原始名称。
/// - `kind`: 出现在错误细节里的标识符种类，用于定位问题。
///
/// 返回：合法时返回归一化后的名称；空白、带首尾空格、超长或含非法字符时返回合同违规。
fn validate_identifier(raw: &str, kind: &str) -> ContractResult<String> {
    if raw != raw.trim() {
        return Err(ContractViolation::new(
            CODE_IDENTIFIER_UNTRIMMED,
            format!("{kind} must not have leading or trailing whitespace"),
        ));
    }
    if raw.is_empty() {
        return Err(ContractViolation::new(
            CODE_IDENTIFIER_EMPTY,
            format!("{kind} must not be empty"),
        ));
    }
    if raw.len() > IDENTIFIER_MAX_LEN {
        return Err(ContractViolation::new(
            CODE_IDENTIFIER_TOO_LONG,
            format!("{kind} exceeds {IDENTIFIER_MAX_LEN} bytes"),
        ));
    }
    // 只允许 ASCII 字母数字与 `_`、`-`、`.`：这些名称会进入指标标签、topic 名称和
    // 管理接口路径，放开字符集会同时带来基数污染与下游解析歧义。
    if !raw
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(ContractViolation::new(
            CODE_IDENTIFIER_CHARSET,
            format!("{kind} allows only ASCII alphanumeric, '_', '-' and '.'"),
        ));
    }
    Ok(raw.to_string())
}

/// 业务作用：校验业务提供的不透明标识符，允许更宽字符集但仍拒绝控制字符与超长值。
///
/// 参数说明：
/// - `raw`: 待校验的原始标识符。
/// - `kind`: 出现在错误细节里的标识符种类。
///
/// 返回：合法时返回归一化标识符；空白、带首尾空格、超长或含控制字符时返回合同违规。
fn validate_opaque(raw: &str, kind: &str) -> ContractResult<String> {
    if raw != raw.trim() {
        return Err(ContractViolation::new(
            CODE_IDENTIFIER_UNTRIMMED,
            format!("{kind} must not have leading or trailing whitespace"),
        ));
    }
    if raw.is_empty() {
        return Err(ContractViolation::new(
            CODE_IDENTIFIER_EMPTY,
            format!("{kind} must not be empty"),
        ));
    }
    if raw.len() > OPAQUE_MAX_LEN {
        return Err(ContractViolation::new(
            CODE_IDENTIFIER_TOO_LONG,
            format!("{kind} exceeds {OPAQUE_MAX_LEN} bytes"),
        ));
    }
    // 控制字符会破坏日志、审计与 CSV/JSON 导出的行边界，且可能被用于伪造管理面输出。
    if raw.chars().any(char::is_control) {
        return Err(ContractViolation::new(
            CODE_IDENTIFIER_CHARSET,
            format!("{kind} must not contain control characters"),
        ));
    }
    Ok(raw.to_string())
}

/// 业务作用：以长度前缀方式拼接字段，消除字段边界歧义，使不同字段组合永远得到不同字节序列。
///
/// 直接字符串拼接会让 `("ab", "c")` 与 `("a", "bc")` 产生同一输入，从而让两个不同步骤
/// 派生出同一个 `effect_id`，进而共享外部幂等键。所有身份与摘要派生都必须走本函数。
///
/// 参数说明：
/// - `fields`: 按固定顺序给出的字段字节切片；顺序本身是编码的一部分。
///
/// 返回：可直接送入 UUIDv5 或摘要器的规范字节串。
pub(crate) fn canonical_bytes(fields: &[&[u8]]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for field in fields {
        encoded.extend_from_slice(&(field.len() as u64).to_be_bytes());
        encoded.extend_from_slice(field);
    }
    encoded
}

/// 业务作用：表示 Saga 实例的稳定身份，贯穿全部步骤、命令、计时器与审计记录。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SagaId(String);

impl SagaId {
    /// 业务作用：校验并构造 Saga 实例身份。
    ///
    /// 参数说明：
    /// - `raw`: 调用方生成的实例标识（通常为 UUID 文本）。
    ///
    /// 返回：合法时返回实例身份；空白、超长或含控制字符时返回合同违规。
    pub fn new(raw: impl AsRef<str>) -> ContractResult<Self> {
        validate_opaque(raw.as_ref(), "saga id").map(Self)
    }

    /// 业务作用：读取实例身份文本，用于持久化、命令载荷与审计关联。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：实例身份字符串切片。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// 业务作用：表示租户身份；多租户隔离与实例创建幂等唯一键都依赖它非空。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TenantId(String);

impl TenantId {
    /// 业务作用：校验并构造租户身份。
    ///
    /// 无租户部署必须显式传入固定 system tenant，而不是留空：数据库的 nullable unique
    /// 允许多行 NULL，会让实例创建幂等唯一键失效并产生重复 Saga。
    ///
    /// 参数说明：
    /// - `raw`: 租户标识。
    ///
    /// 返回：合法时返回租户身份；空白、超长或含控制字符时返回合同违规。
    pub fn new(raw: impl AsRef<str>) -> ContractResult<Self> {
        validate_opaque(raw.as_ref(), "tenant id").map(Self)
    }

    /// 业务作用：读取租户身份文本，用于隔离查询与唯一键构造。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：租户身份字符串切片。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// 业务作用：表示业务幂等键，保证同一业务意图只创建一个 Saga 实例。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BusinessKey(String);

impl BusinessKey {
    /// 业务作用：校验并构造业务幂等键。
    ///
    /// 参数说明：
    /// - `raw`: 业务语义唯一的键（如订单号）。
    ///
    /// 返回：合法时返回业务键；空白、超长或含控制字符时返回合同违规。
    pub fn new(raw: impl AsRef<str>) -> ContractResult<Self> {
        validate_opaque(raw.as_ref(), "business key").map(Self)
    }

    /// 业务作用：读取业务键文本，仅用于唯一键与受权管理查询。
    ///
    /// 该值禁止进入指标标签：业务键基数无界，会直接击穿监控存储。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：业务键字符串切片。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// 业务作用：表示 workflow 名称，作为跨服务流程的稳定标识与指标维度。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkflowName(String);

impl WorkflowName {
    /// 业务作用：校验并构造 workflow 名称。
    ///
    /// 参数说明：
    /// - `raw`: 流程名称。
    ///
    /// 返回：合法时返回名称；空白、带首尾空格、超长或含非法字符时返回合同违规。
    pub fn new(raw: impl AsRef<str>) -> ContractResult<Self> {
        validate_identifier(raw.as_ref(), "workflow name").map(Self)
    }

    /// 业务作用：读取 workflow 名称，用于 definition 查找、指标标签与命令路由。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：workflow 名称字符串切片。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// 业务作用：表示步骤名称，是 definition、step journal 与命令身份的共同锚点。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StepName(String);

impl StepName {
    /// 业务作用：校验并构造步骤名称。
    ///
    /// 参数说明：
    /// - `raw`: 步骤名称。
    ///
    /// 返回：合法时返回名称；空白、带首尾空格、超长或含非法字符时返回合同违规。
    pub fn new(raw: impl AsRef<str>) -> ContractResult<Self> {
        validate_identifier(raw.as_ref(), "step name").map(Self)
    }

    /// 业务作用：读取步骤名称，用于 definition 校验、journal 关联与身份派生。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：步骤名称字符串切片。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// 业务作用：表示参与方逻辑服务身份，用于把 result 事件与 definition 登记的步骤 owner 绑定。
///
/// 这里固定的是**逻辑**身份，不含证书序列号或签名 key id 等会轮换的物理凭据；
/// 物理凭据由独立、可热更新的 trust policy 映射到该逻辑身份，正常轮换不改变 definition 摘要。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ServiceIdentity(String);

impl ServiceIdentity {
    /// 业务作用：校验并构造逻辑服务身份。
    ///
    /// 参数说明：
    /// - `raw`: 逻辑服务名。
    ///
    /// 返回：合法时返回服务身份；空白、带首尾空格、超长或含非法字符时返回合同违规。
    pub fn new(raw: impl AsRef<str>) -> ContractResult<Self> {
        validate_identifier(raw.as_ref(), "service identity").map(Self)
    }

    /// 业务作用：读取逻辑服务身份，用于 producer 绑定校验与 contract projection。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：服务身份字符串切片。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// 业务作用：表示 definition 版本；版本固定到实例后，运行中实例始终按该版本恢复与补偿。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DefinitionVersion(u32);

impl DefinitionVersion {
    /// 业务作用：校验并构造 definition 版本。
    ///
    /// 参数说明：
    /// - `value`: 定义版本，必须从 1 开始单调递增。
    ///
    /// 返回：合法时返回定义版本；传入 0 时返回合同违规，避免“未设置”与“首个版本”混淆。
    pub fn new(value: u32) -> ContractResult<Self> {
        if value == 0 {
            return Err(ContractViolation::new(
                CODE_COUNTER_ZERO,
                "definition version must start at 1",
            ));
        }
        Ok(Self(value))
    }

    /// 业务作用：读取定义版本原始值，用于持久化、身份派生与兼容判断。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：定义版本。
    pub fn get(self) -> u32 {
        self.0
    }
}

/// 业务作用：表示同一业务效果下的投递尝试序号，把“重投”与“新尝试”在身份层面分开。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AttemptNo(u32);

impl AttemptNo {
    /// 首次尝试序号；journal 与命令身份都从这里开始。
    pub const FIRST: Self = Self(1);

    /// 业务作用：校验并构造尝试序号。
    ///
    /// 参数说明：
    /// - `value`: 尝试序号，必须从 1 开始。
    ///
    /// 返回：合法时返回序号；传入 0 时返回合同违规。
    pub fn new(value: u32) -> ContractResult<Self> {
        if value == 0 {
            return Err(ContractViolation::new(
                CODE_COUNTER_ZERO,
                "attempt number must start at 1",
            ));
        }
        Ok(Self(value))
    }

    /// 业务作用：推进到下一次尝试，仅当 definition 的重试策略明确要求新 attempt 时调用。
    ///
    /// 溢出时返回错误而不是回绕：回绕会让新 attempt 复用旧 `command_id`，
    /// 使已经落库的旧尝试证据被冒名顶替。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功返回下一序号；溢出时返回合同违规，调用方应转人工介入。
    pub fn next(self) -> ContractResult<Self> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(|| ContractViolation::new(CODE_COUNTER_OVERFLOW, "attempt number overflow"))
    }

    /// 业务作用：读取尝试序号原始值，用于持久化与身份派生。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尝试序号。
    pub fn get(self) -> u32 {
        self.0
    }
}

/// 业务作用：表示一个步骤在生命周期中的执行阶段；四个阶段各自拥有独立的身份命名空间。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum StepPhase {
    /// 正向执行。
    Execute,
    /// 超时后的取消屏障。
    Cancel,
    /// 显式业务补偿。
    Compensate,
    /// 对未知结果的查询、回调或对账裁决。
    Resolve,
}

impl StepPhase {
    /// 业务作用：返回阶段的稳定文本名，进入身份派生、持久化与指标标签。
    ///
    /// 该字符串是身份派生输入的一部分，一旦发布不得修改。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：阶段稳定名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Execute => "execute",
            Self::Cancel => "cancel",
            Self::Compensate => "compensate",
            Self::Resolve => "resolve",
        }
    }

    /// 业务作用：把持久化列中的稳定文本解析回阶段，是 `as_str` 的严格逆映射。
    ///
    /// 该文本同时是身份派生输入的一部分，因此解析必须与 `as_str` 严格互逆；
    /// 任何近似匹配都会让恢复进程为同一阶段派生出不同身份。
    ///
    /// 参数说明：
    /// - `raw`: 持久化读出的阶段文本。
    ///
    /// 返回：识别成功返回对应阶段；文本不在词汇表内返回 `None`，调用方按数据损坏处理。
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "execute" => Some(Self::Execute),
            "cancel" => Some(Self::Cancel),
            "compensate" => Some(Self::Compensate),
            "resolve" => Some(Self::Resolve),
            _ => None,
        }
    }
}

/// 业务作用：表示跨 attempt 永久稳定的业务效果身份。
///
/// 参与方的 step gate、业务唯一键以及外部系统 idempotency key **必须**使用它；
/// 使用含 attempt 的 [`CommandId`] 会让一次新 attempt 变成一次全新的外部副作用。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EffectId(Uuid);

impl EffectId {
    /// 业务作用：由 Saga 身份、definition 版本、步骤与阶段确定性派生业务效果身份。
    ///
    /// 派生输入不含 attempt，因此同一步骤同一阶段的所有重试共享同一效果身份，
    /// 这正是外部幂等与参与方 gate 去重成立的前提。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `definition_version`: 固定到实例的 definition 版本。
    /// - `step`: 步骤名称。
    /// - `phase`: 执行阶段。
    ///
    /// 返回：跨进程一致的效果身份；相同输入永远得到相同值。
    pub fn derive(
        saga_id: &SagaId,
        definition_version: DefinitionVersion,
        step: &StepName,
        phase: StepPhase,
    ) -> Self {
        let encoded = canonical_bytes(&[
            saga_id.as_str().as_bytes(),
            &definition_version.get().to_be_bytes(),
            step.as_str().as_bytes(),
            phase.as_str().as_bytes(),
        ]);
        Self(Uuid::new_v5(&SAGA_ID_NAMESPACE, &encoded))
    }

    /// 业务作用：读取底层 UUID，用于持久化列、外部幂等键与跨系统传递。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：效果身份 UUID。
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl std::fmt::Display for EffectId {
    /// 业务作用：以标准 UUID 文本输出效果身份，供命令载荷与审计记录使用。
    ///
    /// 参数说明：
    /// - `formatter`: 标准库格式化器。
    ///
    /// 返回：格式化成功返回 `Ok`；写入失败时透传格式化错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// 业务作用：表示单次投递尝试的命令身份，用于 Inbox 去重与 attempt journal。
///
/// 同一 attempt 的所有传输重投复用同一个值；只有 definition 重试策略创建新 attempt 时才变化。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CommandId(Uuid);

impl CommandId {
    /// 业务作用：由业务效果身份与尝试序号确定性派生命令身份。
    ///
    /// 参数说明：
    /// - `effect`: 跨 attempt 稳定的业务效果身份。
    /// - `attempt`: 本次尝试序号。
    ///
    /// 返回：跨进程一致的命令身份；崩溃恢复后重新派生不会得到不同值，
    /// 因此不会为同一尝试重复占用 Inbox 去重身份。
    pub fn derive(effect: &EffectId, attempt: AttemptNo) -> Self {
        let encoded = canonical_bytes(&[effect.as_uuid().as_bytes(), &attempt.get().to_be_bytes()]);
        Self(Uuid::new_v5(&SAGA_ID_NAMESPACE, &encoded))
    }

    /// 业务作用：读取底层 UUID，用于 Inbox 主键与投递审计。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：命令身份 UUID。
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl std::fmt::Display for CommandId {
    /// 业务作用：以标准 UUID 文本输出命令身份，供命令载荷与投递日志使用。
    ///
    /// 参数说明：
    /// - `formatter`: 标准库格式化器。
    ///
    /// 返回：格式化成功返回 `Ok`；写入失败时透传格式化错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}
