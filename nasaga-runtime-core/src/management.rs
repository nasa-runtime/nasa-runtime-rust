//! Saga 管理面主体、权限与原因合同。
//!
//! HTTP/JWT/mTLS 的认证实现属于宿主应用；运行时只接收认证层构造的不可变管理上下文，
//! 并在任何数据库读取或状态变化前执行细粒度权限检查。actor 与 reason 会进入同事务审计。

use std::collections::BTreeSet;

use nasaga_backend::{
    SagaAttemptAuditRow, SagaConflictFactRow, SagaControlAuditRow, SagaManagementAuditRow,
    SagaStepAttemptRow, SagaTransitionAuditRow,
};
use serde::{Deserialize, Serialize};

/// 业务作用：定义 Saga 管理操作的最小授权单元，避免用单一 admin 布尔值放大权限。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SagaManagementPermission {
    /// 暂停自动路由与 timer 动作。
    Pause,
    /// 恢复自动路由与 timer 动作。
    Resume,
    /// 在人工介入后沿冻结计划重试补偿。
    RetryCompensation,
    /// 在 Unknown 解决预算耗尽后发布一次受审计的新查询。
    RetryResolution,
    /// 读取实例、attempt、transition、control 与冲突审计。
    ReadAudit,
    /// 租户受限的实例只读检索（状态/时间窗过滤 + keyset 分页）。
    ///
    /// 与写动作权限分离：运维定位待处置对象只需要本权限，不需要任何改变实例状态的
    /// 能力；响应不含业务 payload。
    ListInstances,
    /// 系统外处置完成后把 `MANUAL_INTERVENTION` 实例人工关闭为 `MANUALLY_CLOSED`。
    ///
    /// 关闭只表达"自动化已由人工关闭"，不伪造 `COMPLETED`/`COMPENSATED` 业务事实；
    /// 动作要求一次性 operation identity 并与审计同事务提交。
    ManualClose,
}

impl SagaManagementPermission {
    /// 业务作用：返回进入鉴权日志与审计合同的稳定权限名。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：低基数稳定权限字符串。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pause => "saga.pause",
            Self::Resume => "saga.resume",
            Self::RetryCompensation => "saga.retry_compensation",
            Self::RetryResolution => "saga.retry_resolution",
            Self::ReadAudit => "saga.audit.read",
            Self::ListInstances => "saga.instance.list",
            Self::ManualClose => "saga.manual_close",
        }
    }
}

/// 业务作用：封闭管理上下文、查询与写动作的确定性拒绝，供协议层稳定映射状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SagaManagementError {
    /// 调用主体、原因或其它审计上下文不符合持久化边界。
    InvalidContext,
    /// 已认证主体缺少当前动作的最小权限。
    PermissionDenied,
    /// 目标实例不存在或不属于已授权租户。
    NotFound,
    /// 当前租户的管理动作窗口预算已耗尽。
    RateLimitExceeded,
    /// 实例状态、控制态、恢复证据或 CAS 快照不允许当前动作。
    PreconditionFailed,
    /// 稳定管理操作身份已绑定另一份动作或审计事实。
    OperationConflict,
}

impl SagaManagementError {
    /// 业务作用：从可能附加事务上下文的错误链提取可公开管理拒绝类别。
    ///
    /// 参数说明：`error` 是管理查询或动作返回的完整错误链。
    ///
    /// 返回：命中管理拒绝或后端唯一身份冲突时返回对应类别；基础设施或未知失败返回 `None`。
    pub fn from_error(error: &anyhow::Error) -> Option<Self> {
        error
            .chain()
            .find_map(|cause| cause.downcast_ref::<Self>().copied())
            .or_else(|| {
                error
                    .chain()
                    .any(|cause| {
                        cause
                            .downcast_ref::<nasaga_backend::SagaBackendError>()
                            .is_some_and(|error| {
                                error.kind() == nasaga_backend::SagaBackendErrorKind::Conflict
                            })
                    })
                    .then_some(Self::OperationConflict)
            })
    }
}

impl std::fmt::Display for SagaManagementError {
    /// 业务作用：输出不暴露实例存在性、租户用量或业务状态细节的稳定管理错误摘要。
    ///
    /// 参数说明：`formatter` 是标准格式化输出目标。
    ///
    /// 返回：摘要写入成功时返回 `Ok`；格式化失败返回对应错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidContext => "Saga management context is invalid",
            Self::PermissionDenied => "Saga management permission is denied",
            Self::NotFound => "Saga management target was not found",
            Self::RateLimitExceeded => "saga_tenant_action_rate_exceeded",
            Self::PreconditionFailed => "Saga management precondition is not satisfied",
            Self::OperationConflict => "Saga management operation has different audit facts",
        })
    }
}

impl std::error::Error for SagaManagementError {}

/// 业务作用：携带认证主体、操作原因和冻结权限集，是全部人工控制入口的强制参数。
///
/// `actor` 必须是认证层给出的稳定主体 id，禁止直接使用请求体中的显示名；`reason`
/// 是事故单/变更单关联所需的人类可读原因，不得包含控制字符或敏感 payload。
#[derive(Debug, Clone)]
pub struct SagaManagementContext {
    actor: String,
    reason: String,
    permissions: BTreeSet<SagaManagementPermission>,
}

/// 业务作用：冻结调用方观察到的 Saga 状态与控制版本，使管理动作在同一事务内拒绝过期快照。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SagaManagementExpectation {
    expected_state_version: Option<u64>,
    expected_control_version: Option<u64>,
}

impl SagaManagementExpectation {
    /// 业务作用：构造管理动作的可选双版本 CAS 合同。
    ///
    /// 参数说明：两个 expected version 分别约束业务状态和控制状态；未提供的一侧不参与比较。
    ///
    /// 返回：返回不可变前置条件，最终比较必须在持有实例事务锁后执行。
    pub fn new(expected_state_version: Option<u64>, expected_control_version: Option<u64>) -> Self {
        Self {
            expected_state_version,
            expected_control_version,
        }
    }

    /// 业务作用：在实例已进入当前数据库事务后复验调用方快照，防止管理动作覆盖并发推进。
    ///
    /// 参数说明：`state_version` 与 `control_version` 来自事务内刚装载的实例行。
    ///
    /// 返回：已提供版本全部一致时成功；任一过期返回稳定并发裁决。
    pub(crate) fn verify(self, state_version: u64, control_version: u64) -> anyhow::Result<()> {
        if self
            .expected_state_version
            .is_some_and(|expected| expected != state_version)
            || self
                .expected_control_version
                .is_some_and(|expected| expected != control_version)
        {
            return Err(crate::SagaConcurrencyError::StaleSnapshot.into());
        }
        Ok(())
    }
}

/// 业务作用：聚合一次有界管理查询返回的 attempt、迁移、控制、人工恢复与冲突事实。
///
/// 每个集合都由同一租户门禁保护；高基数业务身份只出现在本管理响应，不进入指标标签。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaAuditTrail {
    /// 命令投递与结果终态事实。
    pub attempts: Vec<SagaStepAttemptRow>,
    /// 业务状态迁移审计链。
    pub transitions: Vec<SagaTransitionAuditRow>,
    /// pause/resume 控制态主体审计。
    pub controls: Vec<SagaControlAuditRow>,
    /// 人工恢复业务动作审计。
    pub management_operations: Vec<SagaManagementAuditRow>,
    /// 互斥结果事实。
    pub conflicts: Vec<SagaConflictFactRow>,
}

/// 业务作用：携带最后已交付的全局审计事件序号，支持跨类别追加与断线续读。
///
/// `audit_seq` 由数据库在事实事务内单调分配；attempt 状态变化会生成新事件而非覆盖
/// 已交付位置。协议层必须使用服务端密钥认证游标序列化结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SagaAuditPageCursor {
    /// 最后一条已返回事件的全局序号。
    pub audit_seq: u64,
}

/// 业务作用：用封闭类别承载统一审计页中的一条事实，不向协议层暴露数据库表选择逻辑。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SagaAuditRecord {
    /// command attempt 事实及其开始时间。
    Attempt(SagaAttemptAuditRow),
    /// Saga 状态迁移事实。
    Transition(SagaTransitionAuditRow),
    /// pause/resume 控制态事实。
    Control(SagaControlAuditRow),
    /// 人工恢复或关闭事实。
    Management(SagaManagementAuditRow),
    /// 互斥结果证据。
    Conflict(SagaConflictFactRow),
}

/// 业务作用：返回单一有界审计记录流及继续读取所需的稳定游标。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaAuditPage {
    /// 按数据库全局 `audit_seq` 严格递增的不可变事实快照。
    pub records: Vec<SagaAuditRecord>,
    /// 本页非空时指向末项，可作为后续追加事实的持久 checkpoint；空页为 `None`。
    pub next_cursor: Option<SagaAuditPageCursor>,
}

impl SagaManagementContext {
    /// 业务作用：校验并冻结一次管理调用的主体、原因和权限快照。
    ///
    /// 参数说明：
    /// - `actor`: 已认证稳定主体 id，最多 128 字节。
    /// - `reason`: 操作原因或工单摘要，最多 512 字节。
    /// - `permissions`: 认证层为当前请求授予的最小权限集合。
    ///
    /// 返回：字段合法时返回上下文；空值、首尾空白、控制字符或超长输入返回错误。
    pub fn new(
        actor: impl Into<String>,
        reason: impl Into<String>,
        permissions: impl IntoIterator<Item = SagaManagementPermission>,
    ) -> anyhow::Result<Self> {
        let actor = actor.into();
        let reason = reason.into();
        if !valid_audit_text(&actor, 128) || !valid_audit_text(&reason, 512) {
            return Err(SagaManagementError::InvalidContext.into());
        }
        let permissions = permissions.into_iter().collect::<BTreeSet<_>>();
        Ok(Self {
            actor,
            reason,
            permissions,
        })
    }

    /// 业务作用：读取认证主体稳定 id，供同事务审计持久化。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：actor 字符串切片。
    pub fn actor(&self) -> &str {
        &self.actor
    }

    /// 业务作用：读取本次管理动作原因，供同事务审计与事故复盘。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：reason 字符串切片。
    pub fn reason(&self) -> &str {
        &self.reason
    }

    /// 业务作用：在任何持久化读取或副作用前执行最小权限门禁。
    ///
    /// 参数说明：
    /// - `required`: 当前管理动作要求的权限。
    ///
    /// 返回：权限存在返回成功；缺失时返回不含业务数据的拒绝错误。
    pub fn require(&self, required: SagaManagementPermission) -> anyhow::Result<()> {
        if self.permissions.contains(&required) {
            Ok(())
        } else {
            Err(SagaManagementError::PermissionDenied.into())
        }
    }
}

/// 业务作用：校验进入持久化审计列的文本边界，避免截断、日志注入和不可见主体。
///
/// 参数说明：
/// - `value`: actor 或 reason。
/// - `max_len`: 对应数据库列的字节上限。
///
/// 返回：非空、已修剪、无控制字符且长度有界时返回真。
fn valid_audit_text(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
        && value.trim() == value
        && !value.chars().any(char::is_control)
}
