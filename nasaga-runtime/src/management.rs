//! Saga 管理面主体、权限与原因合同。
//!
//! HTTP/JWT/mTLS 的认证实现属于宿主应用；运行时只接收认证层构造的不可变管理上下文，
//! 并在任何数据库读取或状态变化前执行细粒度权限检查。actor 与 reason 会进入同事务审计。

use std::collections::BTreeSet;

use nasaga_mysql::{
    SagaConflictFactRow, SagaControlAuditRow, SagaManagementAuditRow, SagaStepAttemptRow,
    SagaTransitionAuditRow,
};

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
            anyhow::bail!("invalid Saga management actor or reason");
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
            anyhow::bail!("Saga management permission denied: {}", required.as_str())
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
