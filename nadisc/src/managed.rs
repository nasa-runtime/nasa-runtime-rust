//! 受管发现的后端事实与有限操作合同。后端不自行创建续约或重注册循环。

use std::{collections::BTreeMap, fmt, time::Duration};

use crate::Instance;

/// 可用于稳定状态与重试裁决的错误类别，不携带远端响应正文。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorCode {
    InvalidConfig,
    Unsupported,
    Unauthenticated,
    PermissionDenied,
    PermissionFiltered,
    Unavailable,
    RateLimited,
    DeadlineExceeded,
    /// 响应未能完整同步，不能据此发布新候选；普通读取可以使用尚在宽限内的完整旧证据。
    IncompleteSnapshot,
    /// 已确认完整候选或对象身份非法，必须立即撤销旧证据，不能按同步失败宽限。
    InvalidSnapshot,
    CapacityExceeded,
    OutcomeUnknown,
    NoInstances,
    OwnerClosed,
    NotReady,
    Conflict,
}

/// 携带稳定原因与配置路径的发现错误；秘密材料由 adapter 内部管理。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryError {
    pub code: ErrorCode,
    pub reason: &'static str,
    /// 只包含配置字段与已校验的条目名称，不包含配置值或认证材料。
    pub config_path: Option<String>,
}

impl DiscoveryError {
    /// 业务作用：构造可安全展示的后端裁决。
    /// 参数说明：`code` 是稳定类别，`reason` 是不包含凭据的静态原因。
    /// 返回：不保留任意 SDK 响应正文的错误。
    pub const fn new(code: ErrorCode, reason: &'static str) -> Self {
        Self {
            code,
            reason,
            config_path: None,
        }
    }

    /// 业务作用：为配置拒绝补充可定位的字段路径，同时保留稳定原因与错误类别。
    /// 参数说明：`prefix` 只能包含配置字段或已经校验的条目名称，不得传入配置值。
    /// 返回：路径按外层到内层拼接的错误，不保留后端响应正文。
    pub fn at_config_path(mut self, prefix: impl AsRef<str>) -> Self {
        self.config_path = Some(match self.config_path.take() {
            Some(path) => format!("{}.{path}", prefix.as_ref()),
            None => prefix.as_ref().to_owned(),
        });
        self
    }

    /// 业务作用：判断失败是否明确撤销既有发现证据。
    /// 参数说明：无。
    /// 返回：鉴权撤销、身份冲突、已确认非法候选和生命周期终止不允许使用历史路由；单纯同步不完整不构成立即撤权事实。
    pub fn revokes_evidence(&self) -> bool {
        matches!(
            self.code,
            ErrorCode::Unauthenticated
                | ErrorCode::PermissionDenied
                | ErrorCode::PermissionFiltered
                | ErrorCode::OwnerClosed
                | ErrorCode::Conflict
                | ErrorCode::InvalidSnapshot
        )
    }
}

impl fmt::Display for DiscoveryError {
    /// 业务作用：输出稳定类别和已脱敏原因。
    /// 参数说明：`formatter` 接收错误文本。
    /// 返回：仅格式化配置路径、稳定类别和公开原因。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(path) = &self.config_path {
            write!(formatter, "{path}: ")?;
        }
        write!(formatter, "{:?}: {}", self.code, self.reason)
    }
}

impl std::error::Error for DiscoveryError {}

pub type Result<T> = std::result::Result<T, DiscoveryError>;

/// 证据只说明其来源，不替代业务协议认证。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvidenceKind {
    Static,
    Address,
    BackendHealth,
}

/// 后端实际支持的操作；权限不足通过操作错误另行表达。
#[derive(Clone, Copy, Debug)]
pub struct Capabilities {
    pub registration: bool,
    pub enumeration: bool,
    pub evidence: EvidenceKind,
}

/// 一次完整观察；错误与完整空集合分别返回。
#[derive(Debug)]
pub struct ProviderSnapshot {
    pub instances: Vec<Instance>,
    pub evidence: EvidenceKind,
}

/// 冻结的注册事实。字段由宿主解析完毕，重试不生成另一实例身份。
#[derive(Clone, Debug)]
pub struct RegistrationPlan {
    pub id: String,
    pub service: String,
    pub instance: Instance,
    pub attributes: BTreeMap<String, String>,
}

/// 注册查询不能用“健康列表中没找到”猜测管理端意图。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistrationEvidence {
    Healthy,
    Unhealthy,
    Disabled,
    Missing,
}

/// 发布操作必须在实际传输提交处复验，鉴权等待不能提前消耗一次性许可。
pub trait WriteAdmission: Send + Sync {
    /// 业务作用：在发布资格保护范围内同步提交一次后端写入。
    /// 参数说明：`submit` 只能启动传输，不得等待或重入资格接口。
    /// 返回：撤权时不执行回调；已经提交的操作由 owner 记录结果不明状态。
    fn submit(&self, submit: &mut dyn FnMut()) -> Result<()>;
}

/// 每个方法代表一次有界操作，调用者拥有 deadline、取消、续约与恢复策略。
#[async_trait::async_trait]
pub trait ManagedProvider: Send + Sync + 'static {
    /// 业务作用：报告此适配器的能力边界。
    /// 参数说明：无。
    /// 返回：静态、解析和后端健康证据不互相冒充。
    fn capabilities(&self) -> Capabilities;

    /// 业务作用：在声明的服务作用域内确认出站能力，不要求目标已有实例。
    /// 参数说明：无。
    /// 返回：认证或作用域访问失败时拒绝连接可用状态。
    async fn prepare(&self) -> Result<()>;

    /// 业务作用：取得某个受信服务目标的完整实例事实。
    /// 参数说明：`service` 为连接内服务名或显式绑定别名。
    /// 返回：完整空集合表示无实例；截断、撤权和网络异常返回错误。
    async fn observe(&self, service: &str) -> Result<ProviderSnapshot>;

    /// 业务作用：声明后端能否提供不占用短请求额度的变更等待。
    /// 参数说明：无。
    /// 返回：最大等待时间；通知仅触发完整重读，不构成健康证据。
    fn change_wait_timeout(&self) -> Option<Duration> {
        None
    }

    /// 业务作用：为一次变更等待预留实际可能并发的后端长请求额度。
    /// 参数说明：无。
    /// 返回：默认一个请求；同时观察多类资源的 provider 必须报告合计上限。
    fn change_wait_streams(&self) -> u32 {
        1
    }

    /// 业务作用：等待服务或端点变化，调用方取消时释放当前长连接。
    /// 参数说明：`service` 与最近一次完整观察使用同一作用域。
    /// 返回：变化时重新全量观察；同步丢失时明确失败。
    async fn wait_for_change(&self, _service: &str) -> Result<()> {
        Err(DiscoveryError::new(
            ErrorCode::Unsupported,
            "change notification is unavailable",
        ))
    }

    /// 业务作用：枚举当前连接允许的服务范围。
    /// 参数说明：无。
    /// 返回：没有枚举能力时明确拒绝，不申请更大作用域。
    async fn services(&self) -> Result<Vec<String>> {
        Err(DiscoveryError::new(
            ErrorCode::Unsupported,
            "service enumeration is unavailable",
        ))
    }

    /// 业务作用：在外部写入前校验 provider 特定注册字段与所有权键。
    /// 参数说明：`plan` 是不可变注册计划。
    /// 返回：读侧 provider 或不支持的字段必须拒绝。
    fn validate_registration(&self, _plan: &RegistrationPlan) -> Result<()> {
        Err(DiscoveryError::new(
            ErrorCode::Unsupported,
            "provider is read only",
        ))
    }

    /// 业务作用：计算可核对的远端注册键，用于同一 owner 内重复目标预检。
    /// 参数说明：`plan` 是已校验注册计划。
    /// 返回：包含真实后端作用域的键；不把配置连接别名当作远端身份。
    fn registration_key(&self, plan: &RegistrationPlan) -> String {
        format!(
            "{}:{}:{}",
            plan.service, plan.instance.ip, plan.instance.port
        )
    }

    /// 业务作用：识别不同本地别名是否读取相同的后端服务集合，供宿主核对发布投影一致性。
    /// 参数说明：`service` 是已绑定的服务别名。
    /// 返回：不含凭据的后端查询键；有实际作用域的 adapter 必须包含地址、命名空间和过滤条件。
    fn service_key(&self, service: &str) -> Result<String> {
        Ok(service.to_owned())
    }

    /// 业务作用：保留同地址不同后端实例的身份，避免发现层按 IP/端口错误合并。
    /// 参数说明：`instance` 来自本 provider 的完整观察。
    /// 返回：当前服务作用域内的稳定非空键；后端有实例 ID 时应覆盖此默认地址键。
    fn instance_key(&self, instance: &Instance) -> String {
        format!(
            "{}:{}:{:?}",
            instance.ip, instance.port, instance.cluster_name
        )
    }

    /// 业务作用：执行一次注册，不在方法外另起自动重放。
    /// 参数说明：`plan` 保留本次实例身份与端点，`admission` 必须用于实际传输提交。
    /// 返回：远端接纳或明确错误，传输结果不明由 owner 保留待核对记录。
    async fn register(
        &self,
        _plan: &RegistrationPlan,
        _admission: &dyn WriteAdmission,
    ) -> Result<()> {
        Err(DiscoveryError::new(
            ErrorCode::Unsupported,
            "provider is read only",
        ))
    }

    /// 业务作用：续约既有记录或上报本地健康，不覆盖管理端禁用属性。
    /// 参数说明：`plan` 是既有注册，`ready` 是当前本地资格，`admission` 防止鉴权等待后继续发布。
    /// 返回：续约失败不能隐式变成重新注册。
    async fn renew(
        &self,
        _plan: &RegistrationPlan,
        _ready: bool,
        _admission: &dyn WriteAdmission,
    ) -> Result<()> {
        Ok(())
    }

    /// 业务作用：提供本后端需要的最大发送间隔。
    /// 参数说明：`plan` 指定检查模式与有效时限。
    /// 返回：无需主动保活时为空；否则 owner 按间隔监督。
    fn renewal_interval(&self, _plan: &RegistrationPlan) -> Option<Duration> {
        None
    }

    /// 业务作用：声明同一冻结身份与相同健康 payload 的续约能否在响应丢失后重复提交。
    /// 参数说明：`plan` 是已成功注册且尚未开始注销的固定计划。
    /// 返回：默认不允许；返回 true 的 provider 必须保证重复续约不重建身份、不覆盖管理属性，且不把它视为先前请求已停止执行的证明。
    fn supports_idempotent_renewal(&self, _plan: &RegistrationPlan) -> bool {
        false
    }

    /// 业务作用：核对本实例的在册及健康事实。
    /// 参数说明：`plan` 决定精确注册身份。
    /// 返回：查询失败与缺失、禁用、不健康明确区分。
    async fn inspect(&self, _plan: &RegistrationPlan) -> Result<RegistrationEvidence> {
        Err(DiscoveryError::new(
            ErrorCode::Unsupported,
            "registration observation is unavailable",
        ))
    }

    /// 业务作用：删除当前计划的远端记录，不消费本地事实。
    /// 参数说明：`plan` 用于确定注销目标。
    /// 返回：失败后 owner 仍能报告残留并安排同身份清理。
    async fn deregister(&self, _plan: &RegistrationPlan) -> Result<()> {
        Err(DiscoveryError::new(
            ErrorCode::Unsupported,
            "provider is read only",
        ))
    }

    /// 业务作用：停止该适配器的受管会话并禁止后续操作。
    /// 参数说明：无。
    /// 返回：内部资源已释放或由有界调用返回失败。
    async fn close(&self) -> Result<()>;
}
