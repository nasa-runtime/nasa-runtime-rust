//! 动态 Definition Catalog 与参与方能力目录使用的公开数据合同。
//!
//! 完整流程顺序只来自 [`DefinitionArtifact`]。参与方能力只能证明某个 owner 当前能够执行
//! 其中一个步骤，不能反向拼装流程。

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use nasaga_core::{
    CancelMode, Compensation, DefinitionVersion, ResolutionMode, ResolutionSpec, ServiceIdentity,
    StepDefinition, StepName, TimeoutPolicy, WorkflowDefinition, WorkflowName,
    OPAQUE_IDENTIFIER_MAX_BYTES, STRUCTURED_IDENTIFIER_MAX_BYTES,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use url::Url;

use crate::DefinitionRegistry;

/// capability 审计对象键的最大字符数：tenant、四个结构性身份、u32 十进制版本和五个分隔符。
pub const CATALOG_OBJECT_KEY_MAX_LEN: usize =
    OPAQUE_IDENTIFIER_MAX_BYTES + STRUCTURED_IDENTIFIER_MAX_BYTES * 4 + 10 + 5;

/// 业务作用：表示 Catalog 中不可变 definition 的生命周期。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DefinitionLifecycle {
    /// 已持久化但尚不能创建实例。
    Candidate,
    /// 已通过控制面门禁，可以创建新实例。
    Active,
    /// 禁止创建新实例，但继续服务已有实例。
    Deprecated,
    /// 已确认不存在运行期引用，不再装载。
    Retired,
}

impl DefinitionLifecycle {
    /// 业务作用：返回数据库和协议共同使用的稳定名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：生命周期的稳定文本。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::Active => "active",
            Self::Deprecated => "deprecated",
            Self::Retired => "retired",
        }
    }
}

/// 业务作用：表达定义发布的幂等裁决，调用方无需解析错误文本。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DefinitionPublishDisposition {
    /// 首次写入候选定义。
    Published,
    /// 相同键与摘要已存在，持久内容未变化。
    Duplicate,
}

/// 业务作用：封闭 Definition Catalog 可由协议层安全公开的确定性拒绝类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DefinitionCatalogError {
    /// 请求字段不满足 Catalog 的封闭格式或范围合同。
    InvalidArgument,
    /// 认证主体不是目标 definition 或 capability 的持有者。
    PermissionDenied,
    /// 目标 definition 不存在。
    NotFound,
    /// 生命周期、集群确认或 capability 等业务前置条件尚未成立。
    FailedPrecondition,
    /// 相同 tenant/workflow/version 已绑定另一份不可变摘要。
    DigestConflict,
    /// 调用方预期的 definition seal 已不再是当前持久事实。
    PreconditionFailed,
    /// 相同操作身份已绑定另一组动作参数。
    OperationConflict,
}

impl DefinitionCatalogError {
    /// 业务作用：从持久事务上下文包裹的错误链提取 Catalog 确定性拒绝。
    ///
    /// 参数说明：`error` 是 Catalog 写入口返回的完整错误链。
    ///
    /// 返回：命中封闭拒绝时返回对应类别；其它失败返回 `None`。
    pub fn from_error(error: &anyhow::Error) -> Option<Self> {
        error
            .chain()
            .find_map(|cause| cause.downcast_ref::<Self>().copied())
    }
}

impl std::fmt::Display for DefinitionCatalogError {
    /// 业务作用：输出不携带 definition 正文或租户信息的稳定 Catalog 拒绝摘要。
    ///
    /// 参数说明：`formatter` 是标准格式化输出目标。
    ///
    /// 返回：摘要写入成功时返回 `Ok`；格式化失败返回对应错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidArgument => "Saga Catalog request is invalid",
            Self::PermissionDenied => "Saga Catalog actor is not authorized for this object",
            Self::NotFound => "Saga definition was not found",
            Self::FailedPrecondition => "Saga Catalog business precondition is not satisfied",
            Self::DigestConflict => "Saga definition key has a different immutable digest",
            Self::PreconditionFailed => "Saga definition digest no longer matches",
            Self::OperationConflict => {
                "Saga definition operation identity has different parameters"
            }
        })
    }
}

impl std::error::Error for DefinitionCatalogError {}

/// 业务作用：声明步骤的补偿能力。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DefinitionCompensation {
    /// 步骤成功后可进入逆序补偿计划。
    Compensable,
    /// 步骤是不可撤销 pivot。
    NonCompensable,
}

impl From<DefinitionCompensation> for Compensation {
    /// 业务作用：把控制面补偿声明转换为状态机强类型。
    ///
    /// 参数说明：`value` 是公开补偿枚举。
    ///
    /// 返回：对应的核心补偿类型。
    fn from(value: DefinitionCompensation) -> Self {
        match value {
            DefinitionCompensation::Compensable => Self::Compensable,
            DefinitionCompensation::NonCompensable => Self::NonCompensable,
        }
    }
}

impl From<Compensation> for DefinitionCompensation {
    /// 业务作用：把状态机补偿类型投影为可持久化字段。
    ///
    /// 参数说明：`value` 是已校验步骤的补偿能力。
    ///
    /// 返回：稳定的控制面补偿枚举。
    fn from(value: Compensation) -> Self {
        match value {
            Compensation::Compensable => Self::Compensable,
            Compensation::NonCompensable => Self::NonCompensable,
        }
    }
}

/// 业务作用：声明步骤的取消屏障形态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DefinitionCancelMode {
    /// 本地 gate 与业务写竞争同一事务。
    LocalFenceable,
    /// 通过外部幂等取消命令建立屏障。
    ExternallyCancellable,
    /// 无法取消，只能查询或对账。
    ResolveOnly,
}

impl From<DefinitionCancelMode> for CancelMode {
    /// 业务作用：把控制面取消声明转换为状态机强类型。
    ///
    /// 参数说明：`value` 是公开取消枚举。
    ///
    /// 返回：对应的核心取消形态。
    fn from(value: DefinitionCancelMode) -> Self {
        match value {
            DefinitionCancelMode::LocalFenceable => Self::LocalFenceable,
            DefinitionCancelMode::ExternallyCancellable => Self::ExternallyCancellable,
            DefinitionCancelMode::ResolveOnly => Self::ResolveOnly,
        }
    }
}

impl From<CancelMode> for DefinitionCancelMode {
    /// 业务作用：把状态机取消形态投影为可持久化字段。
    ///
    /// 参数说明：`value` 是已校验步骤的取消形态。
    ///
    /// 返回：稳定的控制面取消枚举。
    fn from(value: CancelMode) -> Self {
        match value {
            CancelMode::LocalFenceable => Self::LocalFenceable,
            CancelMode::ExternallyCancellable => Self::ExternallyCancellable,
            CancelMode::ResolveOnly => Self::ResolveOnly,
        }
    }
}

/// 业务作用：声明未知结果的解决通道。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DefinitionResolutionMode {
    /// 外部系统主动回调确定结果。
    Callback,
    /// 参与方按稳定效果身份主动查询。
    Poll,
    /// 交由受审计人工裁决。
    Manual,
}

impl From<DefinitionResolutionMode> for ResolutionMode {
    /// 业务作用：把控制面解决模式转换为状态机强类型。
    ///
    /// 参数说明：`value` 是公开解决模式。
    ///
    /// 返回：对应的核心解决模式。
    fn from(value: DefinitionResolutionMode) -> Self {
        match value {
            DefinitionResolutionMode::Callback => Self::Callback,
            DefinitionResolutionMode::Poll => Self::Poll,
            DefinitionResolutionMode::Manual => Self::Manual,
        }
    }
}

impl From<ResolutionMode> for DefinitionResolutionMode {
    /// 业务作用：把状态机解决模式投影为可持久化字段。
    ///
    /// 参数说明：`value` 是已校验步骤的解决通道。
    ///
    /// 返回：稳定的控制面解决枚举。
    fn from(value: ResolutionMode) -> Self {
        match value {
            ResolutionMode::Callback => Self::Callback,
            ResolutionMode::Poll => Self::Poll,
            ResolutionMode::Manual => Self::Manual,
        }
    }
}

/// 业务作用：表达禁止未知结果或为未知结果配置有界解决通道。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DefinitionResolution {
    /// 必须是文本 `forbidden`，表示本地事务结果总是可知。
    Forbidden(String),
    /// 允许未知结果，并声明解决通道和总预算。
    Allowed {
        /// 解决通道。
        mode: DefinitionResolutionMode,
        /// 从首次未知结果开始计算的总预算毫秒数。
        budget_ms: u64,
    },
}

/// 业务作用：声明步骤超时后的迟到成功裁决。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DefinitionTimeoutPolicy {
    /// 接受迟到成功并继续正向流程。
    AcceptLateSuccess,
    /// 把迟到成功纳入冻结补偿计划。
    CompensateLateSuccess,
}

impl From<DefinitionTimeoutPolicy> for TimeoutPolicy {
    /// 业务作用：把控制面超时策略转换为状态机强类型。
    ///
    /// 参数说明：`value` 是公开超时策略。
    ///
    /// 返回：对应的核心策略。
    fn from(value: DefinitionTimeoutPolicy) -> Self {
        match value {
            DefinitionTimeoutPolicy::AcceptLateSuccess => Self::AcceptLateSuccess,
            DefinitionTimeoutPolicy::CompensateLateSuccess => Self::CompensateLateSuccess,
        }
    }
}

impl From<TimeoutPolicy> for DefinitionTimeoutPolicy {
    /// 业务作用：把状态机超时策略投影为可持久化字段。
    ///
    /// 参数说明：`value` 是已校验步骤的迟到结果策略。
    ///
    /// 返回：稳定的控制面超时策略。
    fn from(value: TimeoutPolicy) -> Self {
        match value {
            TimeoutPolicy::AcceptLateSuccess => Self::AcceptLateSuccess,
            TimeoutPolicy::CompensateLateSuccess => Self::CompensateLateSuccess,
        }
    }
}

/// 业务作用：完整描述 definition 中一个有序步骤的业务和失败语义。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefinitionStepArtifact {
    /// 步骤正文合同；缺省为无 schema 的 JSON，非默认值参与 definition seal。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_contract: Option<nasaga_core::SagaPayloadContract>,
    /// 稳定步骤名称。
    pub name: String,
    /// 唯一有权作证结果的逻辑服务身份。
    pub owner: String,
    /// 补偿能力。
    pub compensation: DefinitionCompensation,
    /// 取消屏障形态。
    pub cancel_mode: DefinitionCancelMode,
    /// 未知结果解决声明。
    pub resolution: DefinitionResolution,
    /// 正向执行期限毫秒数。
    pub timeout_ms: u64,
    /// 迟到成功策略。
    pub timeout_policy: DefinitionTimeoutPolicy,
}

/// 业务作用：承载 workflow owner 发布的完整、有序且带 seal 的流程合同。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefinitionArtifact {
    /// 定义适用的租户域。
    pub tenant: String,
    /// 获授权发布完整流程的逻辑主体。
    pub workflow_owner: String,
    /// 稳定 workflow 名称。
    pub workflow: String,
    /// 从一开始递增的不可变定义版本。
    pub definition_version: u32,
    /// 按正向顺序排列的完整步骤集合。
    pub steps: Vec<DefinitionStepArtifact>,
    /// 发布者根据完整 definition 计算的 canonical 摘要。
    pub seal: String,
}

impl DefinitionArtifact {
    /// 业务作用：转换为状态机 definition，并复验 seal 覆盖完整业务语义。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：字段、流程不变量与 seal 全部成立时返回 definition；否则拒绝进入 Catalog。
    pub fn to_definition(&self) -> anyhow::Result<WorkflowDefinition> {
        let workflow =
            WorkflowName::new(&self.workflow).map_err(|error| anyhow::anyhow!(error.code()))?;
        let version = DefinitionVersion::new(self.definition_version)
            .map_err(|error| anyhow::anyhow!(error.code()))?;
        let mut steps = Vec::with_capacity(self.steps.len());
        for artifact in &self.steps {
            let resolution = match &artifact.resolution {
                DefinitionResolution::Forbidden(value) if value == "forbidden" => {
                    ResolutionSpec::forbidden()
                }
                DefinitionResolution::Forbidden(_) => {
                    anyhow::bail!("definition resolution text must be `forbidden`");
                }
                DefinitionResolution::Allowed { mode, budget_ms } => {
                    ResolutionSpec::allowed((*mode).into(), Duration::from_millis(*budget_ms))
                }
            };
            steps.push(
                StepDefinition::new(
                    StepName::new(&artifact.name).map_err(|error| anyhow::anyhow!(error.code()))?,
                    ServiceIdentity::new(&artifact.owner)
                        .map_err(|error| anyhow::anyhow!(error.code()))?,
                    artifact.compensation.into(),
                    artifact.cancel_mode.into(),
                    resolution,
                    Duration::from_millis(artifact.timeout_ms),
                    artifact.timeout_policy.into(),
                )
                .with_payload_contract(artifact.payload_contract.clone().unwrap_or_default())?,
            );
        }
        let definition = WorkflowDefinition::new(workflow, version, steps)
            .map_err(|error| anyhow::anyhow!(error.code()))?;
        if definition.digest() != self.seal {
            anyhow::bail!("definition seal does not match canonical workflow content");
        }
        Ok(definition)
    }

    /// 业务作用：从已校验 definition 生成可发布 artifact，确保 seal 与实际步骤合同同源。
    ///
    /// 参数说明：`tenant` 是租户域，`workflow_owner` 是发布主体，`definition` 是完整流程。
    ///
    /// 返回：字段顺序稳定且 seal 已计算的 artifact。
    pub fn from_definition(
        tenant: impl Into<String>,
        workflow_owner: impl Into<String>,
        definition: &WorkflowDefinition,
    ) -> Self {
        let steps = definition
            .steps()
            .iter()
            .map(|step| DefinitionStepArtifact {
                payload_contract: (step.payload_contract()
                    != &nasaga_core::SagaPayloadContract::default())
                    .then(|| step.payload_contract().clone()),
                name: step.name().as_str().to_owned(),
                owner: step.owner().as_str().to_owned(),
                compensation: step.compensation().into(),
                cancel_mode: step.cancel_mode().into(),
                resolution: match step.resolution().mode() {
                    Some(mode) => DefinitionResolution::Allowed {
                        mode: mode.into(),
                        budget_ms: u64::try_from(step.resolution().budget().as_millis())
                            .unwrap_or(u64::MAX),
                    },
                    None => DefinitionResolution::Forbidden("forbidden".to_owned()),
                },
                timeout_ms: u64::try_from(step.timeout().as_millis()).unwrap_or(u64::MAX),
                timeout_policy: step.timeout_policy().into(),
            })
            .collect();
        Self {
            tenant: tenant.into(),
            workflow_owner: workflow_owner.into(),
            workflow: definition.name().as_str().to_owned(),
            definition_version: definition.version().get(),
            steps,
            seal: definition.digest(),
        }
    }
}

/// 业务作用：返回持久 Catalog 中 definition 的内容与当前 generation。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefinitionRecord {
    /// 完整流程 artifact。
    pub artifact: DefinitionArtifact,
    /// 当前生命周期。
    pub lifecycle: DefinitionLifecycle,
    /// 最近一次生命周期变更形成的 Catalog generation。
    pub catalog_generation: u64,
    /// definition 首次持久化的数据库时间。
    pub published_at_ms: i64,
    /// definition 首次进入 active 的数据库时间；候选尚未激活时为空。
    pub activated_at_ms: Option<i64>,
}

/// 业务作用：冻结一次 definition 激活必须满足的集群快照与数据面路由合同。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionActivationGate {
    orchestrator_service_identity: ServiceIdentity,
    catalog_generation: u64,
    snapshot_digest: String,
    transport: String,
    redis_key_tag: Option<String>,
    address_policy_digest: Option<String>,
    publisher_contract_digest: Option<String>,
    result_backend_digest: Option<String>,
    result_contract_digests: BTreeMap<String, BTreeSet<String>>,
    activation_contract_digest: String,
}

/// 业务作用：把人工 definition 生命周期动作的 CAS、幂等身份与审计原因冻结为一个持久裁决输入。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionLifecycleOperation {
    expected_sha256: String,
    operation_id: String,
    reason: String,
}

impl DefinitionLifecycleOperation {
    /// 业务作用：校验并构造可进入 Catalog 事务的生命周期操作合同。
    ///
    /// 参数说明：`expected_sha256` 是调用方观察到的 seal，`operation_id` 是主体范围内的幂等键，
    /// `reason` 是必须持久保存的人工动作原因。
    ///
    /// 返回：字段格式和长度均安全时返回合同；空值、非法摘要或超长文本返回错误。
    pub fn new(
        expected_sha256: impl Into<String>,
        operation_id: impl Into<String>,
        reason: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let expected_sha256 = expected_sha256.into();
        let operation_id = operation_id.into();
        let reason = reason.into();
        anyhow::ensure!(
            expected_sha256.len() == 64
                && expected_sha256.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "definition expected digest is invalid"
        );
        anyhow::ensure!(
            !operation_id.is_empty()
                && operation_id == operation_id.trim()
                && operation_id.len() <= 190
                && !operation_id.chars().any(char::is_control),
            "definition operation identity is invalid"
        );
        anyhow::ensure!(
            !reason.is_empty()
                && reason == reason.trim()
                && reason.len() <= 512
                && !reason.chars().any(char::is_control),
            "definition operation reason is invalid"
        );
        Ok(Self {
            expected_sha256: expected_sha256.to_ascii_lowercase(),
            operation_id,
            reason,
        })
    }

    /// 业务作用：为 validated 自动激活构造绑定 definition seal、身份和 Catalog generation 的定长幂等操作。
    ///
    /// 参数说明：`artifact` 是待激活的完整 definition，`catalog_generation` 是已确认快照，`reason` 是持久审计原因。
    ///
    /// 返回：返回长度不受租户或流程名影响的合法操作；seal 或原因不符合生命周期合同时返回错误。
    pub fn for_validated_activation(
        artifact: &DefinitionArtifact,
        catalog_generation: u64,
        reason: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let mut canonical = Vec::new();
        for field in [
            b"nasaga-validated-activation".as_slice(),
            artifact.seal.as_bytes(),
            artifact.tenant.as_bytes(),
            artifact.workflow.as_bytes(),
        ] {
            canonical.extend_from_slice(&(field.len() as u64).to_be_bytes());
            canonical.extend_from_slice(field);
        }
        canonical.extend_from_slice(&artifact.definition_version.to_be_bytes());
        canonical.extend_from_slice(&catalog_generation.to_be_bytes());
        let mut hasher = Sha256::new();
        hasher.update(canonical);
        Self::new(
            artifact.seal.clone(),
            format!("validated-{}", hex::encode(hasher.finalize())),
            reason,
        )
    }

    /// 业务作用：返回必须在持有 definition 行锁时复验的 seal。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：规范化的小写 SHA-256。
    pub fn expected_sha256(&self) -> &str {
        &self.expected_sha256
    }

    /// 业务作用：返回人工生命周期动作的幂等身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已通过长度与非空校验的稳定身份。
    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    /// 业务作用：返回与生命周期动作不可分持久化的审计原因。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已通过长度与非空校验的原因文本。
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl DefinitionActivationGate {
    /// 业务作用：构造只能由同代 Ready 副本和指定数据面共同满足的激活门禁。
    ///
    /// 参数说明：`orchestrator_service_identity` 定位逻辑控制面，`catalog_generation` 与
    /// `snapshot_digest` 固定副本已确认快照，`transport` 固定本次部署的数据面协议。
    ///
    /// 返回：摘要和协议属于封闭集合时返回门禁；无效输入拒绝进入数据库激活事务。
    pub fn new(
        orchestrator_service_identity: ServiceIdentity,
        catalog_generation: u64,
        snapshot_digest: impl Into<String>,
        transport: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let snapshot_digest = snapshot_digest.into();
        anyhow::ensure!(
            snapshot_digest.len() == 64
                && snapshot_digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "catalog snapshot digest is invalid"
        );
        let transport = transport.into();
        anyhow::ensure!(
            matches!(
                transport.as_str(),
                "http" | "grpc" | "kafka" | "redis-stream"
            ),
            "definition activation transport is unsupported"
        );
        let mut gate = Self {
            orchestrator_service_identity,
            catalog_generation,
            snapshot_digest,
            transport,
            redis_key_tag: None,
            address_policy_digest: None,
            publisher_contract_digest: None,
            result_backend_digest: None,
            result_contract_digests: BTreeMap::new(),
            activation_contract_digest: String::new(),
        };
        gate.refresh_activation_contract_digest();
        Ok(gate)
    }

    /// 业务作用：把 Redis Cluster 的实际同槽配置绑定进 definition 激活门禁，确保数据库裁决
    /// 与稍后构造 publisher 使用同一份 route 上下文。
    ///
    /// 参数说明：`key_tag` 是运行配置要求 capability stream 首个生效 hash tag 匹配的值。
    ///
    /// 返回：仅在 transport 为 Redis Streams 且 tag 满足名称合同时返回门禁；其它组合返回错误。
    pub fn with_redis_key_tag(mut self, key_tag: impl Into<String>) -> anyhow::Result<Self> {
        let key_tag = key_tag.into();
        anyhow::ensure!(
            self.transport == "redis-stream" && valid_redis_key_tag(&key_tag),
            "definition activation Redis key tag is invalid"
        );
        self.redis_key_tag = Some(key_tag);
        self.refresh_activation_contract_digest();
        Ok(self)
    }

    /// 业务作用：把当前 transport 的受信地址政策绑定进副本确认与 definition 激活门禁。
    ///
    /// 参数说明：`digest` 是调用方对 transport 专属 canonical allowlist 计算的 SHA-256。
    ///
    /// 返回：摘要合法时返回更新后的门禁；格式无效时拒绝形成可持久确认的运行时合同。
    pub fn with_address_policy_digest(mut self, digest: impl Into<String>) -> anyhow::Result<Self> {
        let digest = digest.into();
        anyhow::ensure!(
            digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "definition activation address policy digest is invalid"
        );
        self.address_policy_digest = Some(digest.to_ascii_lowercase());
        self.refresh_activation_contract_digest();
        Ok(self)
    }

    /// 业务作用：把最终 publisher 使用的路由、凭据、client 与超时合同绑定进副本确认，
    /// 防止同一 Catalog 快照被运行参数不同的副本同时发布为权威。
    ///
    /// 参数说明：`digest` 是调用方对已解析 publisher 输入计算的 canonical SHA-256。
    ///
    /// 返回：摘要格式合法时返回更新后的门禁；格式无效时拒绝形成持久确认。
    pub fn with_publisher_contract_digest(
        mut self,
        digest: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let digest = digest.into();
        anyhow::ensure!(
            digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "definition activation publisher contract digest is invalid"
        );
        self.publisher_contract_digest = Some(digest.to_ascii_lowercase());
        self.refresh_activation_contract_digest();
        Ok(self)
    }

    /// 业务作用：把 Kafka result 返回路径的后端身份绑定进激活事务，确保参与方能力与
    /// Orchestrator consumer 使用同一套 broker 命名边界。
    ///
    /// 参数说明：`digest` 是规范化 broker 地址集合与 result topic 前缀形成的 SHA-256。
    ///
    /// 返回：仅在 transport 为 Kafka 且摘要格式合法时返回门禁；其它组合拒绝激活。
    pub fn with_result_backend_digest(mut self, digest: impl Into<String>) -> anyhow::Result<Self> {
        let digest = digest.into();
        anyhow::ensure!(
            self.transport == "kafka"
                && digest.len() == 64
                && digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "definition activation result backend digest is invalid"
        );
        self.result_backend_digest = Some(digest.to_ascii_lowercase());
        self.refresh_activation_contract_digest();
        Ok(self)
    }

    /// 业务作用：把一个 participant owner 的 result 发送凭据与 Orchestrator 接收凭据绑定进激活事务。
    ///
    /// 参数说明：`owner` 是 definition step 的服务身份，`digest` 是协议专属凭据合同的 SHA-256。
    ///
    /// 返回：身份与摘要合法时返回门禁；非法输入拒绝形成副本确认合同。
    pub fn with_result_contract_digest(
        mut self,
        owner: impl Into<String>,
        digest: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let owner = owner.into();
        ServiceIdentity::new(&owner)?;
        let digest = digest.into();
        anyhow::ensure!(
            digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "definition activation result contract digest is invalid"
        );
        self.result_contract_digests
            .entry(owner)
            .or_default()
            .insert(digest.to_ascii_lowercase());
        self.refresh_activation_contract_digest();
        Ok(self)
    }

    /// 业务作用：重算副本必须共同确认的运行时路由合同摘要。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；transport、Redis tag、地址政策与 publisher 合同按带类型边界的固定顺序写入摘要。
    fn refresh_activation_contract_digest(&mut self) {
        let mut digest = Sha256::new();
        update_snapshot_digest(&mut digest, b"nasaga-activation-contract");
        update_snapshot_digest(&mut digest, self.transport.as_bytes());
        match self.redis_key_tag.as_deref() {
            Some(key_tag) => {
                digest.update([1]);
                update_snapshot_digest(&mut digest, key_tag.as_bytes());
            }
            None => digest.update([0]),
        }
        match self.address_policy_digest.as_deref() {
            Some(policy_digest) => {
                digest.update([1]);
                update_snapshot_digest(&mut digest, policy_digest.as_bytes());
            }
            None => digest.update([0]),
        }
        match self.publisher_contract_digest.as_deref() {
            Some(publisher_digest) => {
                digest.update([1]);
                update_snapshot_digest(&mut digest, publisher_digest.as_bytes());
            }
            None => digest.update([0]),
        }
        match self.result_backend_digest.as_deref() {
            Some(result_backend_digest) => {
                digest.update([1]);
                update_snapshot_digest(&mut digest, result_backend_digest.as_bytes());
            }
            None => digest.update([0]),
        }
        digest.update((self.result_contract_digests.len() as u64).to_be_bytes());
        for (owner, contracts) in &self.result_contract_digests {
            update_snapshot_digest(&mut digest, owner.as_bytes());
            digest.update((contracts.len() as u64).to_be_bytes());
            for contract in contracts {
                update_snapshot_digest(&mut digest, contract.as_bytes());
            }
        }
        self.activation_contract_digest = hex::encode(digest.finalize());
    }

    /// 业务作用：返回有权确认本次激活快照的逻辑 Orchestrator 身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：构造门禁时已校验的长期服务身份。
    pub fn orchestrator_service_identity(&self) -> &ServiceIdentity {
        &self.orchestrator_service_identity
    }

    /// 业务作用：返回待激活 definition 所依赖的精确 Catalog generation。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：副本确认和数据库状态必须共同命中的代际。
    pub fn catalog_generation(&self) -> u64 {
        self.catalog_generation
    }

    /// 业务作用：返回全部有效 Ready 副本必须一致确认的完整快照摘要。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：六十四位十六进制摘要。
    pub fn snapshot_digest(&self) -> &str {
        &self.snapshot_digest
    }

    /// 业务作用：返回 definition 每个步骤在激活时必须具备的受管数据面协议。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：HTTP、gRPC、Kafka 或 Redis Streams 的稳定协议名。
    pub fn transport(&self) -> &str {
        &self.transport
    }

    /// 业务作用：返回 Redis Streams 激活时必须由 capability route 满足的 Cluster 同槽 tag。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Redis 激活门禁绑定的 tag；其它 transport 或尚未补齐上下文时为空。
    pub fn redis_key_tag(&self) -> Option<&str> {
        self.redis_key_tag.as_deref()
    }

    /// 业务作用：返回 Kafka candidate 必须匹配的 result broker 与 topic 命名合同摘要。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Kafka 激活门禁已绑定的摘要；其它 transport 返回空。
    pub fn result_backend_digest(&self) -> Option<&str> {
        self.result_backend_digest.as_deref()
    }

    /// 业务作用：判断 capability 声明的 result 凭据合同是否属于该 owner 的受信接收集合。
    ///
    /// 参数说明：`owner` 是步骤服务身份，`digest` 是 participant 发布的协议专属合同摘要。
    ///
    /// 返回：门禁中存在完全一致的 owner 与摘要时为真。
    pub fn accepts_result_contract(&self, owner: &str, digest: &str) -> bool {
        self.result_contract_digests
            .get(owner)
            .is_some_and(|contracts| contracts.contains(digest))
    }

    /// 业务作用：返回全部 Ready 副本必须一致确认的数据面与地址政策摘要。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：绑定 transport、Redis tag、地址政策和最终 publisher 输入的 canonical SHA-256。
    pub fn activation_contract_digest(&self) -> &str {
        &self.activation_contract_digest
    }
}

/// 业务作用：声明参与方当前可执行步骤的协议和逐实例地址事实。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityDescriptor {
    /// 本 handler 接受的正文合同，必须与 definition 中对应步骤一致。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_contract: Option<nasaga_core::SagaPayloadContract>,
    /// 能力获准服务的租户，必须与 definition key 和授权范围一致。
    pub tenant: String,
    /// 能力所属逻辑服务身份，必须与认证主体一致。
    pub owner: String,
    /// 当前副本的稳定身份，用于租约续租。
    pub replica_identity: String,
    /// 支持的 workflow。
    pub workflow: String,
    /// 支持的 definition 版本。
    pub definition_version: u32,
    /// 支持的步骤名称。
    pub step: String,
    /// 步骤补偿能力。
    pub compensation: DefinitionCompensation,
    /// 步骤取消形态。
    pub cancel_mode: DefinitionCancelMode,
    /// 是否托管未知结果解决入口。
    pub allow_unknown: bool,
    /// 托管的解决模式；禁止未知时为空。
    pub resolution_mode: Option<DefinitionResolutionMode>,
    /// command transport 的稳定名称。
    pub transport: String,
    /// 当前实例 origin 或 broker route，不包含凭据。
    pub endpoint: String,
    /// HTTP 模式下实际生效的 Saga 基础路径。
    pub effective_saga_base_path: Option<String>,
    /// result producer 与 Orchestrator 接收端共同持有的协议专属凭据或后端合同摘要。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_contract_digest: Option<String>,
    /// endpoint 与路径绑定的单调 route generation；持久值由 Catalog 在主键行锁内分配。
    pub route_generation: u64,
    /// 客户端建议的租约时长；服务端执行有界裁剪。
    pub requested_lease_ms: u64,
}

impl CapabilityDescriptor {
    /// 业务作用：计算不受续租建议影响的 capability 内容摘要，供幂等续租与 route 变更检测。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：覆盖 owner、步骤合同、transport、endpoint 与 route generation 的小写十六进制摘要。
    pub fn digest(&self) -> String {
        let mut canonical = self.clone();
        canonical.requested_lease_ms = 0;
        let bytes = serde_json::to_vec(&canonical)
            .expect("serializing a capability descriptor cannot fail");
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hex::encode(hasher.finalize())
    }

    /// 业务作用：计算不含租约建议和已分配代际的路由合同摘要，供 Catalog 判断真实内容是否变化。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：覆盖身份、步骤语义、transport、endpoint 与有效路径的稳定小写十六进制摘要。
    pub fn route_contract_digest(&self) -> String {
        let mut canonical = self.clone();
        canonical.route_generation = 0;
        canonical.requested_lease_ms = 0;
        let bytes = serde_json::to_vec(&canonical)
            .expect("serializing a capability descriptor cannot fail");
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hex::encode(hasher.finalize())
    }

    /// 业务作用：复验 capability 与 definition 中被引用步骤的不可变语义一致。
    ///
    /// 参数说明：`tenant` 是候选流程的授权租户，`definition` 是候选流程，`step` 是其中一个步骤。
    ///
    /// 返回：owner、步骤身份、版本与能力完全一致时为真。
    pub fn matches_step(
        &self,
        tenant: &str,
        definition: &WorkflowDefinition,
        step: &StepDefinition,
    ) -> bool {
        self.payload_contract.clone().unwrap_or_default() == *step.payload_contract()
            && self.tenant == tenant
            && self.workflow == definition.name().as_str()
            && self.definition_version == definition.version().get()
            && self.step == step.name().as_str()
            && self.owner == step.owner().as_str()
            && self.compensation == step.compensation().into()
            && self.cancel_mode == step.cancel_mode().into()
            && self.allow_unknown == step.resolution().allow_unknown()
            && self.resolution_mode == step.resolution().mode().map(Into::into)
    }
}

/// 业务作用：按 command transport 复验 capability 的完整可发布 route 合同，确保目录准入、
/// candidate 激活与实际 publisher 对同一 descriptor 得出一致结论。
///
/// 参数说明：`descriptor` 携带 transport、endpoint、HTTP 专属有效路径与 result 凭据合同摘要。
///
/// 返回：HTTP origin 与路径、gRPC HTTPS origin、Kafka topic 或 Redis stream 名称完整合法时成功；
/// transport 专属字段缺失、越界或出现在其它 transport 时返回参数错误。
pub fn validate_capability_route_contract(
    descriptor: &CapabilityDescriptor,
) -> Result<(), DefinitionCatalogError> {
    descriptor
        .payload_contract
        .clone()
        .unwrap_or_default()
        .validate()
        .map_err(|_| DefinitionCatalogError::InvalidArgument)?;
    let path_contract_is_valid = match descriptor.transport.as_str() {
        "http" => descriptor
            .effective_saga_base_path
            .as_deref()
            .is_some_and(valid_capability_http_base_path),
        "grpc" | "kafka" | "redis-stream" => descriptor.effective_saga_base_path.is_none(),
        _ => false,
    };
    if !path_contract_is_valid {
        return Err(DefinitionCatalogError::InvalidArgument);
    }
    let result_contract_is_valid = match descriptor.transport.as_str() {
        "http" | "grpc" | "kafka" | "redis-stream" => descriptor
            .result_contract_digest
            .as_deref()
            .is_some_and(|digest| {
                digest.len() == 64
                    && digest == digest.to_ascii_lowercase()
                    && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            }),
        _ => false,
    };
    if !result_contract_is_valid {
        return Err(DefinitionCatalogError::InvalidArgument);
    }

    let endpoint_is_valid = match descriptor.transport.as_str() {
        "http" => valid_capability_origin(&descriptor.endpoint, &["http", "https"]),
        "grpc" => valid_capability_origin(&descriptor.endpoint, &["https"]),
        "kafka" => {
            !descriptor.endpoint.is_empty()
                && descriptor.endpoint.len() <= 249
                && descriptor.endpoint.trim() == descriptor.endpoint
                && !matches!(descriptor.endpoint.as_str(), "." | "..")
                && descriptor
                    .endpoint
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        }
        "redis-stream" => validate_redis_stream_route(&descriptor.endpoint, None).is_ok(),
        _ => false,
    };
    if endpoint_is_valid {
        Ok(())
    } else {
        Err(DefinitionCatalogError::InvalidArgument)
    }
}

/// 业务作用：判断 capability 网络 endpoint 是否为 publisher 与地址政策都能安全解释的纯 origin。
///
/// 参数说明：`endpoint` 是目录输入，`schemes` 是当前 transport 允许的协议集合。
///
/// 返回：URI 含合法 host、允许协议且不携带凭据、路径、query 或 fragment 时为真。
fn valid_capability_origin(endpoint: &str, schemes: &[&str]) -> bool {
    let raw_origin_is_canonical = endpoint
        .split_once("://")
        .is_some_and(|(scheme, authority)| {
            schemes.contains(&scheme)
                && !authority.is_empty()
                && (authority.bytes().all(|byte| byte != b'/')
                    || (authority.ends_with('/')
                        && authority[..authority.len() - 1]
                            .bytes()
                            .all(|byte| byte != b'/')))
        });
    Url::parse(endpoint).is_ok_and(|url| {
        endpoint.trim() == endpoint
            && raw_origin_is_canonical
            && schemes.contains(&url.scheme())
            && url.host_str().is_some_and(|host| !host.is_empty())
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && matches!(url.path(), "" | "/")
    })
}

/// 业务作用：按 Redis Cluster 实际选 slot 的规则校验 stream 名与运行配置的同槽合同。
///
/// 参数说明：`stream` 是 capability 或 publisher 使用的完整 key，`key_tag` 是可选的受信配置值。
///
/// 返回：名称有界，且提供 tag 时首个生效 hash tag 精确匹配则成功；否则返回参数错误。
pub fn validate_redis_stream_route(
    stream: &str,
    key_tag: Option<&str>,
) -> Result<(), DefinitionCatalogError> {
    let valid = !stream.is_empty()
        && stream.len() <= 190
        && !stream.chars().any(char::is_control)
        && key_tag.is_none_or(|tag| {
            valid_redis_key_tag(tag) && first_redis_hash_tag(stream) == Some(tag)
        });
    if valid {
        Ok(())
    } else {
        Err(DefinitionCatalogError::InvalidArgument)
    }
}

/// 业务作用：校验运行配置中的 Redis Cluster hash tag 可安全嵌入 stream key。
///
/// 参数说明：`tag` 是不含花括号的配置值。
///
/// 返回：非空、有界、无控制字符和花括号时为真。
fn valid_redis_key_tag(tag: &str) -> bool {
    !tag.is_empty()
        && tag.len() <= 64
        && !tag.contains(['{', '}'])
        && !tag.chars().any(char::is_control)
}

/// 业务作用：按 Redis 服务端规则解析 key 首对花括号形成的实际 hash tag，保持 activation 与
/// publisher 的 Cluster slot 裁决一致。
///
/// 参数说明：`name` 是完整 Redis key。
///
/// 返回：首对花括号之间非空时返回 tag；空 tag、未闭合或不存在时返回空。
fn first_redis_hash_tag(name: &str) -> Option<&str> {
    let open = name.find('{')?;
    let close = name[open + 1..].find('}')?;
    if close == 0 {
        return None;
    }
    Some(&name[open + 1..open + 1 + close])
}

/// 业务作用：判断 HTTP capability 的有效 Saga path 是否能无歧义地追加固定操作名并参与签名。
///
/// 参数说明：`path` 是已经包含应用 context 的目录路径。
///
/// 返回：非根、无尾斜杠、编码、模板或特殊路径段的 canonical path 为真。
fn valid_capability_http_base_path(path: &str) -> bool {
    path != "/"
        && path.starts_with('/')
        && !path.ends_with('/')
        && path.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.' | b'~')
        })
        && !path.contains("//")
        && !path.contains('%')
        && !path.contains('?')
        && !path.contains('#')
        && !path.contains('*')
        && !path.contains('{')
        && !path.contains('}')
        && !path.split('/').any(|segment| matches!(segment, "." | ".."))
}

/// 业务作用：返回服务端接受的 capability 租约与幂等摘要。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityReceipt {
    /// 服务端数据库时钟计算的租约截止时间。
    pub accepted_until_ms: i64,
    /// 不含租约截止时间的 capability 内容摘要。
    pub capability_digest: String,
    /// Catalog 在 capability 主键行锁内确认的单调 route generation。
    pub route_generation: u64,
    /// 本次写入所处 Catalog generation。
    pub catalog_generation: u64,
}

/// 业务作用：保存能力目录中一个仍在租约内的逐实例 route。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredCapability {
    /// 参与方发布的完整能力描述。
    pub descriptor: CapabilityDescriptor,
    /// 服务端数据库时钟确定的租约截止时间。
    pub accepted_until_ms: i64,
    /// 持久化内容摘要。
    pub capability_digest: String,
}

/// 业务作用：从有效能力快照中按完整步骤身份选择唯一数据面 route，避免同租户同名 endpoint
/// 的无关能力替代目标 descriptor 参与地址政策裁决。
///
/// 参数说明：
/// - `capabilities`: 当前 Catalog generation 内仍在租约期的完整能力集合。
/// - `transport`: 当前数据面的稳定协议名。
/// - `tenant`: definition 所属租户。
/// - `definition`: 当前调度实例固定的完整 definition。
/// - `step`: definition 中待选择 route 的完整步骤合同。
///
/// 返回：没有匹配能力时返回 `Ok(None)`；所有匹配副本声明同一合法 endpoint 与有效路径，且 Kafka
/// result 后端摘要一致时返回其中一个完整 descriptor；route 非法或出现不同目标时返回错误。
pub fn select_capability_route<'a>(
    capabilities: &'a [RegisteredCapability],
    transport: &str,
    tenant: &str,
    definition: &WorkflowDefinition,
    step: &StepDefinition,
) -> Result<Option<&'a CapabilityDescriptor>, DefinitionCatalogError> {
    let mut selected: Option<&CapabilityDescriptor> = None;
    for descriptor in capabilities
        .iter()
        .map(|capability| &capability.descriptor)
        .filter(|descriptor| {
            descriptor.transport == transport && descriptor.matches_step(tenant, definition, step)
        })
    {
        validate_capability_route_contract(descriptor)?;
        if selected.is_some_and(|current| {
            current.endpoint != descriptor.endpoint
                || current.effective_saga_base_path != descriptor.effective_saga_base_path
                || (transport == "kafka"
                    && current.result_contract_digest != descriptor.result_contract_digest)
        }) {
            return Err(DefinitionCatalogError::FailedPrecondition);
        }
        selected.get_or_insert(descriptor);
    }
    Ok(selected)
}

/// 业务作用：取得同一步骤的完整逐实例直接路由集合，允许 HTTP/gRPC 副本使用独立 endpoint。
/// 参数说明：`capabilities` 是仍在租期内的同代快照；`transport`、`tenant`、`definition`、`step` 共同限定步骤合同。
/// 返回：合法 HTTP/gRPC 路由按 endpoint 与副本身份排序；消息代理仍要求唯一目标，非法或同副本冲突时拒绝。
pub fn select_capability_routes<'a>(
    capabilities: &'a [RegisteredCapability],
    transport: &str,
    tenant: &str,
    definition: &WorkflowDefinition,
    step: &StepDefinition,
) -> Result<Vec<&'a CapabilityDescriptor>, DefinitionCatalogError> {
    if !matches!(transport, "http" | "grpc") {
        return select_capability_route(capabilities, transport, tenant, definition, step)
            .map(|route| route.into_iter().collect());
    }
    let mut routes: Vec<&CapabilityDescriptor> = Vec::new();
    for descriptor in capabilities
        .iter()
        .map(|capability| &capability.descriptor)
        .filter(|descriptor| {
            descriptor.transport == transport && descriptor.matches_step(tenant, definition, step)
        })
    {
        validate_capability_route_contract(descriptor)?;
        // 同一副本不能同时自报互斥地址或结果合同，否则不得随机挑选其中一份权威。
        if routes.iter().any(|current| {
            current.replica_identity == descriptor.replica_identity && *current != descriptor
        }) {
            return Err(DefinitionCatalogError::FailedPrecondition);
        }
        routes.push(descriptor);
    }
    routes.sort_by(|left, right| {
        (&left.endpoint, &left.replica_identity).cmp(&(&right.endpoint, &right.replica_identity))
    });
    routes.dedup();
    Ok(routes)
}

/// 业务作用：把同一数据库 generation 的 definition 与有效 capability 形成不可拆分加载结果。
#[derive(Debug, Clone)]
pub struct DynamicCatalogSnapshot {
    /// 本次读取确认的共享 generation。
    pub generation: u64,
    /// definition 与 route 内容形成的确定性摘要，供多副本确认同一代快照。
    pub snapshot_digest: String,
    /// active 与仍被旧实例引用的 deprecated definition 快照。
    pub registry: DefinitionRegistry,
    /// 当前 generation 中候选与可运行 definition 的完整持久化记录。
    pub definition_records: Vec<DefinitionRecord>,
    /// 读取时仍在服务端租约内的参与方逐实例能力。
    pub capabilities: Vec<RegisteredCapability>,
}

impl DynamicCatalogSnapshot {
    /// 业务作用：把已复验的 definition 与 capability 组合为带确定性内容摘要的代际快照。
    ///
    /// 参数说明：`generation` 是共享代际，`registry` 是运行定义，`capabilities` 是有效路由。
    ///
    /// 返回：返回可供多副本确认的不可拆分快照。
    pub fn new(
        generation: u64,
        registry: DefinitionRegistry,
        capabilities: Vec<RegisteredCapability>,
    ) -> Self {
        Self::with_definition_records(generation, registry, Vec::new(), capabilities)
    }

    /// 业务作用：把 candidate、active、deprecated definition 与有效 capability 共同绑定进快照摘要。
    ///
    /// 参数说明：`generation` 是共享代际，`registry` 只承载可运行定义，`definition_records` 还包含
    /// 尚未激活的候选定义，`capabilities` 是读取时有效的逐实例路由。
    ///
    /// 返回：摘要覆盖租户、owner、生命周期、seal 和路由且字段边界无歧义的不可拆分快照。
    pub fn with_definition_records(
        generation: u64,
        registry: DefinitionRegistry,
        mut definition_records: Vec<DefinitionRecord>,
        mut capabilities: Vec<RegisteredCapability>,
    ) -> Self {
        definition_records.sort_by(|left, right| {
            (
                &left.artifact.tenant,
                &left.artifact.workflow,
                left.artifact.definition_version,
            )
                .cmp(&(
                    &right.artifact.tenant,
                    &right.artifact.workflow,
                    right.artifact.definition_version,
                ))
        });
        capabilities.sort_by(|left, right| {
            (
                &left.descriptor.tenant,
                &left.descriptor.owner,
                &left.descriptor.replica_identity,
                &left.descriptor.workflow,
                left.descriptor.definition_version,
                &left.descriptor.step,
            )
                .cmp(&(
                    &right.descriptor.tenant,
                    &right.descriptor.owner,
                    &right.descriptor.replica_identity,
                    &right.descriptor.workflow,
                    right.descriptor.definition_version,
                    &right.descriptor.step,
                ))
        });
        let mut digest = Sha256::new();
        digest.update(generation.to_be_bytes());
        // 可运行 registry 也必须进入摘要，否则静态快照或租户投影漂移时仍可伪装成同代内容。
        let mut runtime_definitions = registry
            .definitions()
            .map(|definition| {
                (
                    definition.name().as_str(),
                    definition.version().get(),
                    definition.digest(),
                )
            })
            .collect::<Vec<_>>();
        runtime_definitions.sort();
        for (workflow, version, definition_digest) in runtime_definitions {
            update_snapshot_digest(&mut digest, workflow.as_bytes());
            digest.update(version.to_be_bytes());
            update_snapshot_digest(&mut digest, definition_digest.as_bytes());
        }
        for record in &definition_records {
            update_snapshot_digest(&mut digest, record.artifact.tenant.as_bytes());
            update_snapshot_digest(&mut digest, record.artifact.workflow_owner.as_bytes());
            update_snapshot_digest(&mut digest, record.artifact.workflow.as_bytes());
            digest.update(record.artifact.definition_version.to_be_bytes());
            update_snapshot_digest(&mut digest, record.lifecycle.as_str().as_bytes());
            update_snapshot_digest(&mut digest, record.artifact.seal.as_bytes());
            digest.update(record.catalog_generation.to_be_bytes());
        }
        for capability in &capabilities {
            update_snapshot_digest(&mut digest, capability.descriptor.tenant.as_bytes());
            update_snapshot_digest(&mut digest, capability.descriptor.owner.as_bytes());
            update_snapshot_digest(
                &mut digest,
                capability.descriptor.replica_identity.as_bytes(),
            );
            update_snapshot_digest(&mut digest, capability.descriptor.workflow.as_bytes());
            digest.update(capability.descriptor.definition_version.to_be_bytes());
            update_snapshot_digest(&mut digest, capability.descriptor.step.as_bytes());
            update_snapshot_digest(&mut digest, capability.capability_digest.as_bytes());
        }
        Self {
            generation,
            snapshot_digest: hex::encode(digest.finalize()),
            registry,
            definition_records,
            capabilities,
        }
    }
}

/// 业务作用：以长度前缀把可变字段加入 Catalog 摘要，避免相邻字段拼接产生边界歧义。
///
/// 参数说明：`digest` 是当前 SHA-256 状态，`value` 是一个完整领域字段的原始字节。
///
/// 返回：无；字段长度和内容按固定顺序写入摘要状态。
fn update_snapshot_digest(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}
