//! Saga Application 生命周期组件。
//!
//! 受管模式根据角色配置、静态 descriptor 与 Definition Catalog 构造运行资源；自定义模式允许高级
//! 调用方显式提交运行计划。组件在 Ready 前完成本地步骤合同和历史非终态实例校验，成功后才发布
//! 能力并启动 durable timer。应用停机时先关闭能力入口，再由数据库等更早启动的依赖执行反向清理。

#[cfg(any(feature = "web", feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
mod api;
#[cfg(any(feature = "web", feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
use api::SagaApiActor;
mod capability_lease;
mod catalog_authority;
mod discovery;
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
use discovery::managed_grpc_discovery_target;
use discovery::managed_http_discovery_target;
#[cfg(feature = "nacos-discovery")]
pub(crate) use discovery::registration_metadata as discovery_registration_metadata;
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
mod grpc_target;
mod latency_metrics;
pub(crate) mod security;
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
mod start_payload;
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
use grpc_target::{
    build_managed_grpc_target, build_managed_grpc_targets, probe_managed_grpc_target,
    ManagedGrpcCredentialMaterial, ManagedGrpcTarget,
};

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "saga")]
use ::nasaga_runtime;
use naoutbox_core::{OutboxPublishError, OutboxPublisher};
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
use nasaga_runtime::SagaStreamPoller;
use nasaga_runtime::{DefinitionRegistry, Orchestrator, ParticipantRuntime};
#[cfg(all(not(feature = "saga"), feature = "saga-pgsql"))]
use nasaga_runtime_pgsql as nasaga_runtime;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, ReadyContext, ShutdownAction,
    ShutdownContext, StartContext,
};

const DEFAULT_TIMER_POLL_INTERVAL_MS: u64 = 500;
const DEFAULT_TIMER_ERROR_BACKOFF_MS: u64 = 1_000;
const DEFAULT_TIMER_OPERATION_TIMEOUT_MS: u64 = 5_000;
const MAX_TIMER_INTERVAL_MS: u64 = 60_000;
const MAX_TIMER_FAILURE_THRESHOLD: u32 = 100;
#[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
const MANAGED_KAFKA_RESULT_PROBE_HEADER: &str = "nasa-saga-result-probe";

/// 业务作用：冻结当前部署允许取得的 Saga 权限集合，禁止根据已链接 handler 或组件组合猜测角色。
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SagaRole {
    /// 持有全局实例、journal、timer、result Inbox 与 command Outbox 的协调角色。
    Orchestrator,
    /// 只持有本地 command Inbox、participant gate、业务事实与 result Outbox 的参与角色。
    Participant,
    /// 只取得远程发起与查询能力，不拥有本地全局状态机或参与方 gate。
    Client,
    /// 显式同时承担协调与参与职责；必须另行批准共享故障域。
    Combined,
}

/// 业务作用：选择 Saga 运行计划由框架自动构造，还是由高级调用方显式提交完整计划。
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SagaPlanMode {
    /// `napp` 从受信配置和静态 descriptor 构造全部运行资源。
    #[default]
    Managed,
    /// 兼容尚未被受管 transport 覆盖的拓扑，但仍受角色和 datasource 门禁约束。
    Custom,
}

/// 业务作用：选择流程定义来自本地不可变快照，还是共享动态 Catalog。
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DefinitionCatalogMode {
    /// 从 `#[saga_workflow]` 收集当前二进制的完整定义。
    #[default]
    Static,
    /// 从共享持久 Catalog 装载；允许初始没有 active definition。
    Dynamic,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：承载协调角色独占的数据源、身份和状态机预算。
struct SagaOrchestratorSettings {
    datasource_ref: Option<String>,
    timer_poll_interval_ms: Option<u64>,
    timer_error_backoff_ms: Option<u64>,
    timer_operation_timeout_ms: Option<u64>,
    timer_failure_threshold: Option<u32>,
    inbox_consumer: Option<String>,
    cancel_max_attempts: Option<u32>,
    compensate_max_attempts: Option<u32>,
    resolve_max_attempts: Option<u32>,
    timer_claim_limit: Option<u32>,
    timer_lease_ms: Option<i64>,
    pause_backoff_ms: Option<i64>,
    startup_scan_limit: Option<u32>,
    enable_manual_close: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：承载参与角色的长期服务身份、本地事务域与受信协调者身份。
struct SagaParticipantSettings {
    service_identity: Option<String>,
    consumer_identity: Option<String>,
    orchestrator_identity: Option<String>,
    datasource_ref: Option<String>,
    bindings: BTreeMap<String, SagaParticipantBindingSettings>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：声明一个参与方步骤集合唯一绑定的本地事务数据源。
struct SagaParticipantBindingSettings {
    datasource_ref: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：承载只读调用角色及可靠发起所需的本地事务域。
struct SagaClientSettings {
    service_identity: Option<String>,
    orchestrator_identity: Option<String>,
    orchestrator_discovery_ref: Option<String>,
    credential_ref: Option<String>,
    protocol: Option<SagaClientProtocol>,
    reliable_start: bool,
    datasource_ref: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// 业务作用：选择受管远程 Saga client 使用 HTTP 还是 generated gRPC 协议。
enum SagaClientProtocol {
    Http,
    Grpc,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：承载 Definition Catalog 模式与动态发布依赖引用。
struct SagaDefinitionCatalogSettings {
    mode: DefinitionCatalogMode,
    datasource_ref: Option<String>,
    activation_policy: Option<String>,
    watch_interval_ms: Option<u64>,
    capability_registry_ref: Option<String>,
    publisher_authorization_policy_ref: Option<String>,
    signing_keys: BTreeMap<String, String>,
    publish_tenants: Vec<String>,
    registry_client: Option<SagaRegistryClientSettings>,
}

impl Default for SagaDefinitionCatalogSettings {
    /// 业务作用：建立不隐式取得动态控制面权威的静态 Catalog 缺省配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不含数据源、发布端、租户或签名材料的本地定义模式。
    fn default() -> Self {
        Self {
            mode: DefinitionCatalogMode::Static,
            datasource_ref: None,
            activation_policy: None,
            watch_interval_ms: None,
            capability_registry_ref: None,
            publisher_authorization_policy_ref: None,
            signing_keys: BTreeMap::new(),
            publish_tenants: Vec::new(),
            registry_client: None,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：把 capability/definition 控制面客户端与 command/result 数据面解耦。
struct SagaRegistryClientSettings {
    protocol: Option<SagaClientProtocol>,
    discovery_ref: Option<String>,
    credential_ref: Option<String>,
    definition_signing_key_id: Option<String>,
    definition_signing_key_ref: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：承载 Saga 数据面 transport 的封闭选择，具体协议参数由对应受管适配器解析。
struct SagaTransportSettings {
    command_result: Option<SagaCommandResultTransportSettings>,
    address_policies: BTreeMap<String, SagaAddressPolicySettings>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：将 capability 可达的协议、主机、端口与消息命名空间收敛为受信配置边界。
struct SagaAddressPolicySettings {
    http_schemes: Vec<String>,
    http_hosts: Vec<String>,
    http_ports: Vec<u16>,
    grpc_hosts: Vec<String>,
    grpc_ports: Vec<u16>,
    kafka_topic_prefixes: Vec<String>,
    redis_stream_prefixes: Vec<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
/// 业务作用：选择 command/result 数据面的唯一协议实现。
enum SagaTransportKind {
    Http,
    Grpc,
    Kafka,
    RedisStream,
}

impl SagaTransportKind {
    /// 业务作用：返回 capability、Catalog 激活门禁与配置共同使用的稳定协议名。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：HTTP、gRPC、Kafka 或 Redis Streams 的公开协议标识。
    fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Grpc => "grpc",
            Self::Kafka => "kafka",
            Self::RedisStream => "redis-stream",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：把数据面协议类型与其专属冻结配置绑定为一个权威。
struct SagaCommandResultTransportSettings {
    kind: Option<SagaTransportKind>,
    http: Option<SagaHttpTransportSettings>,
    grpc: Option<SagaGrpcTransportSettings>,
    kafka: Option<SagaKafkaTransportSettings>,
    redis_stream: Option<SagaRedisStreamTransportSettings>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：冻结 owner route 的发现模式与出站地址政策引用。
struct SagaRoutingSettings {
    mode: Option<String>,
    address_policy_ref: Option<String>,
    static_routes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：冻结 HTTP command/result 数据面的发现、共享重放权威、凭据与有界请求预算。
struct SagaHttpTransportSettings {
    shared_replay_claim: Option<String>,
    orchestrator_discovery_ref: Option<String>,
    advertised_endpoint: Option<String>,
    routing: SagaRoutingSettings,
    command_credential_ref: Option<String>,
    result_credential_ref: Option<String>,
    producer_credentials: BTreeMap<String, String>,
    request_timeout_ms: Option<u64>,
    body_limit_bytes: Option<usize>,
    concurrency_limit: Option<usize>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：冻结 gRPC command/result 数据面的发现、route 政策、mTLS 凭据与 deadline。
struct SagaGrpcTransportSettings {
    orchestrator_discovery_ref: Option<String>,
    advertised_endpoint: Option<String>,
    routing: SagaRoutingSettings,
    credential_ref: Option<String>,
    peer_principals: BTreeMap<String, String>,
    request_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：冻结 Kafka command/result 的受管 client、topic、group、DLT 与 route 合同。
struct SagaKafkaTransportSettings {
    client_ref: Option<String>,
    command_topic: Option<String>,
    result_topic: Option<String>,
    result_group: Option<String>,
    result_dlt_topic: Option<String>,
    routing: SagaRoutingSettings,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：冻结 Redis Streams command/result 的同槽命名、consumer 身份、DLT、凭据与 route 合同。
struct SagaRedisStreamTransportSettings {
    client_ref: Option<String>,
    key_tag: Option<String>,
    command_stream: Option<String>,
    command_group: Option<String>,
    command_consumer: Option<String>,
    command_dlt_stream: Option<String>,
    result_stream: Option<String>,
    result_group: Option<String>,
    result_consumer: Option<String>,
    result_dlt_stream: Option<String>,
    credential_ref: Option<String>,
    routing: SagaRoutingSettings,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：声明 Orchestrator HTTP API 是否对外暴露及其管理面与授权政策。
struct SagaHttpApiSettings {
    enabled: bool,
    expose_admin: bool,
    expose_definition_registry: bool,
    authorization_policy_ref: Option<String>,
    callers: BTreeMap<String, SagaHttpCallerSettings>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：把一个 HTTP API 调用主体绑定到独立凭据、租户范围和封闭权限集合。
struct SagaHttpCallerSettings {
    credential_ref: Option<String>,
    tenants: Vec<String>,
    workflows: Vec<String>,
    permissions: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：声明 Orchestrator gRPC API 是否对外暴露及其管理面与授权政策。
struct SagaGrpcApiSettings {
    enabled: bool,
    expose_admin: bool,
    expose_definition_registry: bool,
    authorization_policy_ref: Option<String>,
    callers: BTreeMap<String, SagaGrpcCallerSettings>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：把一个受管 gRPC mTLS principal 绑定到逻辑主体、租户范围和封闭权限集合。
struct SagaGrpcCallerSettings {
    service_identity: Option<String>,
    tenants: Vec<String>,
    workflows: Vec<String>,
    permissions: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：把 HTTP 与 gRPC 业务 API 暴露面与 command/result transport 选择分离。
struct SagaApiSettings {
    page_token_key_ref: Option<String>,
    http: SagaHttpApiSettings,
    grpc: SagaGrpcApiSettings,
}

impl Default for SagaCommandResultTransportSettings {
    /// 业务作用：建立尚未选择 command/result 协议的空 transport 配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：所有协议块均为空，后续角色校验必须绑定唯一实现后才能进入 Ready。
    fn default() -> Self {
        Self {
            kind: None,
            http: None,
            grpc: None,
            kafka: None,
            redis_stream: None,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：冻结 Saga HTTP 在 Application context 内占用的保留子路径。
struct SagaHttpSettings {
    base_path: Option<String>,
}

/// 业务作用：在动态 Catalog 还没有任何 active definition 时保持 Outbox 事实，等待首份受信 route 快照发布。
///
/// 该发布端不会宣称网络交付成功；若在空 Catalog 窗口内出现事件，它只返回瞬态结论，使持久行留在原位等待 route 可用。
struct AwaitingCatalogPublisher;

#[async_trait::async_trait]
impl OutboxPublisher for AwaitingCatalogPublisher {
    /// 业务作用：拒绝在尚无 active definition 与受信 route 时前移任何 Outbox 事件。
    ///
    /// 参数说明：`_event` 是已提交但尚不具备路由权威的事件。
    ///
    /// 返回：始终返回瞬态失败，禁止 dispatcher 将未投递事实标记为成功或死信。
    async fn publish(&self, _event: &naoutbox_core::OutboxEvent) -> Result<(), OutboxPublishError> {
        Err(OutboxPublishError::transient(
            "Saga route snapshot is not active",
        ))
    }
}

/// 业务作用：保存一个已校验的 HTTP 实例地址与其线上有效 Saga 基础路径。
#[derive(Clone)]
struct ManagedHttpTarget {
    url: reqwest::Url,
    signed_path: String,
    authenticator: nasaga_runtime::SagaHttpMessageAuthenticator,
    discovery: Option<Arc<discovery::ManagedSagaDiscovery>>,
    timeout: Duration,
}

/// 业务作用：描述远程 Orchestrator start API 的稳定领域输入，供纯 client 角色以固定身份安全重试。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SagaRemoteStartRequest {
    /// 认证主体获准操作的租户。
    pub tenant_id: String,
    /// 调用方生成并在重试中保持不变的 Saga 身份。
    pub saga_id: String,
    /// 已激活的流程名称。
    pub workflow: String,
    /// 调用方明确选择的定义版本。
    pub definition_version: u32,
    /// 调用方可选的定义摘要兼容门禁。
    pub expected_definition_digest: Option<String>,
    /// 业务幂等键。
    pub business_key: String,
    /// 本次发起意图在重试中保持不变的触发身份。
    pub trigger_id: String,
    /// 实例级业务期限，使用 Unix 毫秒。
    pub deadline_at_ms: Option<i64>,
    /// 首个正向步骤的业务输入。
    pub input: Option<serde_json::Value>,
    /// 原始正文入口；与 input 互斥，保留 schema 身份与精确业务字节。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<nasaga_runtime::SagaPayload>,
}

/// 业务作用：表示远程 start 的两种可提交结论，二者都允许调用方停止重投同一请求。
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum SagaRemoteStartDisposition {
    /// Orchestrator 已原子创建实例、首条命令和 timer。
    Committed,
    /// 相同 Saga 身份与相同发起摘要已经提交。
    Duplicate,
}

/// 业务作用：保存远程 Orchestrator 返回的已提交 Saga 快照，不暴露数据库或 transport 实现。
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SagaRemoteSnapshot {
    /// 租户身份。
    pub tenant_id: String,
    /// Saga 身份。
    pub saga_id: String,
    /// 流程名称。
    pub workflow: String,
    /// 创建时冻结的定义版本。
    pub definition_version: u32,
    /// 创建时冻结的定义摘要。
    pub definition_digest: String,
    /// 业务幂等键。
    pub business_key: String,
    /// 当前业务状态。
    pub status: String,
    /// 与业务状态正交的管理控制状态。
    pub control_state: String,
    /// 当前推进方向。
    pub direction: String,
    /// 当前步骤；终态没有当前步骤。
    pub current_step: Option<String>,
    /// 业务状态的 CAS 版本。
    pub state_version: u64,
    /// 管理控制态的 CAS 版本。
    pub control_version: u64,
    /// 实例级业务期限，使用 Unix 毫秒。
    pub deadline_at_ms: Option<i64>,
    /// 最近一次失败的稳定原因码。
    pub failure_code: Option<String>,
    /// 最近一次持久化的 W3C traceparent。
    pub traceparent: Option<String>,
}

/// 业务作用：声明远程实例检索的租户、可选 workflow、状态集合与不透明分页边界。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SagaRemoteQueryRequest {
    /// 授权目标租户。
    pub tenant_id: String,
    /// 空值表示检索该租户全部 workflow。
    pub workflow: Option<String>,
    /// 空集合表示全部状态；非空值使用核心稳定状态名。
    pub statuses: Vec<String>,
    /// 创建时间下界，使用 Unix 毫秒；与上界共同选择时间 keyset 排序。
    pub created_from_ms: Option<i64>,
    /// 创建时间上界，使用 Unix 毫秒；空值表示不限制该方向。
    pub created_to_ms: Option<i64>,
    /// 单页上限，范围 1..=1000。
    pub page_size: u32,
    /// 上一页返回的不透明 token。
    pub page_token: Option<String>,
}

/// 业务作用：保存远程 query 返回的低敏实例摘要，不携带 payload、tenant 外数据或 transport 细节。
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SagaRemoteSummary {
    pub tenant_id: String,
    pub saga_id: String,
    pub workflow: String,
    pub definition_version: u32,
    pub business_key: String,
    pub status: String,
    pub control_state: String,
    pub direction: String,
    pub current_step: Option<String>,
    pub state_version: u64,
    pub failure_code: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// 业务作用：组合一页远程 Saga 摘要与只可用于相同过滤条件的下一页 token。
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SagaRemoteQueryPage {
    pub sagas: Vec<SagaRemoteSummary>,
    pub next_page_token: Option<String>,
}

/// 业务作用：组合远程 start 的持久收据与提交后的实例快照。
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SagaRemoteStartReceipt {
    /// 远端明确返回的提交或重复裁决。
    pub status: SagaRemoteStartDisposition,
    /// 本次裁决对应的已提交实例快照。
    pub saga: SagaRemoteSnapshot,
}

/// 业务作用：提供 Ready 后受管的远程 Saga 发起与查询能力，不取得本地 Orchestrator 权威。
pub struct SagaRemoteClient {
    transport: SagaRemoteClientTransport,
    start_intent: Option<Arc<dyn naoutbox_core::DurableOutboxAppend>>,
}

/// 业务作用：冻结远程 client 的唯一协议实现，调用方 API 不暴露连接与认证差异。
#[derive(Clone)]
enum SagaRemoteClientTransport {
    Http(Arc<SagaRemoteHttpClient>),
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    Grpc(ManagedGrpcTarget),
}

/// 业务作用：冻结远程 HTTP API 的实例路径、调用身份、凭据与响应边界。
struct SagaRemoteHttpClient {
    client: reqwest::Client,
    producer: nasaga_runtime::ServiceIdentity,
    instances_target: ManagedHttpTarget,
    response_limit_bytes: usize,
}

/// 业务作用：保存一次受管远程 HTTP 调用的状态与有界原始响应。
struct SagaRemoteHttpResponse {
    status: reqwest::StatusCode,
    body: Vec<u8>,
}

/// 业务作用：把持久 start-intent Outbox 事件投递给远程 Orchestrator，并按封闭收据决定是否前移。
struct SagaStartIntentPublisher {
    transport: SagaRemoteClientTransport,
}

impl SagaRemoteClient {
    /// 业务作用：通过受管远程 Orchestrator 发起 Saga，并把提交与重复收敛为类型化收据。
    ///
    /// 参数说明：`request` 携带稳定 Saga 身份、定义版本、业务幂等键、触发身份和首步输入。
    ///
    /// 返回：远端明确提交或确认重复时返回快照；网络结果不明、认证拒绝、合同冲突或响应越界时返回错误，调用方可用同一请求重试。
    pub async fn start(
        &self,
        request: &SagaRemoteStartRequest,
    ) -> ApplicationResult<SagaRemoteStartReceipt> {
        validate_remote_start_request(request)?;
        match &self.transport {
            SagaRemoteClientTransport::Http(http) => {
                let body = serde_json::to_vec(request).map_err(|error| {
                    saga_source_error(
                        ApplicationPhase::Running,
                        "remote Saga start request encoding failed",
                        error,
                    )
                })?;
                let response = http
                    .send_signed(
                        reqwest::Method::POST,
                        http.instances_target.url.clone(),
                        &http.instances_target.signed_path,
                        body,
                    )
                    .await?;
                if !response.status.is_success() {
                    return Err(remote_http_status_error("start", response.status));
                }
                serde_json::from_slice(&response.body).map_err(|error| {
                    saga_source_error(
                        ApplicationPhase::Running,
                        "remote Saga start receipt is invalid",
                        error,
                    )
                })
            }
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            SagaRemoteClientTransport::Grpc(target) => {
                use nasaga_runtime::orchestrator_proto as proto;
                let input = request
                    .payload
                    .clone()
                    .or_else(|| request.input.clone().map(nasaga_runtime::SagaPayload::json))
                    .map(|payload| proto::SagaPayload {
                        content_type: payload.content_type,
                        schema_id: payload.schema_id,
                        body: payload.body,
                    });
                let deadline = request.deadline_at_ms.map(managed_grpc_timestamp);
                let mut client = proto::saga_orchestrator_client::SagaOrchestratorClient::new(
                    target.channel.clone(),
                );
                let response = client
                    .start_saga(proto::StartSagaRequest {
                        key: Some(proto::SagaKey {
                            tenant_id: request.tenant_id.clone(),
                            saga_id: request.saga_id.clone(),
                        }),
                        workflow: request.workflow.clone(),
                        definition_version: request.definition_version,
                        expected_definition_digest: request
                            .expected_definition_digest
                            .clone()
                            .unwrap_or_default(),
                        business_key: request.business_key.clone(),
                        trigger_id: request.trigger_id.clone(),
                        input,
                        deadline,
                    })
                    .await
                    .map_err(|error| {
                        saga_source_error(
                            ApplicationPhase::Running,
                            "remote Saga gRPC start result is uncertain",
                            error,
                        )
                    })?
                    .into_inner();
                let status = match proto::StartDisposition::try_from(response.disposition) {
                    Ok(proto::StartDisposition::Committed) => SagaRemoteStartDisposition::Committed,
                    Ok(proto::StartDisposition::Duplicate) => SagaRemoteStartDisposition::Duplicate,
                    _ => {
                        return Err(saga_error(
                            ApplicationPhase::Running,
                            "remote Saga gRPC start receipt is invalid",
                        ))
                    }
                };
                let saga = response.saga.ok_or_else(|| {
                    saga_error(
                        ApplicationPhase::Running,
                        "remote Saga gRPC start snapshot is missing",
                    )
                })?;
                Ok(SagaRemoteStartReceipt {
                    status,
                    saga: remote_snapshot_from_grpc(saga)?,
                })
            }
        }
    }

    /// 业务作用：读取远程 Orchestrator 中同租户 Saga 的已提交快照，不在 client 进程缓存业务状态。
    ///
    /// 参数说明：`tenant_id` 与 `saga_id` 共同定位认证主体获准读取的实例。
    ///
    /// 返回：实例存在时返回快照，不存在时返回空；认证拒绝、网络结果不明或响应非法时返回错误。
    pub async fn get(
        &self,
        tenant_id: &str,
        saga_id: &str,
    ) -> ApplicationResult<Option<SagaRemoteSnapshot>> {
        nasaga_runtime::__private::core::TenantId::new(tenant_id).map_err(|error| {
            saga_source_error(
                ApplicationPhase::Running,
                "remote Saga tenant identity is invalid",
                error,
            )
        })?;
        nasaga_runtime::__private::core::SagaId::new(saga_id).map_err(|error| {
            saga_source_error(
                ApplicationPhase::Running,
                "remote Saga identity is invalid",
                error,
            )
        })?;
        match &self.transport {
            SagaRemoteClientTransport::Http(http) => {
                let signed_path = format!(
                    "{}/{}/{}",
                    http.instances_target.signed_path, tenant_id, saga_id
                );
                let mut url = http.instances_target.url.clone();
                url.set_path(&signed_path);
                let response = http
                    .send_signed(reqwest::Method::GET, url, &signed_path, Vec::new())
                    .await?;
                if response.status == reqwest::StatusCode::NOT_FOUND {
                    return Ok(None);
                }
                if !response.status.is_success() {
                    return Err(remote_http_status_error("get", response.status));
                }
                serde_json::from_slice(&response.body)
                    .map(Some)
                    .map_err(|error| {
                        saga_source_error(
                            ApplicationPhase::Running,
                            "remote Saga snapshot is invalid",
                            error,
                        )
                    })
            }
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            SagaRemoteClientTransport::Grpc(target) => {
                use nasaga_runtime::orchestrator_proto as proto;
                let mut client = proto::saga_orchestrator_client::SagaOrchestratorClient::new(
                    target.channel.clone(),
                );
                match client
                    .get_saga(proto::GetSagaRequest {
                        key: Some(proto::SagaKey {
                            tenant_id: tenant_id.to_owned(),
                            saga_id: saga_id.to_owned(),
                        }),
                    })
                    .await
                {
                    Ok(response) => remote_snapshot_from_grpc(response.into_inner()).map(Some),
                    Err(error) if error.code() == nagrpc::Code::NotFound => Ok(None),
                    Err(error) => Err(saga_source_error(
                        ApplicationPhase::Running,
                        "remote Saga gRPC query is unavailable",
                        error,
                    )),
                }
            }
        }
    }

    /// 业务作用：按租户、workflow 与状态从远程 Orchestrator 读取一页低敏实例摘要。
    ///
    /// 参数说明：`request` 的 token 只能重用于完全相同的过滤条件，页大小范围为 1..=1000。
    ///
    /// 返回：HTTP/gRPC 均返回协议无关页面；参数、认证、token、网络或响应合同异常时返回封闭运行期错误。
    pub async fn query(
        &self,
        request: &SagaRemoteQueryRequest,
    ) -> ApplicationResult<SagaRemoteQueryPage> {
        nasaga_runtime::__private::core::TenantId::new(&request.tenant_id).map_err(|error| {
            saga_source_error(
                ApplicationPhase::Running,
                "remote Saga query tenant is invalid",
                error,
            )
        })?;
        if let Some(workflow) = request.workflow.as_deref() {
            nasaga_runtime::__private::core::WorkflowName::new(workflow).map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Running,
                    "remote Saga query workflow is invalid",
                    error,
                )
            })?;
        }
        if request.page_size == 0 || request.page_size > 1_000 {
            return Err(saga_error(
                ApplicationPhase::Running,
                "remote Saga query page size must be within 1..=1000",
            ));
        }
        for status in &request.statuses {
            if nasaga_runtime::__private::core::SagaStatus::parse(status).is_none() {
                return Err(saga_error(
                    ApplicationPhase::Running,
                    "remote Saga query status is invalid",
                ));
            }
        }
        match &self.transport {
            SagaRemoteClientTransport::Http(http) => {
                let signed_path = format!("{}/query", http.instances_target.signed_path);
                let mut url = http.instances_target.url.clone();
                url.set_path(&signed_path);
                let body = serde_json::to_vec(&serde_json::json!({
                    "tenant_id": request.tenant_id,
                    "workflow": request.workflow,
                    "statuses": request.statuses,
                    "page_token": request.page_token,
                    "page_size": request.page_size,
                    "created_from_ms": request.created_from_ms,
                    "created_to_ms": request.created_to_ms,
                }))
                .map_err(|error| {
                    saga_source_error(
                        ApplicationPhase::Running,
                        "remote Saga HTTP query encoding failed",
                        error,
                    )
                })?;
                let response = http
                    .send_signed(reqwest::Method::POST, url, &signed_path, body)
                    .await?;
                if !response.status.is_success() {
                    return Err(remote_http_status_error("query", response.status));
                }
                serde_json::from_slice(&response.body).map_err(|error| {
                    saga_source_error(
                        ApplicationPhase::Running,
                        "remote Saga HTTP query response is invalid",
                        error,
                    )
                })
            }
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            SagaRemoteClientTransport::Grpc(target) => {
                use nasaga_runtime::orchestrator_proto as proto;
                let statuses = request
                    .statuses
                    .iter()
                    .map(|status| match status.as_str() {
                        "RUNNING" => proto::SagaStatus::Running,
                        "CANCELLING" => proto::SagaStatus::Cancelling,
                        "WAITING_RESOLUTION" => proto::SagaStatus::WaitingResolution,
                        "COMPENSATING" => proto::SagaStatus::Compensating,
                        "COMPLETED" => proto::SagaStatus::Completed,
                        "COMPENSATED" => proto::SagaStatus::Compensated,
                        "MANUAL_INTERVENTION" => proto::SagaStatus::ManualIntervention,
                        "MANUALLY_CLOSED" => proto::SagaStatus::ManuallyClosed,
                        _ => proto::SagaStatus::Unspecified,
                    })
                    .map(|status| status as i32)
                    .collect();
                let mut client = proto::saga_orchestrator_client::SagaOrchestratorClient::new(
                    target.channel.clone(),
                );
                let response = client
                    .query_sagas(proto::QuerySagasRequest {
                        created_from_ms: request.created_from_ms,
                        created_to_ms: request.created_to_ms,
                        tenant_id: request.tenant_id.clone(),
                        workflow: request.workflow.clone().unwrap_or_default(),
                        statuses,
                        page_size: request.page_size,
                        page_token: request.page_token.clone().unwrap_or_default(),
                    })
                    .await
                    .map_err(|error| {
                        saga_source_error(
                            ApplicationPhase::Running,
                            "remote Saga gRPC query is unavailable",
                            error,
                        )
                    })?
                    .into_inner();
                let sagas = response
                    .sagas
                    .into_iter()
                    .map(remote_summary_from_grpc)
                    .collect::<ApplicationResult<Vec<_>>>()?;
                Ok(SagaRemoteQueryPage {
                    sagas,
                    next_page_token: (!response.next_page_token.is_empty())
                        .then_some(response.next_page_token),
                })
            }
        }
    }

    /// 业务作用：在当前同源业务事务内持久化可靠发起意图，使业务事实提交后可持续重投到远程 Orchestrator。
    ///
    /// 参数说明：`request` 必须在重试中保持相同 Saga/trigger 身份，`traceparent` 是可选的受信链路上下文。
    ///
    /// 返回：意图已追加到当前业务事务时返回稳定事件身份；外层事务仍须明确提交，返回值不表示远端已受理或完成。
    /// direct 模式、缺失事务、跨数据源或持久化失败时返回错误；dispatcher 固定扫描 client 指定的数据源。
    pub async fn enqueue_start(
        &self,
        request: &SagaRemoteStartRequest,
        traceparent: Option<&str>,
    ) -> ApplicationResult<String> {
        validate_remote_start_request(request)?;
        let payload = serde_json::to_vec(request).map_err(|error| {
            saga_source_error(
                ApplicationPhase::Running,
                "reliable Saga start intent encoding failed",
                error,
            )
        })?;
        let append = self.start_intent.as_ref().ok_or_else(|| {
            saga_error(
                ApplicationPhase::Running,
                "this Saga client is configured for direct start only",
            )
        })?;
        let event_id = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, &payload).to_string();
        let mut event = naoutbox_core::OutboxEvent {
            event_id: event_id.clone(),
            aggregate_type: "SagaStartIntent".to_owned(),
            aggregate_id: request.saga_id.clone(),
            event_type: "saga.start.requested".to_owned(),
            payload,
            traceparent: None,
            tenant: request.tenant_id.clone(),
        };
        if let Some(traceparent) = traceparent {
            event.traceparent = Some(traceparent.to_owned());
        }
        let context =
            naoutbox_core::OutboxWriteContext::new(&request.tenant_id).map_err(|error| {
                saga_error(
                    ApplicationPhase::Running,
                    format!("reliable Saga start tenant is invalid: {error}"),
                )
            })?;
        append
            .append_transactional_with_context(&context, &event)
            .await
            .map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Running,
                    "reliable Saga start intent could not be persisted",
                    error,
                )
            })?;
        Ok(event_id)
    }
}

#[async_trait::async_trait]
impl OutboxPublisher for SagaStartIntentPublisher {
    /// 业务作用：投递已提交的 start-intent，只有远端明确 Committed 或 Duplicate 才允许 Outbox 前移。
    ///
    /// 参数说明：`event` 必须是可靠 client 写入的稳定 Saga start 请求。
    ///
    /// 返回：持久收据明确时成功；确定性合同拒绝可进入 DLT，网络不明、限流或服务端故障保留原事件重投。
    async fn publish(&self, event: &naoutbox_core::OutboxEvent) -> Result<(), OutboxPublishError> {
        if event.aggregate_type != "SagaStartIntent" || event.event_type != "saga.start.requested" {
            return Err(OutboxPublishError::new(
                "Saga start-intent publisher received an unrelated event",
            ));
        }
        match &self.transport {
            SagaRemoteClientTransport::Http(http) => {
                let response = http
                    .send_signed(
                        reqwest::Method::POST,
                        http.instances_target.url.clone(),
                        &http.instances_target.signed_path,
                        event.payload.clone(),
                    )
                    .await
                    .map_err(|_| {
                        OutboxPublishError::transient("Saga start delivery is uncertain")
                    })?;
                if response.status.is_success() {
                    let receipt: SagaRemoteStartReceipt = serde_json::from_slice(&response.body)
                        .map_err(|_| {
                            OutboxPublishError::transient("Saga start receipt body is unavailable")
                        })?;
                    return match receipt.status {
                        SagaRemoteStartDisposition::Committed
                        | SagaRemoteStartDisposition::Duplicate => Ok(()),
                    };
                }
                if response.status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    || response.status.is_server_error()
                {
                    return Err(OutboxPublishError::transient(
                        "Saga start receiver is temporarily unavailable",
                    ));
                }
                Err(OutboxPublishError::new(
                    "Saga start receiver deterministically rejected the intent",
                ))
            }
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            SagaRemoteClientTransport::Grpc(_) => {
                let request: SagaRemoteStartRequest = serde_json::from_slice(&event.payload)
                    .map_err(|_| OutboxPublishError::new("Saga start-intent payload is invalid"))?;
                validate_remote_start_request(&request).map_err(|_| {
                    OutboxPublishError::new("Saga start-intent domain fields are invalid")
                })?;
                let client = SagaRemoteClient {
                    transport: self.transport.clone(),
                    start_intent: None,
                };
                client.start(&request).await.map(|_| ()).map_err(|error| {
                    if managed_grpc_start_rejection_is_deterministic(&error) {
                        OutboxPublishError::new(
                            "Saga gRPC start receiver deterministically rejected the intent",
                        )
                    } else {
                        OutboxPublishError::transient(
                            "Saga gRPC start receiver is temporarily unavailable",
                        )
                    }
                })
            }
        }
    }
}

/// 业务作用：只把 gRPC 明确返回的调用方或合同拒绝判为可靠 start-intent 的确定性失败。
///
/// 参数说明：`error` 是远程 client 保留底层 gRPC status 的运行期错误。
///
/// 返回：无需等待外部状态变化即可确定同一意图不会成功时返回真；无 status、限额和传输失败返回假。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn managed_grpc_start_rejection_is_deterministic(error: &ApplicationError) -> bool {
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        if let Some(status) = cause.downcast_ref::<nagrpc::Status>() {
            return matches!(
                status.code(),
                nagrpc::Code::InvalidArgument
                    | nagrpc::Code::Unauthenticated
                    | nagrpc::Code::PermissionDenied
                    | nagrpc::Code::NotFound
                    | nagrpc::Code::AlreadyExists
                    | nagrpc::Code::FailedPrecondition
                    | nagrpc::Code::OutOfRange
                    | nagrpc::Code::Unimplemented
            );
        }
        source = cause.source();
    }
    false
}

impl SagaRemoteHttpClient {
    /// 业务作用：为远程 Saga API 的实际规范路径和原始 body 生成一次性签名并执行有界请求。
    ///
    /// 参数说明：`method`、`url`、`signed_path` 与 `body` 必须描述同一线上请求，禁止重定向改变签名目标。
    ///
    /// 返回：收到有界响应时返回状态与原始字节；时钟、网络或响应大小不确定时返回可重试错误。
    async fn send_signed(
        &self,
        method: reqwest::Method,
        url: reqwest::Url,
        signed_path: &str,
        body: Vec<u8>,
    ) -> ApplicationResult<SagaRemoteHttpResponse> {
        let (target, remaining) = self.instances_target.resolve().await?;
        let suffix = signed_path
            .strip_prefix(&self.instances_target.signed_path)
            .ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Running,
                    "Saga API operation path is invalid",
                )
            })?;
        let resolved_path = format!("{}{suffix}", target.signed_path);
        let mut resolved_url = target.url;
        resolved_url.set_path(&resolved_path);
        resolved_url.set_query(url.query());
        let signed_path = resolved_path.as_str();
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Running,
                    "remote Saga request clock is unavailable",
                    error,
                )
            })?
            .as_millis();
        let timestamp_ms = u64::try_from(timestamp_ms).map_err(|error| {
            saga_source_error(
                ApplicationPhase::Running,
                "remote Saga request clock is outside the supported range",
                error,
            )
        })?;
        let nonce = nasaga_runtime::SagaHttpMessageAuthenticator::issue_nonce();
        let signature = self.http_signature(signed_path, timestamp_ms, &nonce, &body);
        let response = self
            .client
            .request(method, resolved_url)
            .timeout(remaining)
            .header("content-type", "application/json")
            .header("x-saga-producer", self.producer.as_str())
            .header("x-saga-timestamp", timestamp_ms.to_string())
            .header("x-saga-nonce", nonce)
            .header("x-saga-signature", signature)
            .body(body)
            .send()
            .await
            .map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Running,
                    "remote Saga HTTP result is uncertain",
                    error,
                )
            })?;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|length| length > self.response_limit_bytes as u64)
        {
            return Err(saga_error(
                ApplicationPhase::Running,
                "remote Saga HTTP response exceeds the configured boundary",
            ));
        }
        let body = response.bytes().await.map_err(|error| {
            saga_source_error(
                ApplicationPhase::Running,
                "remote Saga HTTP response body is unavailable",
                error,
            )
        })?;
        if body.len() > self.response_limit_bytes {
            return Err(saga_error(
                ApplicationPhase::Running,
                "remote Saga HTTP response exceeds the configured boundary",
            ));
        }
        Ok(SagaRemoteHttpResponse {
            status,
            body: body.to_vec(),
        })
    }

    /// 业务作用：为同一远程调用事实生成 HMAC，不让签名参数在调用路径分散组装。
    ///
    /// 参数说明：`path`、`timestamp_ms`、`nonce` 与 `body` 是即将线上发送的原始字段。
    ///
    /// 返回：绑定 client 身份和远端规范路径的十六进制签名。
    fn http_signature(&self, path: &str, timestamp_ms: u64, nonce: &str, body: &[u8]) -> String {
        self.instances_target
            .authenticator
            .sign(&self.producer, path, timestamp_ms, nonce, body)
    }
}

/// 业务作用：在网络调用前复验远程 start 的领域身份，避免把非法路径或无效定义版本交给认证端点。
///
/// 参数说明：`request` 是业务调用方准备以稳定身份重试的发起意图。
///
/// 返回：全部领域字段符合核心类型合同且触发身份非空时成功；否则在 client 本地拒绝。
fn validate_remote_start_request(request: &SagaRemoteStartRequest) -> ApplicationResult<()> {
    if request.input.is_some() && request.payload.is_some() {
        return Err(saga_error(
            ApplicationPhase::Running,
            "Saga start has two payload sources",
        ));
    }
    if let Some(payload) = &request.payload {
        payload
            .validate()
            .map_err(|_| saga_error(ApplicationPhase::Running, "Saga input contract is invalid"))?;
    }
    nasaga_runtime::__private::core::TenantId::new(&request.tenant_id).map_err(|error| {
        saga_source_error(
            ApplicationPhase::Running,
            "remote Saga tenant identity is invalid",
            error,
        )
    })?;
    nasaga_runtime::__private::core::SagaId::new(&request.saga_id).map_err(|error| {
        saga_source_error(
            ApplicationPhase::Running,
            "remote Saga identity is invalid",
            error,
        )
    })?;
    nasaga_runtime::__private::core::WorkflowName::new(&request.workflow).map_err(|error| {
        saga_source_error(
            ApplicationPhase::Running,
            "remote Saga workflow is invalid",
            error,
        )
    })?;
    nasaga_runtime::__private::core::DefinitionVersion::new(request.definition_version).map_err(
        |error| {
            saga_source_error(
                ApplicationPhase::Running,
                "remote Saga definition version is invalid",
                error,
            )
        },
    )?;
    nasaga_runtime::__private::core::BusinessKey::new(&request.business_key).map_err(|error| {
        saga_source_error(
            ApplicationPhase::Running,
            "remote Saga business key is invalid",
            error,
        )
    })?;
    if !managed_start_trigger_is_valid(&request.trigger_id) {
        return Err(saga_error(
            ApplicationPhase::Running,
            "remote Saga trigger identity is invalid",
        ));
    }
    Ok(())
}

/// 业务作用：统一约束 HTTP、gRPC 与远程 client 的业务启动触发身份。
///
/// 参数说明：`value` 是调用方在网络重试期间保持稳定的事件身份。
///
/// 返回：无首尾空白且长度位于持久化合同范围内时返回真。
fn managed_start_trigger_is_valid(value: &str) -> bool {
    !value.is_empty() && value.len() <= 191 && value.trim() == value
}

/// 业务作用：把远端非成功 HTTP 状态收敛为不包含响应体或凭据的稳定 Application 错误。
///
/// 参数说明：`operation` 是固定 API 名，`status` 是远端返回的标准状态码。
///
/// 返回：归属于 Saga Running 阶段且可安全记录的脱敏错误。
fn remote_http_status_error(
    operation: &'static str,
    status: reqwest::StatusCode,
) -> ApplicationError {
    saga_error(
        ApplicationPhase::Running,
        format!("remote Saga {operation} was rejected with HTTP status {status}"),
    )
}

/// 业务作用：统一 tenant、workflow、definition version 与 step 组成的受管路由键。
type ManagedStepRouteKey = (String, String, u32, String);

/// 业务作用：保存一个 generation 内每个步骤的 HTTP 多实例 command 目标。
#[cfg(feature = "web")]
type ManagedHttpCommandRoutes = BTreeMap<ManagedStepRouteKey, Vec<ManagedHttpTarget>>;

/// 业务作用：保存一个 generation 内每个步骤的 Redis command 发布端。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
type ManagedRedisCommandRoutes =
    BTreeMap<ManagedStepRouteKey, Arc<nasaga_runtime::SagaRedisStreamPublisher>>;

/// 业务作用：把 static definition 中的 owner 投影与受信 HTTP route、生产者身份和同代凭据冻结为 Outbox 发布端。
#[cfg(feature = "web")]
struct ManagedHttpPublisher {
    client: reqwest::Client,
    request_timeout: Duration,
    producer: nasaga_runtime::ServiceIdentity,
    routing: Arc<std::sync::RwLock<ManagedHttpRoutingSnapshot>>,
    result_target: Option<ManagedHttpTarget>,
}

/// 业务作用：把同一 Catalog generation 的 command owner 投影与逐实例 HTTP route 冻结为原子读取快照。
#[cfg(feature = "web")]
#[derive(Default)]
struct ManagedHttpRoutingSnapshot {
    command_targets: ManagedHttpCommandRoutes,
}

/// 业务作用：冻结 Kafka command 的流程步骤 topic 与参与方 result topic，避免 payload 自报路由。
#[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
#[derive(Default)]
struct ManagedKafkaRoutingSnapshot {
    command_topics: BTreeMap<ManagedStepRouteKey, String>,
}

/// 业务作用：使用受管 Kafka producer lane 发布 Saga Outbox，并以 broker ACK 作为唯一前移收据。
#[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
struct ManagedKafkaPublisher {
    lane: nafka::ProducerLane,
    routing: Arc<std::sync::RwLock<ManagedKafkaRoutingSnapshot>>,
    result_topic: Option<String>,
}

/// 业务作用：把 Kafka command topic 的稳定身份投影交给当前受管步骤分发与本地事务。
#[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
struct ManagedKafkaCommandConsumer {
    routes: BTreeMap<String, (String, String, u32, String)>,
    group: String,
    producer: nasaga_runtime::ServiceIdentity,
    state: Arc<SagaRuntimeState>,
    delivery_policy: nasaga_runtime::CommandDeliveryPolicy,
}

/// 业务作用：从 owner 独占 result topic 认证参与方，并把结果交给唯一 Orchestrator 推进权威。
#[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
struct ManagedKafkaResultConsumer {
    topic_pattern: String,
    topic_prefix: String,
    group: String,
    orchestrator: SagaOrchestratorApi,
    state: Arc<SagaRuntimeState>,
    delivery_policy: nasaga_runtime::ResultDeliveryPolicy,
}

/// 业务作用：冻结同一 Catalog generation 中每个步骤的 Redis stream 发布端。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
#[derive(Default)]
struct ManagedRedisRoutingSnapshot {
    command_publishers: ManagedRedisCommandRoutes,
}

/// 业务作用：按 command/result envelope 选择冻结 Redis stream，只在 `XADD` 明确确认后完成发布。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
struct ManagedRedisPublisher {
    routing: Arc<std::sync::RwLock<ManagedRedisRoutingSnapshot>>,
    result_publisher: Option<Arc<nasaga_runtime::SagaRedisStreamPublisher>>,
}

/// 业务作用：描述一个逻辑生产者用于 Redis stream 出站签名的密钥。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedRedisSigningKeyMaterial {
    key_id: String,
    key_hex: String,
}

/// 业务作用：描述 Redis stream 入站 key id 绑定的认证服务与验签密钥。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedRedisVerificationKeyMaterial {
    service_identity: String,
    key_hex: String,
}

/// 业务作用：从单个 secret reference 冻结 Redis 数据面出站签名与入站身份验证表。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ManagedRedisCredentialMaterial {
    signing_keys: BTreeMap<String, ManagedRedisSigningKeyMaterial>,
    verification_keys: BTreeMap<String, ManagedRedisVerificationKeyMaterial>,
}

/// 业务作用：把受管 Redis command 交给精确命中的参与方事务适配器。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
struct ManagedRedisCommandHandler {
    state: Arc<SagaRuntimeState>,
}

/// 业务作用：把受管 Redis result 交给当前唯一 Orchestrator 推进事务。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
struct ManagedRedisResultHandler {
    state: Arc<SagaRuntimeState>,
}

/// 业务作用：冻结同一 Catalog generation 中每个流程步骤的直接 gRPC command 目标。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
#[derive(Default)]
struct ManagedGrpcRoutingSnapshot {
    command_targets: BTreeMap<ManagedStepRouteKey, ManagedGrpcTarget>,
}

/// 业务作用：通过 generated unary client 投递 Saga Outbox，并按四值收据决定持久行是否前移。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
struct ManagedGrpcPublisher {
    routing: Arc<std::sync::RwLock<ManagedGrpcRoutingSnapshot>>,
    result_target: Option<ManagedGrpcTarget>,
}

/// 业务作用：冻结 definition 激活与实际 command/result publisher 共用的完整运行合同。
#[derive(Clone)]
struct ManagedDefinitionActivationContract {
    transport: SagaTransportKind,
    redis_key_tag: Option<String>,
    address_policy: Option<SagaAddressPolicySettings>,
    publisher_contract_digest: String,
    result_backend_digest: Option<String>,
    trusted_result_contracts: BTreeMap<String, BTreeSet<String>>,
    security_generation: u64,
    security: Option<Arc<ManagedDefinitionActivationSecurity>>,
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    grpc_credential: Option<Arc<ManagedGrpcCredentialMaterial>>,
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    grpc_timeout: Option<Duration>,
}

/// 业务作用：保留 publisher 的固定配置与凭据引用，使 Catalog 校验跟随统一安全快照而不改变定义摘要。
struct ManagedDefinitionActivationSecurity {
    state: Arc<security::SagaSecurityState>,
    publisher_seed: Sha256,
    publisher_refs: BTreeSet<String>,
    http_result_refs: BTreeMap<String, String>,
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    grpc_peer_bindings: BTreeMap<String, String>,
}

impl catalog_authority::CatalogSecurityEpoch for ManagedDefinitionActivationContract {
    /// 业务作用：把当前 publisher 和结果验签集合绑定为 Catalog 操作可复验的安全身份。
    /// 参数说明：无。
    /// 返回：同一原子快照的发布代际与完整合同摘要；材料不能准备时拒绝产生权威身份。
    fn epoch(&self) -> Option<catalog_authority::CatalogSecurityStamp> {
        let current = self.current().ok()?;
        let mut digest = Sha256::new();
        digest.update(current.publisher_contract_digest.as_bytes());
        update_managed_canonical_json(
            &mut digest,
            &serde_json::to_value(&current.trusted_result_contracts).ok()?,
        );
        Some(catalog_authority::CatalogSecurityStamp {
            generation: current.security_generation,
            contract_digest: digest
                .finalize()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        })
    }
}

impl ManagedDefinitionActivationContract {
    /// 业务作用：为一次 Catalog 操作固定实际 publisher 材料与仍受信的 result 凭据集合。
    /// 参数说明：无。
    /// 返回：同一安全快照形成的完整合同；引用缺失或旧证书失效时拒绝构造权威证据。
    fn current(&self) -> ApplicationResult<Self> {
        let Some(source) = &self.security else {
            return Ok(self.clone());
        };
        let snapshot = source.state.current();
        let mut current = self.clone();
        current.security = None;
        // 发布代际与材料取自同一个 ConfigView 中的安全快照；材料往返变化也不能恢复旧操作资格。
        current.security_generation = snapshot.secrets.generation();
        let mut digest = source.publisher_seed.clone();
        for reference in &source.publisher_refs {
            let material = snapshot
                .secrets
                .get(reference)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    saga_error(
                        ApplicationPhase::Running,
                        "managed Saga publisher secret is unavailable",
                    )
                })?;
            digest.update((reference.len() as u64).to_be_bytes());
            digest.update(reference.as_bytes());
            digest.update((material.expose().len() as u64).to_be_bytes());
            digest.update(material.expose());
        }
        current.publisher_contract_digest = digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        if self.transport == SagaTransportKind::Http {
            current.trusted_result_contracts = source
                .http_result_refs
                .iter()
                .map(|(owner, reference)| {
                    let credentials = snapshot.http.get(reference).ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Running,
                            "managed Saga result credential is unavailable",
                        )
                    })?;
                    Ok((owner.clone(), credentials.accepted_result_contracts()))
                })
                .collect::<ApplicationResult<_>>()?;
        }
        #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
        if self.transport == SagaTransportKind::Grpc {
            current.trusted_result_contracts = source
                .grpc_peer_bindings
                .iter()
                .map(|(owner, binding)| {
                    let contracts = snapshot
                        .peer_principals(binding)
                        .iter()
                        .map(|principal| {
                            managed_result_contract_digest(
                                b"napp-saga-grpc-result",
                                [principal.as_bytes()],
                            )
                        })
                        .collect();
                    (owner.clone(), contracts)
                })
                .collect();
        }
        Ok(current)
    }
}

/// 业务作用：让公开 gRPC API 复用同一 SagaRuntimeState、授权快照和不透明分页签名权威。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
struct ManagedGrpcApiService {
    state: Arc<SagaRuntimeState>,
    callers: security::GrpcPeerBindings<SagaApiActor>,
    definition_signing_keys: BTreeMap<String, String>,
    orchestrator_service_identity: nasaga_runtime::ServiceIdentity,
    activation: ManagedDefinitionActivationContract,
    page_token_key: [u8; 32],
}

/// 业务作用：让 managed gRPC command service 在计划发布后按冻结步骤身份调用唯一参与方事务。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
struct ManagedGrpcCommandHandler {
    state: Arc<SagaRuntimeState>,
}

/// 业务作用：让 managed gRPC result service 在计划发布后复用唯一 Orchestrator 推进事务。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
struct ManagedGrpcResultHandler {
    state: Arc<SagaRuntimeState>,
}

/// 业务作用：为受管 command 入站绑定多个经 mTLS 验证的 principal 与逻辑 Orchestrator 身份。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
struct ManagedGrpcCommandService {
    state: Arc<SagaRuntimeState>,
    peers: security::GrpcPeerBindings<nasaga_runtime::ServiceIdentity>,
}

/// 业务作用：为受管 result 入站绑定多个经 mTLS 验证的 principal 与参与方 owner 身份。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
struct ManagedGrpcResultService {
    state: Arc<SagaRuntimeState>,
    peers: security::GrpcPeerBindings<nasaga_runtime::ServiceIdentity>,
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
impl nasaga_runtime::SagaCommandHandler for ManagedGrpcCommandHandler {
    /// 业务作用：把已认证 gRPC command 交给 envelope 精确命中的本地步骤事务。
    ///
    /// 参数说明：`envelope` 是命令事实，`producer` 来自 mTLS 映射。
    ///
    /// 返回：本地提交或重复时返回可确认结论；未 Ready、未知步骤或事务失败返回错误。
    async fn handle_authenticated_command(
        &self,
        envelope: &nasaga_runtime::SagaCommandEnvelope,
        producer: &nasaga_runtime::ServiceIdentity,
    ) -> anyhow::Result<nasaga_runtime::ParticipantHandled> {
        self.handle_authenticated_command_traced(envelope, producer, None)
            .await
    }

    /// 业务作用：在同一 gRPC 收据 trace 下执行精确步骤事务，不让协议层接触业务 handler。
    ///
    /// 参数说明：命令与 producer 已认证，`receipt_trace` 是可选 W3C 上下文。
    ///
    /// 返回：事务明确提交后返回；其它结果保持 Outbox 可重投。
    async fn handle_authenticated_command_traced(
        &self,
        envelope: &nasaga_runtime::SagaCommandEnvelope,
        producer: &nasaga_runtime::ServiceIdentity,
        receipt_trace: Option<&nasaga_runtime::TraceContext>,
    ) -> anyhow::Result<nasaga_runtime::ParticipantHandled> {
        self.state.ensure_ready().map_err(anyhow::Error::new)?;
        self.state
            .command_handler(envelope)
            .ok_or_else(|| anyhow::anyhow!("managed Saga gRPC command route is unavailable"))?
            .handle(envelope, producer, receipt_trace)
            .await
    }
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
impl nasaga_runtime::SagaResultHandler for ManagedGrpcResultHandler {
    /// 业务作用：把已认证 gRPC result 交给当前 Orchestrator 的唯一推进事务。
    ///
    /// 参数说明：结果、producer 与当前时刻共同形成推进输入。
    ///
    /// 返回：推进或重复已提交时返回；未 Ready、角色不符或事务失败返回错误。
    async fn handle_authenticated_result(
        &self,
        envelope: &nasaga_runtime::SagaResultEnvelope,
        producer: &nasaga_runtime::ServiceIdentity,
        now_ms: i64,
    ) -> anyhow::Result<nasaga_runtime::HandleOutcome> {
        self.handle_authenticated_result_traced(envelope, producer, None, now_ms)
            .await
    }

    /// 业务作用：携带受信 trace 推进 result，保持 HTTP 与 gRPC 共用同一状态机事务。
    ///
    /// 参数说明：结果、认证 producer、可选 trace 与当前时刻均来自受管协议边界。
    ///
    /// 返回：本地提交明确时返回；其它结论不允许发布端前移。
    async fn handle_authenticated_result_traced(
        &self,
        envelope: &nasaga_runtime::SagaResultEnvelope,
        producer: &nasaga_runtime::ServiceIdentity,
        receipt_trace: Option<&nasaga_runtime::TraceContext>,
        now_ms: i64,
    ) -> anyhow::Result<nasaga_runtime::HandleOutcome> {
        let permit = self.state.result_permit()?;
        self.state
            .managed_orchestrator()
            .ok_or_else(|| anyhow::anyhow!("managed Saga gRPC result role is unavailable"))?
            .handle_result(
                envelope,
                producer,
                receipt_trace,
                now_ms,
                &self.state,
                &permit,
            )
            .await
    }
}

#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
impl nasaga_runtime::SagaCommandHandler for ManagedRedisCommandHandler {
    /// 业务作用：把已验证 Redis command 交给 envelope 精确命中的本地步骤事务。
    ///
    /// 参数说明：`envelope` 是命令事实，`producer` 来自 key id 或独占 stream ACL 绑定。
    ///
    /// 返回：本地提交或重复时返回可确认结论；未 Ready、未知步骤或事务失败返回错误。
    async fn handle_authenticated_command(
        &self,
        envelope: &nasaga_runtime::SagaCommandEnvelope,
        producer: &nasaga_runtime::ServiceIdentity,
    ) -> anyhow::Result<nasaga_runtime::ParticipantHandled> {
        self.handle_authenticated_command_traced(envelope, producer, None)
            .await
    }

    /// 业务作用：在同一 Redis 收据 trace 下执行步骤事务，保持 Inbox、gate、业务事实与 result Outbox 原子。
    ///
    /// 参数说明：命令与 `producer` 已通过 transport 认证，`receipt_trace` 是可选 W3C 上下文。
    ///
    /// 返回：事务明确提交后返回；其它结论保留 PEL 等待重投。
    async fn handle_authenticated_command_traced(
        &self,
        envelope: &nasaga_runtime::SagaCommandEnvelope,
        producer: &nasaga_runtime::ServiceIdentity,
        receipt_trace: Option<&nasaga_runtime::TraceContext>,
    ) -> anyhow::Result<nasaga_runtime::ParticipantHandled> {
        self.state.ensure_ready().map_err(anyhow::Error::new)?;
        self.state
            .command_handler(envelope)
            .ok_or_else(|| anyhow::anyhow!("managed Saga Redis command route is unavailable"))?
            .handle(envelope, producer, receipt_trace)
            .await
    }
}

#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
impl nasaga_runtime::SagaResultHandler for ManagedRedisResultHandler {
    /// 业务作用：把已验证 Redis result 交给当前 Orchestrator 的唯一推进事务。
    ///
    /// 参数说明：结果、认证 `producer` 与当前时刻共同形成推进输入。
    ///
    /// 返回：推进或重复吸收已提交时返回；其它结论不允许 XACK。
    async fn handle_authenticated_result(
        &self,
        envelope: &nasaga_runtime::SagaResultEnvelope,
        producer: &nasaga_runtime::ServiceIdentity,
        now_ms: i64,
    ) -> anyhow::Result<nasaga_runtime::HandleOutcome> {
        self.handle_authenticated_result_traced(envelope, producer, None, now_ms)
            .await
    }

    /// 业务作用：携带受信 Redis 收据 trace 推进 result，不在 transport 复制状态机。
    ///
    /// 参数说明：结果、认证生产者、可选 trace 与当前时刻来自受管边界。
    ///
    /// 返回：本地提交明确时返回；未 Ready、角色不符或数据库异常返回错误。
    async fn handle_authenticated_result_traced(
        &self,
        envelope: &nasaga_runtime::SagaResultEnvelope,
        producer: &nasaga_runtime::ServiceIdentity,
        receipt_trace: Option<&nasaga_runtime::TraceContext>,
        now_ms: i64,
    ) -> anyhow::Result<nasaga_runtime::HandleOutcome> {
        let permit = self.state.result_permit()?;
        self.state
            .managed_orchestrator()
            .ok_or_else(|| anyhow::anyhow!("managed Saga Redis result role is unavailable"))?
            .handle_result(
                envelope,
                producer,
                receipt_trace,
                now_ms,
                &self.state,
                &permit,
            )
            .await
    }
}

/// 业务作用：从受管 gRPC request 的 TLS extension 解析可信逻辑服务身份。
///
/// 参数说明：`request` 含 listener 写入的 PeerIdentity，`peers` 是 Ready 前冻结的指纹映射。
///
/// 返回：指纹命中时返回逻辑身份；缺失或未知对端返回 Unauthenticated。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn authenticate_managed_grpc_peer<T>(
    request: &nagrpc::Request<T>,
    peers: &security::GrpcPeerBindings<nasaga_runtime::ServiceIdentity>,
) -> Result<nasaga_runtime::ServiceIdentity, nagrpc::Status> {
    request
        .extensions()
        .get::<nagrpc::PeerIdentity>()
        .and_then(|peer| peers.get(peer.principal()))
        .ok_or_else(|| nagrpc::Status::unauthenticated("verified Saga gRPC peer is required"))
}

/// 业务作用：从唯一合法的 gRPC traceparent metadata 生成受信收据上下文。
///
/// 参数说明：`request` 是 listener 已完成大小与 TLS 门禁的 unary 请求。
///
/// 返回：恰有一个合法值时返回上下文；其它情况不续接调用方 trace。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn managed_grpc_trace<T>(request: &nagrpc::Request<T>) -> Option<nasaga_runtime::TraceContext> {
    let mut values = request.metadata().get_all("traceparent").iter();
    match (values.next(), values.next()) {
        (Some(value), None) => value
            .to_str()
            .ok()
            .and_then(nasaga_runtime::TraceContext::parse_traceparent),
        _ => None,
    }
}

/// 业务作用：把运行核心的封闭 gRPC 收据投影为公开 protobuf 收据。
///
/// 参数说明：`receipt` 已包含全部合法投递结论。
///
/// 返回：调用端可按 enum 裁决 Outbox，不解析 status 文本。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn managed_grpc_receipt(
    receipt: nasaga_runtime::SagaGrpcReceipt,
) -> nasaga_runtime::grpc_proto::SagaDeliveryReceipt {
    let (kind, reason) = match receipt {
        nasaga_runtime::SagaGrpcReceipt::Committed => (
            nasaga_runtime::grpc_proto::SagaReceiptKind::Committed,
            String::new(),
        ),
        nasaga_runtime::SagaGrpcReceipt::Duplicate => (
            nasaga_runtime::grpc_proto::SagaReceiptKind::Duplicate,
            String::new(),
        ),
        nasaga_runtime::SagaGrpcReceipt::DeterministicReject { reason } => (
            nasaga_runtime::grpc_proto::SagaReceiptKind::DeterministicReject,
            reason.to_owned(),
        ),
        nasaga_runtime::SagaGrpcReceipt::Retryable => (
            nasaga_runtime::grpc_proto::SagaReceiptKind::Retryable,
            String::new(),
        ),
    };
    nasaga_runtime::grpc_proto::SagaDeliveryReceipt {
        kind: kind as i32,
        reason,
    }
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
#[nagrpc::async_trait]
impl nasaga_runtime::grpc_proto::saga_command_transport_server::SagaCommandTransport
    for ManagedGrpcCommandService
{
    /// 业务作用：认证 Orchestrator 证书并仅在本地事务明确提交后返回成功收据。
    ///
    /// 参数说明：`request` 携带不可改写的 envelope JSON 与 TLS peer extension。
    ///
    /// 返回：身份错误返回 Unauthenticated；其余业务结论返回封闭收据。
    async fn deliver(
        &self,
        request: nagrpc::Request<nasaga_runtime::grpc_proto::SagaDeliveryRequest>,
    ) -> Result<nagrpc::Response<nasaga_runtime::grpc_proto::SagaDeliveryReceipt>, nagrpc::Status>
    {
        let producer = authenticate_managed_grpc_peer(&request, &self.peers)?;
        let trace = managed_grpc_trace(&request);
        let server = nasaga_runtime::SagaGrpcCommandServer::new(
            Arc::new(ManagedGrpcCommandHandler {
                state: Arc::clone(&self.state),
            }),
            producer.clone(),
        );
        let receipt = server
            .adjudicate(
                &nasaga_runtime::SagaGrpcPeerIdentity::MtlsPrincipal(producer),
                &request.get_ref().envelope_json,
                trace.as_ref(),
            )
            .await;
        Ok(nagrpc::Response::new(managed_grpc_receipt(receipt)))
    }
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
#[nagrpc::async_trait]
impl nasaga_runtime::grpc_proto::saga_result_transport_server::SagaResultTransport
    for ManagedGrpcResultService
{
    /// 业务作用：认证参与方证书并仅在 Orchestrator 结果事务明确提交后返回成功收据。
    ///
    /// 参数说明：`request` 携带 result JSON 与 TLS peer extension。
    ///
    /// 返回：身份错误返回 Unauthenticated；时钟不可用返回 Internal；其它结论返回封闭收据。
    async fn deliver(
        &self,
        request: nagrpc::Request<nasaga_runtime::grpc_proto::SagaDeliveryRequest>,
    ) -> Result<nagrpc::Response<nasaga_runtime::grpc_proto::SagaDeliveryReceipt>, nagrpc::Status>
    {
        let producer = authenticate_managed_grpc_peer(&request, &self.peers)?;
        let trace = managed_grpc_trace(&request);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
            .ok_or_else(|| nagrpc::Status::internal("Saga gRPC clock is unavailable"))?;
        let server = nasaga_runtime::SagaGrpcResultServer::new(
            Arc::new(ManagedGrpcResultHandler {
                state: Arc::clone(&self.state),
            }),
            producer.clone(),
        );
        let receipt = server
            .adjudicate(
                &nasaga_runtime::SagaGrpcPeerIdentity::MtlsPrincipal(producer),
                &request.get_ref().envelope_json,
                trace.as_ref(),
                now_ms,
            )
            .await;
        Ok(nagrpc::Response::new(managed_grpc_receipt(receipt)))
    }
}

/// 业务作用：认证公开 gRPC API 调用者，并以 mTLS principal 取得冻结权限快照。
///
/// 参数说明：`request` 的 PeerIdentity 来自 listener TLS 层，`callers` 是 Start 时冻结的 RBAC 映射。
///
/// 返回：命中返回调用主体；缺失或未知 principal 返回 Unauthenticated。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn managed_grpc_api_caller<T>(
    request: &nagrpc::Request<T>,
    callers: &security::GrpcPeerBindings<SagaApiActor>,
) -> Result<SagaApiActor, nagrpc::Status> {
    request
        .extensions()
        .get::<nagrpc::PeerIdentity>()
        .and_then(|peer| callers.get(peer.principal()))
        .ok_or_else(|| nagrpc::Status::unauthenticated("verified Saga API peer is required"))
}

/// 业务作用：在 Catalog 读取或写入前把 registry 权限同时约束到租户和 workflow。
///
/// 参数说明：`caller` 是认证快照，`tenant` 与 `workflow` 是 DefinitionKey 的完整授权目标。
///
/// 返回：通用 registry 权限及 workflow grant 均命中时成功；其它情况返回 PermissionDenied。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn authorize_managed_grpc_registry(
    caller: &SagaApiActor,
    tenant: &str,
    workflow: &str,
) -> Result<(), nagrpc::Status> {
    SagaOrchestratorApi::authorize_registry(caller, tenant, workflow).map_err(|_| {
        nagrpc::Status::permission_denied("Saga workflow registry permission is required")
    })
}

/// 业务作用：把 Unix 毫秒转换为 protobuf Timestamp，保持负时刻也使用规范 nanos。
///
/// 参数说明：`millis` 是数据库或领域时钟的 Unix 毫秒。
///
/// 返回：秒和纳秒规范化后的 Timestamp。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn managed_grpc_timestamp(millis: i64) -> nagrpc::codegen::prost_types::Timestamp {
    let seconds = millis.div_euclid(1_000);
    let nanos = i32::try_from(millis.rem_euclid(1_000) * 1_000_000).unwrap_or_default();
    nagrpc::codegen::prost_types::Timestamp { seconds, nanos }
}

/// 业务作用：编码标准 `google.rpc.ErrorInfo`，让跨语言客户端按稳定 reason 分类确定性拒绝。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
#[derive(Clone, PartialEq, nagrpc::codegen::prost::Message)]
struct ManagedGoogleRpcErrorInfo {
    #[prost(string, tag = "1")]
    reason: String,
    #[prost(string, tag = "2")]
    domain: String,
    #[prost(map = "string, string", tag = "3")]
    metadata: std::collections::HashMap<String, String>,
}

/// 业务作用：编码承载标准 detail 的 `google.rpc.Status` 外层消息。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
#[derive(Clone, PartialEq, nagrpc::codegen::prost::Message)]
struct ManagedGoogleRpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<nagrpc::codegen::prost_types::Any>,
}

/// 业务作用：构造同时具有标准 gRPC code 和稳定 `ErrorInfo.reason` 的公开错误。
///
/// 参数说明：`code` 是协议状态，`message` 是可读摘要，`reason` 是客户端稳定分支键。
///
/// 返回：details 中只包含低基数原因，不携带租户、实例或业务内容。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn managed_grpc_error_info_status(
    code: nagrpc::Code,
    message: &'static str,
    reason: &'static str,
) -> nagrpc::Status {
    use nagrpc::codegen::prost::Message as _;
    let error_info = ManagedGoogleRpcErrorInfo {
        reason: reason.to_owned(),
        domain: "nasa.runtime.saga".to_owned(),
        metadata: std::collections::HashMap::new(),
    };
    let status = ManagedGoogleRpcStatus {
        code: code as i32,
        message: message.to_owned(),
        details: vec![nagrpc::codegen::prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.ErrorInfo".to_owned(),
            value: error_info.encode_to_vec(),
        }],
    };
    nagrpc::Status::with_details(code, message, status.encode_to_vec().into())
}

/// 业务作用：把 Registry 类型化失败映射为一致的 gRPC status，并保留确定性冲突的稳定 ErrorInfo。
///
/// 参数说明：`error` 是 Catalog 错误链，`operation` 是不含业务标识的公开动作摘要。
///
/// 返回：参数、权限、不存在、前置条件和冲突使用确定性 code；存储失败返回 Unavailable。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn managed_grpc_catalog_status(error: &anyhow::Error, operation: &'static str) -> nagrpc::Status {
    match classify_managed_catalog_api_failure(error) {
        ManagedCatalogApiFailure::Invalid => {
            nagrpc::Status::invalid_argument("Saga Registry request is invalid")
        }
        ManagedCatalogApiFailure::PermissionDenied => {
            nagrpc::Status::permission_denied("Saga Registry permission is required")
        }
        ManagedCatalogApiFailure::NotFound => {
            nagrpc::Status::not_found("Saga definition was not found")
        }
        ManagedCatalogApiFailure::FailedPrecondition => {
            nagrpc::Status::failed_precondition("Saga Registry precondition is not satisfied")
        }
        ManagedCatalogApiFailure::DigestConflict => managed_grpc_error_info_status(
            nagrpc::Code::AlreadyExists,
            "Saga definition key already has another digest",
            "DEFINITION_DIGEST_CONFLICT",
        ),
        ManagedCatalogApiFailure::DigestPrecondition => managed_grpc_error_info_status(
            nagrpc::Code::Aborted,
            "Saga definition digest no longer matches",
            "DEFINITION_DIGEST_PRECONDITION_FAILED",
        ),
        ManagedCatalogApiFailure::OperationConflict => managed_grpc_error_info_status(
            nagrpc::Code::AlreadyExists,
            "Saga definition operation identity has different parameters",
            "DEFINITION_OPERATION_CONFLICT",
        ),
        ManagedCatalogApiFailure::Unavailable => {
            nagrpc::Status::unavailable(format!("Saga Registry {operation} is unavailable"))
        }
    }
}

/// 业务作用：把 protobuf Timestamp 复验并转换为领域使用的 Unix 毫秒。
///
/// 参数说明：`timestamp` 是调用方提供的 deadline。
///
/// 返回：规范且可表示时返回毫秒；非法 nanos 或溢出返回 InvalidArgument。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn managed_grpc_timestamp_ms(
    timestamp: &nagrpc::codegen::prost_types::Timestamp,
) -> Result<i64, nagrpc::Status> {
    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        return Err(nagrpc::Status::invalid_argument(
            "Saga timestamp nanos is invalid",
        ));
    }
    timestamp
        .seconds
        .checked_mul(1_000)
        .and_then(|value| value.checked_add(i64::from(timestamp.nanos / 1_000_000)))
        .ok_or_else(|| nagrpc::Status::invalid_argument("Saga timestamp is out of range"))
}

/// 业务作用：把 generated gRPC 快照收敛为协议无关的远程 client 读取模型。
///
/// 参数说明：`snapshot` 来自经 mTLS 验证的 SagaOrchestrator 服务。
///
/// 返回：key 与 enum 均合法时返回快照；缺失 key 或零值状态返回运行期协议错误。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn remote_snapshot_from_grpc(
    snapshot: nasaga_runtime::orchestrator_proto::SagaSnapshot,
) -> ApplicationResult<SagaRemoteSnapshot> {
    use nasaga_runtime::orchestrator_proto as proto;
    let key = snapshot.key.ok_or_else(|| {
        saga_error(
            ApplicationPhase::Running,
            "remote Saga gRPC snapshot key is missing",
        )
    })?;
    let status = proto::SagaStatus::try_from(snapshot.status)
        .ok()
        .filter(|value| *value != proto::SagaStatus::Unspecified)
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Running,
                "remote Saga gRPC snapshot status is invalid",
            )
        })?
        .as_str_name()
        .trim_start_matches("SAGA_STATUS_")
        .to_owned();
    let control_state = proto::SagaControlState::try_from(snapshot.control_state)
        .ok()
        .filter(|value| *value != proto::SagaControlState::Unspecified)
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Running,
                "remote Saga gRPC control state is invalid",
            )
        })?
        .as_str_name()
        .trim_start_matches("SAGA_CONTROL_STATE_")
        .to_owned();
    Ok(SagaRemoteSnapshot {
        tenant_id: key.tenant_id,
        saga_id: key.saga_id,
        workflow: snapshot.workflow,
        definition_version: snapshot.definition_version,
        definition_digest: snapshot.definition_digest,
        business_key: snapshot.business_key,
        status,
        control_state,
        direction: snapshot.direction,
        current_step: snapshot.current_step,
        state_version: snapshot.state_version,
        control_version: snapshot.control_version,
        deadline_at_ms: snapshot.deadline_at_ms,
        failure_code: (!snapshot.failure_code.is_empty()).then_some(snapshot.failure_code),
        traceparent: (!snapshot.traceparent.is_empty()).then_some(snapshot.traceparent),
    })
}

/// 业务作用：把 gRPC query 的低敏快照投影为远程 client 统一摘要并强制时间字段存在。
///
/// 参数说明：`snapshot` 必须来自受管 QuerySagas 响应。
///
/// 返回：身份、enum 与数据库时间完整时返回摘要；缺失或零值字段作为协议错误拒绝。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn remote_summary_from_grpc(
    snapshot: nasaga_runtime::orchestrator_proto::SagaSnapshot,
) -> ApplicationResult<SagaRemoteSummary> {
    let created_at_ms = snapshot
        .created_at
        .as_ref()
        .map(managed_grpc_timestamp_ms)
        .transpose()
        .map_err(|error| {
            saga_error(
                ApplicationPhase::Running,
                format!("remote Saga gRPC created timestamp is invalid: {error}"),
            )
        })?
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Running,
                "remote Saga gRPC created timestamp is missing",
            )
        })?;
    let updated_at_ms = snapshot
        .updated_at
        .as_ref()
        .map(managed_grpc_timestamp_ms)
        .transpose()
        .map_err(|error| {
            saga_error(
                ApplicationPhase::Running,
                format!("remote Saga gRPC updated timestamp is invalid: {error}"),
            )
        })?
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Running,
                "remote Saga gRPC updated timestamp is missing",
            )
        })?;
    let remote = remote_snapshot_from_grpc(snapshot)?;
    Ok(SagaRemoteSummary {
        tenant_id: remote.tenant_id,
        saga_id: remote.saga_id,
        workflow: remote.workflow,
        definition_version: remote.definition_version,
        business_key: remote.business_key,
        status: remote.status,
        control_state: remote.control_state,
        direction: remote.direction,
        current_step: remote.current_step,
        state_version: remote.state_version,
        failure_code: remote.failure_code,
        created_at_ms,
        updated_at_ms,
    })
}

/// 业务作用：把数据库实例事实投影为不携带 payload 的公开 protobuf 快照。
///
/// 参数说明：`row` 是已提交实例；该读取模型当前不复制数据库时间列。
///
/// 返回：身份、冻结 definition、业务/控制状态和 CAS 版本的稳定消息。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn managed_grpc_instance_snapshot(
    row: &nasaga_runtime::SagaInstanceRow,
) -> nasaga_runtime::orchestrator_proto::SagaSnapshot {
    use nasaga_runtime::orchestrator_proto as proto;
    let status = match row.status.as_str() {
        "RUNNING" => proto::SagaStatus::Running,
        "CANCELLING" => proto::SagaStatus::Cancelling,
        "WAITING_RESOLUTION" => proto::SagaStatus::WaitingResolution,
        "COMPENSATING" => proto::SagaStatus::Compensating,
        "COMPLETED" => proto::SagaStatus::Completed,
        "COMPENSATED" => proto::SagaStatus::Compensated,
        "MANUAL_INTERVENTION" => proto::SagaStatus::ManualIntervention,
        "MANUALLY_CLOSED" => proto::SagaStatus::ManuallyClosed,
        _ => proto::SagaStatus::Unspecified,
    };
    let control_state = match row.control_state.as_str() {
        "ACTIVE" => proto::SagaControlState::Active,
        "PAUSED" => proto::SagaControlState::Paused,
        _ => proto::SagaControlState::Unspecified,
    };
    proto::SagaSnapshot {
        key: Some(proto::SagaKey {
            tenant_id: row.tenant.as_str().to_owned(),
            saga_id: row.saga_id.as_str().to_owned(),
        }),
        workflow: row.workflow.as_str().to_owned(),
        definition_version: row.definition_version.get(),
        definition_digest: row.definition_digest.clone(),
        business_key: row.business_key.as_str().to_owned(),
        status: status as i32,
        control_state: control_state as i32,
        state_version: row.version,
        control_version: row.control_version,
        failure_code: row.failure_code.clone().unwrap_or_default(),
        created_at: None,
        updated_at: None,
        direction: row.direction.as_str().to_owned(),
        current_step: row
            .current_step
            .as_ref()
            .map(|step| step.as_str().to_owned()),
        deadline_at_ms: row.deadline_at_ms,
        traceparent: row.traceparent.clone().unwrap_or_default(),
    }
}

/// 业务作用：把检索摘要投影为带创建/更新时间的公开 protobuf 快照。
///
/// 参数说明：`row` 是不含 payload 的租户受限摘要。
///
/// 返回：保留业务身份、状态、版本和时间的稳定消息。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn managed_grpc_summary_snapshot(
    row: &nasaga_runtime::SagaInstanceSummary,
) -> nasaga_runtime::orchestrator_proto::SagaSnapshot {
    let mut snapshot = managed_grpc_instance_snapshot(&nasaga_runtime::SagaInstanceRow {
        saga_id: row.saga_id.clone(),
        tenant: row.tenant.clone(),
        workflow: row.workflow.clone(),
        business_key: row.business_key.clone(),
        definition_version: row.definition_version,
        definition_digest: row.definition_digest.clone(),
        start_request_digest: None,
        status: row.status,
        control_state: row.control_state,
        control_version: row.control_version,
        direction: row.direction,
        current_step: row.current_step.clone(),
        compensation_plan_version: None,
        version: row.version,
        deadline_at_ms: row.deadline_at_ms,
        failure_code: row.failure_code.clone(),
        traceparent: row.traceparent.clone(),
    });
    snapshot.created_at = Some(managed_grpc_timestamp(row.created_at_ms));
    snapshot.updated_at = Some(managed_grpc_timestamp(row.updated_at_ms));
    snapshot
}

/// 业务作用：把数据库无关的统一审计事实投影为公开 protobuf 记录并填充标准时间戳。
///
/// 参数说明：`record` 是运行时已完成租户门禁和 keyset 排序的一条事实。
///
/// 返回：返回不暴露表结构、但保留稳定身份与事实细节的 protobuf 记录。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn managed_grpc_audit_record(
    record: nasaga_runtime::SagaAuditRecord,
) -> nasaga_runtime::orchestrator_proto::SagaAuditRecord {
    use nasaga_runtime::orchestrator_proto as proto;
    match record {
        nasaga_runtime::SagaAuditRecord::Attempt(row) => {
            let attempt = row.attempt;
            proto::SagaAuditRecord {
                audit_id: format!(
                    "attempt:{}:{}:{}",
                    attempt.step.as_str(),
                    attempt.phase.as_str(),
                    attempt.attempt.get()
                ),
                kind: "attempt".to_owned(),
                state_version: 0,
                actor: String::new(),
                reason: attempt.status.as_str().to_owned(),
                occurred_at: Some(managed_grpc_timestamp(row.occurred_at_ms)),
                details: Some(proto::SagaPayload {
                    content_type: "application/json".to_owned(),
                    schema_id: "nasa.saga.audit.attempt".to_owned(),
                    body: serde_json::to_vec(&serde_json::json!({
                        "step": attempt.step.as_str(),
                        "phase": attempt.phase.as_str(),
                        "attempt": attempt.attempt.get(),
                        "effect_id": attempt.effect_id,
                        "command_id": attempt.command_id,
                        "status": attempt.status.as_str(),
                        "outcome_event_id": attempt.outcome_event_id,
                        "occurred_at": row.occurred_at,
                    }))
                    .unwrap_or_default(),
                }),
            }
        }
        nasaga_runtime::SagaAuditRecord::Transition(row) => proto::SagaAuditRecord {
            audit_id: format!("transition:{}", row.transition_seq),
            kind: "transition".to_owned(),
            state_version: row.transition_seq,
            actor: String::new(),
            reason: row.trigger_kind,
            occurred_at: Some(managed_grpc_timestamp(row.occurred_at_ms)),
            details: Some(proto::SagaPayload {
                content_type: "application/json".to_owned(),
                schema_id: "nasa.saga.audit.transition".to_owned(),
                body: serde_json::to_vec(&serde_json::json!({
                    "from_state": row.from_state,
                    "to_state": row.to_state,
                    "trigger_id": row.trigger_id,
                    "definition_version": row.definition_version,
                    "occurred_at": row.occurred_at,
                }))
                .unwrap_or_default(),
            }),
        },
        nasaga_runtime::SagaAuditRecord::Control(row) => proto::SagaAuditRecord {
            audit_id: format!("control:{}", row.control_seq),
            kind: "control".to_owned(),
            state_version: row.control_seq,
            actor: row.actor,
            reason: row.reason,
            occurred_at: Some(managed_grpc_timestamp(row.occurred_at_ms)),
            details: Some(proto::SagaPayload {
                content_type: "application/json".to_owned(),
                schema_id: "nasa.saga.audit.control".to_owned(),
                body: serde_json::to_vec(&serde_json::json!({
                    "from_state": row.from_state,
                    "to_state": row.to_state,
                    "operation_id": row.operation_id,
                    "occurred_at": row.occurred_at,
                }))
                .unwrap_or_default(),
            }),
        },
        nasaga_runtime::SagaAuditRecord::Management(row) => proto::SagaAuditRecord {
            audit_id: format!("management:{}", row.operation_id),
            kind: "management".to_owned(),
            state_version: 0,
            actor: row.actor,
            reason: row.reason,
            occurred_at: Some(managed_grpc_timestamp(row.occurred_at_ms)),
            details: Some(proto::SagaPayload {
                content_type: "application/json".to_owned(),
                schema_id: "nasa.saga.audit.management".to_owned(),
                body: serde_json::to_vec(&serde_json::json!({
                    "operation_id": row.operation_id,
                    "action": row.action,
                    "occurred_at": row.occurred_at,
                }))
                .unwrap_or_default(),
            }),
        },
        nasaga_runtime::SagaAuditRecord::Conflict(row) => proto::SagaAuditRecord {
            audit_id: format!("conflict:{}", row.incoming_event_id),
            kind: "conflict".to_owned(),
            state_version: 0,
            actor: String::new(),
            reason: row.conflict_kind.clone(),
            occurred_at: Some(managed_grpc_timestamp(row.occurred_at_ms)),
            details: Some(proto::SagaPayload {
                content_type: "application/json".to_owned(),
                schema_id: "nasa.saga.audit.conflict".to_owned(),
                body: serde_json::to_vec(&serde_json::json!({
                    "incoming_event_id": row.incoming_event_id,
                    "step": row.step.as_str(),
                    "phase": row.phase.as_str(),
                    "attempt": row.attempt.get(),
                    "existing_status": row.existing_status.as_str(),
                    "incoming_status": row.incoming_status.as_str(),
                    "conflict_kind": row.conflict_kind,
                    "occurred_at": row.occurred_at,
                }))
                .unwrap_or_default(),
            }),
        },
    }
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
#[nagrpc::async_trait]
impl nasaga_runtime::orchestrator_proto::saga_orchestrator_server::SagaOrchestrator
    for ManagedGrpcApiService
{
    /// 业务作用：通过公开 gRPC 合同发起 Saga，并复用唯一创建摘要和首步原子事务。
    ///
    /// 参数说明：请求显式携带租户、Saga、definition 版本、业务键、trigger 与原始 JSON payload。
    ///
    /// 返回：首次创建或幂等命中返回快照；参数、权限、摘要冲突和暂时失败使用标准 status。
    async fn start_saga(
        &self,
        request: nagrpc::Request<nasaga_runtime::orchestrator_proto::StartSagaRequest>,
    ) -> Result<
        nagrpc::Response<nasaga_runtime::orchestrator_proto::StartSagaResponse>,
        nagrpc::Status,
    > {
        use nasaga_runtime::orchestrator_proto as proto;
        self.state
            .ensure_ready()
            .map_err(|_| nagrpc::Status::unavailable("Saga runtime is not ready"))?;
        let caller = managed_grpc_api_caller(&request, &self.callers)?;
        let trace = managed_grpc_trace(&request);
        let request = request.into_inner();
        let key = request
            .key
            .ok_or_else(|| nagrpc::Status::invalid_argument("Saga key is required"))?;
        let input = start_payload::decode_grpc_start_payload(request.input)?;
        let deadline_at_ms = request
            .deadline
            .as_ref()
            .map(managed_grpc_timestamp_ms)
            .transpose()?;
        let orchestrator = self.state.managed_orchestrator().ok_or_else(|| {
            nagrpc::Status::failed_precondition("Saga Orchestrator is unavailable")
        })?;
        let outcome = orchestrator
            .start_instance(
                &caller,
                api::SagaApiStart {
                    tenant_id: key.tenant_id,
                    saga_id: key.saga_id,
                    workflow: request.workflow,
                    definition_version: request.definition_version,
                    expected_definition_digest: (!request.expected_definition_digest.is_empty())
                        .then_some(request.expected_definition_digest),
                    business_key: request.business_key,
                    trigger_id: request.trigger_id,
                    deadline_at_ms,
                    input: None,
                    payload: input,
                },
                trace.as_ref(),
                &self.state,
            )
            .await
            .map_err(api::SagaApiStartFailure::grpc_status)?;
        let (disposition, row) = match outcome {
            nasaga_runtime::StartOutcome::Started(row) => (proto::StartDisposition::Committed, row),
            nasaga_runtime::StartOutcome::AlreadyExists(row) => {
                (proto::StartDisposition::Duplicate, row)
            }
        };
        Ok(nagrpc::Response::new(proto::StartSagaResponse {
            disposition: disposition as i32,
            request_digest: row.start_request_digest.clone().unwrap_or_default(),
            saga: Some(managed_grpc_instance_snapshot(&row)),
        }))
    }

    /// 业务作用：读取同租户实例快照，并在检查存在性前完成 mTLS 与租户权限门禁。
    ///
    /// 参数说明：请求 key 共同定位租户与实例。
    ///
    /// 返回：命中返回快照；不存在返回 NotFound，其它失败返回标准 status。
    async fn get_saga(
        &self,
        request: nagrpc::Request<nasaga_runtime::orchestrator_proto::GetSagaRequest>,
    ) -> Result<nagrpc::Response<nasaga_runtime::orchestrator_proto::SagaSnapshot>, nagrpc::Status>
    {
        self.state
            .ensure_ready()
            .map_err(|_| nagrpc::Status::unavailable("Saga runtime is not ready"))?;
        let caller = managed_grpc_api_caller(&request, &self.callers)?;
        let key = request
            .into_inner()
            .key
            .ok_or_else(|| nagrpc::Status::invalid_argument("Saga key is required"))?;
        let orchestrator = self.state.managed_orchestrator().ok_or_else(|| {
            nagrpc::Status::failed_precondition("Saga Orchestrator is unavailable")
        })?;
        let row = orchestrator
            .get_instance(&caller, &key.tenant_id, &key.saga_id)
            .await
            .map_err(ManagedApiFailure::grpc_status)?;
        Ok(nagrpc::Response::new(managed_grpc_instance_snapshot(&row)))
    }

    /// 业务作用：按租户、workflow、状态和不透明 keyset token 执行有界实例检索。
    ///
    /// 参数说明：请求过滤条件与 token 必须保持一致，page_size 范围为 1..=1000。
    ///
    /// 返回：返回一页摘要及下一页 token；非法 token 或权限不足不访问数据库。
    async fn query_sagas(
        &self,
        request: nagrpc::Request<nasaga_runtime::orchestrator_proto::QuerySagasRequest>,
    ) -> Result<
        nagrpc::Response<nasaga_runtime::orchestrator_proto::QuerySagasResponse>,
        nagrpc::Status,
    > {
        use nasaga_runtime::orchestrator_proto as proto;
        self.state
            .ensure_ready()
            .map_err(|_| nagrpc::Status::unavailable("Saga runtime is not ready"))?;
        let caller = managed_grpc_api_caller(&request, &self.callers)?;
        let request = request.into_inner();
        let statuses = request
            .statuses
            .iter()
            .map(|value| {
                proto::SagaStatus::try_from(*value)
                    .ok()
                    .filter(|value| *value != proto::SagaStatus::Unspecified)
                    .map(|value| {
                        value
                            .as_str_name()
                            .trim_start_matches("SAGA_STATUS_")
                            .to_owned()
                    })
                    .ok_or_else(|| nagrpc::Status::invalid_argument("Saga status is invalid"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let orchestrator = self.state.managed_orchestrator().ok_or_else(|| {
            nagrpc::Status::failed_precondition("Saga Orchestrator is unavailable")
        })?;
        let page = orchestrator
            .query_instances(
                &caller,
                api::SagaApiQuery {
                    tenant_id: request.tenant_id,
                    workflow: (!request.workflow.is_empty()).then_some(request.workflow),
                    statuses,
                    created_from_ms: request.created_from_ms,
                    created_to_ms: request.created_to_ms,
                    after_saga_id: None,
                    page_token: (!request.page_token.is_empty()).then_some(request.page_token),
                    page_size: Some(request.page_size),
                },
                &self.page_token_key,
            )
            .await
            .map_err(|error| match error {
                ManagedApiFailure::Invalid => {
                    nagrpc::Status::invalid_argument("Saga query is invalid")
                }
                ManagedApiFailure::PermissionDenied => {
                    nagrpc::Status::permission_denied("Saga query permission is required")
                }
                _ => nagrpc::Status::unavailable("Saga query is unavailable"),
            })?;
        let rows = page.rows;
        let next_page_token = page.next_page_token.unwrap_or_default();
        Ok(nagrpc::Response::new(proto::QuerySagasResponse {
            sagas: rows.iter().map(managed_grpc_summary_snapshot).collect(),
            next_page_token,
        }))
    }

    /// 业务作用：读取有界审计事实并投影为统一记录，不暴露数据库表结构。
    ///
    /// 参数说明：请求 key、page_size 与不透明有界游标定位审计页。
    ///
    /// 返回：授权且实例存在时返回记录；非法游标、越权或存储失败返回标准 status。
    async fn get_audit_trail(
        &self,
        request: nagrpc::Request<nasaga_runtime::orchestrator_proto::GetAuditTrailRequest>,
    ) -> Result<
        nagrpc::Response<nasaga_runtime::orchestrator_proto::GetAuditTrailResponse>,
        nagrpc::Status,
    > {
        use nasaga_runtime::orchestrator_proto as proto;
        self.state
            .ensure_ready()
            .map_err(|_| nagrpc::Status::unavailable("Saga runtime is not ready"))?;
        let caller = managed_grpc_api_caller(&request, &self.callers)?;
        let request = request.into_inner();
        let key = request
            .key
            .ok_or_else(|| nagrpc::Status::invalid_argument("Saga key is required"))?;
        let orchestrator = self.state.managed_orchestrator().ok_or_else(|| {
            nagrpc::Status::failed_precondition("Saga Orchestrator is unavailable")
        })?;
        let page = orchestrator
            .query_audit(
                &caller,
                api::SagaApiAudit {
                    tenant_id: key.tenant_id,
                    saga_id: key.saga_id,
                    page_size: Some(request.page_size),
                    page_token: (!request.page_token.is_empty()).then_some(request.page_token),
                },
                &self.page_token_key,
            )
            .await
            .map_err(ManagedApiFailure::grpc_status)?;
        let records = page
            .records
            .into_iter()
            .map(managed_grpc_audit_record)
            .collect();
        let next_page_token = page.next_page_token.unwrap_or_default();
        Ok(nagrpc::Response::new(proto::GetAuditTrailResponse {
            records,
            next_page_token,
        }))
    }
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
impl ManagedGrpcApiService {
    /// 业务作用：执行一个经 mTLS/RBAC 认证的管理动作，并以 expected version 防止覆盖并发推进。
    ///
    /// 参数说明：`request` 携带实例、操作身份、原因和可选 CAS 版本，`action` 是方法名冻结的动作。
    ///
    /// 返回：动作与审计同事务提交后返回最新快照；越权、不存在、CAS 或状态冲突返回标准 status。
    async fn execute_admin(
        &self,
        request: nagrpc::Request<nasaga_runtime::orchestrator_proto::SagaManagementRequest>,
        action: ManagedAdminAction,
    ) -> Result<nagrpc::Response<nasaga_runtime::orchestrator_proto::SagaSnapshot>, nagrpc::Status>
    {
        self.state
            .ensure_ready()
            .map_err(|_| nagrpc::Status::unavailable("Saga runtime is not ready"))?;
        let caller = managed_grpc_api_caller(&request, &self.callers)?;
        let request = request.into_inner();
        let key = request
            .key
            .ok_or_else(|| nagrpc::Status::invalid_argument("Saga key is required"))?;
        let orchestrator = self.state.managed_orchestrator().ok_or_else(|| {
            nagrpc::Status::failed_precondition("Saga Orchestrator is unavailable")
        })?;
        let after = orchestrator
            .administer_instance(
                &caller,
                action,
                api::SagaApiAdmin {
                    tenant_id: key.tenant_id,
                    saga_id: key.saga_id,
                    operation_id: request.operation_id,
                    reason: request.reason,
                    expected_state_version: request.expected_state_version,
                    expected_control_version: request.expected_control_version,
                },
            )
            .await
            .map_err(ManagedApiFailure::grpc_status)?;
        Ok(nagrpc::Response::new(managed_grpc_instance_snapshot(
            &after,
        )))
    }
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
#[nagrpc::async_trait]
impl nasaga_runtime::orchestrator_proto::saga_orchestrator_admin_server::SagaOrchestratorAdmin
    for ManagedGrpcApiService
{
    /// 业务作用：暂停实例自动推进并持久记录认证主体与原因。
    ///
    /// 参数说明：`request` 携带目标实例、幂等操作号、原因以及受信调用方元数据。
    ///
    /// 返回：管理事务提交后返回最新实例快照；身份、配额或状态门禁不满足时拒绝操作。
    async fn pause_saga(
        &self,
        request: nagrpc::Request<nasaga_runtime::orchestrator_proto::SagaManagementRequest>,
    ) -> Result<nagrpc::Response<nasaga_runtime::orchestrator_proto::SagaSnapshot>, nagrpc::Status>
    {
        self.execute_admin(request, ManagedAdminAction::Pause).await
    }

    /// 业务作用：恢复实例自动推进且不重置已过去的业务期限。
    ///
    /// 参数说明：`request` 携带目标实例、幂等操作号、原因以及受信调用方元数据。
    ///
    /// 返回：管理事务提交后返回最新实例快照；身份、配额或状态门禁不满足时拒绝操作。
    async fn resume_saga(
        &self,
        request: nagrpc::Request<nasaga_runtime::orchestrator_proto::SagaManagementRequest>,
    ) -> Result<nagrpc::Response<nasaga_runtime::orchestrator_proto::SagaSnapshot>, nagrpc::Status>
    {
        self.execute_admin(request, ManagedAdminAction::Resume)
            .await
    }

    /// 业务作用：沿冻结逆序计划开启受审计的补偿重试。
    ///
    /// 参数说明：`request` 携带目标实例、幂等操作号、原因以及受信调用方元数据。
    ///
    /// 返回：补偿重试事务提交后返回最新实例快照；非补偿状态或越权请求被拒绝。
    async fn retry_compensation(
        &self,
        request: nagrpc::Request<nasaga_runtime::orchestrator_proto::SagaManagementRequest>,
    ) -> Result<nagrpc::Response<nasaga_runtime::orchestrator_proto::SagaSnapshot>, nagrpc::Status>
    {
        self.execute_admin(request, ManagedAdminAction::RetryCompensation)
            .await
    }

    /// 业务作用：为 Unknown 效果开启受审计的新 resolve 周期。
    ///
    /// 参数说明：`request` 携带目标实例、幂等操作号、原因以及受信调用方元数据。
    ///
    /// 返回：resolve 重试事务提交后返回最新实例快照；不存在 Unknown 效果时拒绝操作。
    async fn retry_resolution(
        &self,
        request: nagrpc::Request<nasaga_runtime::orchestrator_proto::SagaManagementRequest>,
    ) -> Result<nagrpc::Response<nasaga_runtime::orchestrator_proto::SagaSnapshot>, nagrpc::Status>
    {
        self.execute_admin(request, ManagedAdminAction::RetryResolution)
            .await
    }

    /// 业务作用：记录系统外处置并终止自动推进，不伪造成功业务事实。
    ///
    /// 参数说明：`request` 携带目标实例、幂等操作号、关闭原因以及受信调用方元数据。
    ///
    /// 返回：人工关闭事实提交后返回最新实例快照；身份或状态门禁不满足时拒绝操作。
    async fn manual_close(
        &self,
        request: nagrpc::Request<nasaga_runtime::orchestrator_proto::SagaManagementRequest>,
    ) -> Result<nagrpc::Response<nasaga_runtime::orchestrator_proto::SagaSnapshot>, nagrpc::Status>
    {
        self.execute_admin(request, ManagedAdminAction::ManualClose)
            .await
    }
}

/// 业务作用：计算协议 canonical document 的小写 SHA-256，避免信任调用方自报摘要。
///
/// 参数说明：`document` 是 protobuf 中未经改写的原始文档字节。
///
/// 返回：64 个小写十六进制字符。
fn managed_document_sha256(document: &[u8]) -> String {
    Sha256::digest(document)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// 业务作用：把持久 definition 记录投影为 gRPC 生命周期收据。
///
/// 参数说明：`record` 已复验 artifact seal 与数据库 key 一致。
///
/// 返回：包含完整 key、seal、生命周期和 Catalog generation 的消息。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn managed_grpc_definition_record(
    record: &nasaga_runtime::DefinitionRecord,
) -> nasaga_runtime::orchestrator_proto::DefinitionRecord {
    use nasaga_runtime::orchestrator_proto as proto;
    let lifecycle = match record.lifecycle {
        nasaga_runtime::DefinitionLifecycle::Candidate => proto::DefinitionLifecycle::Candidate,
        nasaga_runtime::DefinitionLifecycle::Active => proto::DefinitionLifecycle::Active,
        nasaga_runtime::DefinitionLifecycle::Deprecated => proto::DefinitionLifecycle::Deprecated,
        nasaga_runtime::DefinitionLifecycle::Retired => proto::DefinitionLifecycle::Retired,
    };
    proto::DefinitionRecord {
        key: Some(proto::DefinitionKey {
            tenant_id: record.artifact.tenant.clone(),
            workflow: record.artifact.workflow.clone(),
            definition_version: record.artifact.definition_version,
        }),
        sha256: record.artifact.seal.clone(),
        lifecycle: lifecycle as i32,
        catalog_generation: record.catalog_generation,
        published_at: Some(managed_grpc_timestamp(record.published_at_ms)),
        activated_at: record.activated_at_ms.map(managed_grpc_timestamp),
    }
}

/// 业务作用：按当前 Orchestrator datasource/driver 读取完整 definition 记录。
///
/// 参数说明：runtime 固定持久后端，tenant/workflow/version 定位定义。
///
/// 返回：存在返回记录，不存在返回空，数据库或内容漂移返回错误。
#[cfg(any(feature = "web", feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
async fn load_managed_definition(
    runtime: &SagaOrchestratorApi,
    tenant: &str,
    workflow: &str,
    version: u32,
) -> anyhow::Result<Option<nasaga_runtime::DefinitionRecord>> {
    match runtime.driver() {
        natx_core::DatabaseDriver::MySql => {
            #[cfg(feature = "saga")]
            return nasaga_runtime::load_definition_for(
                runtime.datasource_ref().as_str(),
                tenant,
                workflow,
                version,
            )
            .await;
            #[cfg(not(feature = "saga"))]
            anyhow::bail!("MySQL Saga runtime is unavailable");
        }
        natx_core::DatabaseDriver::PostgreSql => {
            #[cfg(feature = "saga-pgsql")]
            return nasaga_runtime_pgsql::load_definition_for(
                runtime.datasource_ref().as_str(),
                tenant,
                workflow,
                version,
            )
            .await;
            #[cfg(not(feature = "saga-pgsql"))]
            anyhow::bail!("PostgreSQL Saga runtime is unavailable");
        }
    }
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
#[nagrpc::async_trait]
impl nasaga_runtime::orchestrator_proto::saga_definition_registry_server::SagaDefinitionRegistry
    for ManagedGrpcApiService
{
    /// 业务作用：登记或续租认证 owner 的逐实例 capability，不允许请求字段冒充其它服务。
    ///
    /// 参数说明：canonical_document 必须是 runtime CapabilityDescriptor JSON，key/owner/step/摘要必须一致。
    ///
    /// 返回：运行期间数据库时钟裁剪后的租约收据，Catalog 摘流不阻止合法续租；认证、合同、摘要、停机或持久化失败返回标准 status。
    async fn register_capability(
        &self,
        request: nagrpc::Request<nasaga_runtime::orchestrator_proto::RegisterCapabilityRequest>,
    ) -> Result<
        nagrpc::Response<nasaga_runtime::orchestrator_proto::CapabilityReceipt>,
        nagrpc::Status,
    > {
        use nasaga_runtime::orchestrator_proto as proto;
        // 续租用于恢复缺失路由，不能依赖待恢复的 Catalog 业务资格；认证和授权仍完整执行。
        self.state
            .ensure_running()
            .map_err(|_| nagrpc::Status::unavailable("Saga runtime is not ready"))?;
        let caller = managed_grpc_api_caller(&request, &self.callers)?;
        let request = request.into_inner();
        let capability = request
            .capability
            .ok_or_else(|| nagrpc::Status::invalid_argument("Saga capability is required"))?;
        let key = capability.definition.ok_or_else(|| {
            nagrpc::Status::invalid_argument("Saga capability definition key is required")
        })?;
        authorize_managed_grpc_registry(&caller, &key.tenant_id, &key.workflow)?;
        if capability.descriptor_format != "nasaga-capability-json"
            || managed_document_sha256(&capability.canonical_document) != capability.sha256
        {
            return Err(nagrpc::Status::invalid_argument(
                "Saga capability document contract is invalid",
            ));
        }
        let mut descriptor: nasaga_runtime::CapabilityDescriptor =
            serde_json::from_slice(&capability.canonical_document).map_err(|_| {
                nagrpc::Status::invalid_argument("Saga capability document is invalid")
            })?;
        if descriptor.owner != caller.identity.as_str()
            || descriptor.tenant != key.tenant_id
            || descriptor.owner != capability.owner
            || descriptor.workflow != key.workflow
            || descriptor.definition_version != key.definition_version
            || descriptor.step != capability.step
            || descriptor.replica_identity != request.registration_id
        {
            return Err(nagrpc::Status::permission_denied(
                "Saga capability identity does not match the authenticated caller",
            ));
        }
        if capability.requested_lease_seconds == 0 {
            return Err(nagrpc::Status::invalid_argument(
                "Saga capability lease is invalid",
            ));
        }
        descriptor.requested_lease_ms = u64::from(capability.requested_lease_seconds)
            .checked_mul(1_000)
            .ok_or_else(|| nagrpc::Status::invalid_argument("Saga capability lease is invalid"))?;
        let runtime = self.state.managed_orchestrator().ok_or_else(|| {
            nagrpc::Status::failed_precondition("Saga Orchestrator is unavailable")
        })?;
        // 登记前复验运行状态，停机中的请求不能延长参与资格。
        self.state
            .ensure_running()
            .map_err(|_| nagrpc::Status::unavailable("Saga runtime is stopping"))?;
        let receipt = runtime
            .register_capability_record(&caller, &self.activation, &descriptor)
            .await
            .map_err(|error| managed_grpc_catalog_status(&error, "capability registration"))?;
        Ok(nagrpc::Response::new(proto::CapabilityReceipt {
            registration_id: request.registration_id,
            capability_digest: receipt.capability_digest,
            accepted_until: Some(managed_grpc_timestamp(receipt.accepted_until_ms)),
            route_generation: receipt.route_generation,
        }))
    }

    /// 业务作用：持久化认证 workflow owner 的完整不可变 definition seal。
    ///
    /// 参数说明：canonical_document 必须是 runtime DefinitionArtifact JSON，protobuf key 与摘要需一致。
    ///
    /// 返回：首次或幂等发布返回 candidate 记录；同 key 异摘要、越权或持久化失败返回标准 status。
    async fn publish_definition(
        &self,
        request: nagrpc::Request<nasaga_runtime::orchestrator_proto::PublishDefinitionRequest>,
    ) -> Result<
        nagrpc::Response<nasaga_runtime::orchestrator_proto::PublishDefinitionResponse>,
        nagrpc::Status,
    > {
        use nasaga_runtime::orchestrator_proto as proto;
        self.state
            .ensure_running()
            .map_err(|_| nagrpc::Status::unavailable("Saga runtime is not ready"))?;
        let caller = managed_grpc_api_caller(&request, &self.callers)?;
        let artifact = request
            .into_inner()
            .artifact
            .ok_or_else(|| nagrpc::Status::invalid_argument("Saga definition is required"))?;
        let key = artifact
            .key
            .ok_or_else(|| nagrpc::Status::invalid_argument("Saga definition key is required"))?;
        authorize_managed_grpc_registry(&caller, &key.tenant_id, &key.workflow)?;
        let signed = ManagedSignedDefinitionArtifact {
            format: artifact.format,
            canonical_document: String::from_utf8(artifact.canonical_document).map_err(|_| {
                nagrpc::Status::invalid_argument("Saga definition document must be UTF-8")
            })?,
            sha256: artifact.sha256,
            signing_key_id: artifact.signing_key_id,
            signature: String::from_utf8(artifact.signature).map_err(|_| {
                nagrpc::Status::invalid_argument("Saga definition signature must be Base64 text")
            })?,
        };
        let definition = SagaOrchestratorApi::validate_signed_definition(
            &caller,
            &self.definition_signing_keys,
            &signed,
        )
        .map_err(|error| managed_grpc_catalog_status(&error, "definition validation"))?;
        if definition.tenant != key.tenant_id
            || definition.workflow != key.workflow
            || definition.definition_version != key.definition_version
        {
            return Err(nagrpc::Status::permission_denied(
                "Saga definition document does not match its envelope key",
            ));
        }
        let runtime = self.state.managed_orchestrator().ok_or_else(|| {
            nagrpc::Status::failed_precondition("Saga Orchestrator is unavailable")
        })?;
        let result = runtime
            .publish_definition_record(&caller, &definition)
            .await
            .map_err(|error| managed_grpc_catalog_status(&error, "definition publication"))?;
        let (disposition, record) = result;
        let disposition = match disposition {
            nasaga_runtime::DefinitionPublishDisposition::Published => {
                proto::DefinitionPublishDisposition::Committed
            }
            nasaga_runtime::DefinitionPublishDisposition::Duplicate => {
                proto::DefinitionPublishDisposition::Duplicate
            }
        };
        Ok(nagrpc::Response::new(proto::PublishDefinitionResponse {
            disposition: disposition as i32,
            definition: Some(managed_grpc_definition_record(&record)),
        }))
    }

    /// 业务作用：激活已满足 schema、capability、route 和集群确认门禁的 candidate definition。
    ///
    /// 参数说明：`request` 携带完整 definition key 与受信调用方元数据。
    ///
    /// 返回：原子切换后返回 active 记录；任一激活前置条件不成立时保持原状态并拒绝请求。
    async fn activate_definition(
        &self,
        request: nagrpc::Request<nasaga_runtime::orchestrator_proto::DefinitionOperationRequest>,
    ) -> Result<
        nagrpc::Response<nasaga_runtime::orchestrator_proto::DefinitionRecord>,
        nagrpc::Status,
    > {
        self.change_definition(request, nasaga_runtime::DefinitionLifecycle::Active)
            .await
    }

    /// 业务作用：将 active definition 标记为 deprecated，只阻止新 start 而不打断在途实例。
    ///
    /// 参数说明：`request` 携带完整 definition key 与受信调用方元数据。
    ///
    /// 返回：状态提交后返回 deprecated 记录；目标不存在或调用方越权时拒绝请求。
    async fn deprecate_definition(
        &self,
        request: nagrpc::Request<nasaga_runtime::orchestrator_proto::DefinitionOperationRequest>,
    ) -> Result<
        nagrpc::Response<nasaga_runtime::orchestrator_proto::DefinitionRecord>,
        nagrpc::Status,
    > {
        self.change_definition(request, nasaga_runtime::DefinitionLifecycle::Deprecated)
            .await
    }

    /// 业务作用：由 workflow owner 退休已经释放持久引用的 deprecated definition。
    ///
    /// 参数说明：`request` 绑定定义键、预期摘要、操作身份和审计原因。
    ///
    /// 返回：原子提交后返回 retired 记录；仍有引用、越权或摘要冲突时拒绝变更。
    async fn retire_definition(
        &self,
        request: nagrpc::Request<nasaga_runtime::orchestrator_proto::DefinitionOperationRequest>,
    ) -> Result<
        nagrpc::Response<nasaga_runtime::orchestrator_proto::DefinitionRecord>,
        nagrpc::Status,
    > {
        self.change_definition(request, nasaga_runtime::DefinitionLifecycle::Retired)
            .await
    }

    /// 业务作用：按完整 key 读取当前 definition 生命周期，且先执行租户 registry 权限门禁。
    ///
    /// 参数说明：`request` 携带完整 definition key 与受信调用方元数据。
    ///
    /// 返回：授权通过时返回持久记录；目标不存在、运行时未就绪或调用方越权时返回对应协议错误。
    async fn get_definition(
        &self,
        request: nagrpc::Request<nasaga_runtime::orchestrator_proto::GetDefinitionRequest>,
    ) -> Result<
        nagrpc::Response<nasaga_runtime::orchestrator_proto::DefinitionRecord>,
        nagrpc::Status,
    > {
        self.state
            .ensure_running()
            .map_err(|_| nagrpc::Status::unavailable("Saga runtime is not ready"))?;
        let caller = managed_grpc_api_caller(&request, &self.callers)?;
        let key = request
            .into_inner()
            .key
            .ok_or_else(|| nagrpc::Status::invalid_argument("Saga definition key is required"))?;
        let runtime = self.state.managed_orchestrator().ok_or_else(|| {
            nagrpc::Status::failed_precondition("Saga Orchestrator is unavailable")
        })?;
        let record = runtime
            .get_definition_record(
                &caller,
                &key.tenant_id,
                &key.workflow,
                key.definition_version,
            )
            .await
            .map_err(ManagedApiFailure::grpc_status)?;
        Ok(nagrpc::Response::new(managed_grpc_definition_record(
            &record,
        )))
    }
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
impl ManagedGrpcApiService {
    /// 业务作用：复用一个封闭入口执行 definition 生命周期迁移并复验 expected digest。
    ///
    /// 参数说明：请求携带完整 key、预期 seal、幂等操作身份和原因，`target` 选择生命周期动作。
    ///
    /// 返回：变更提交后返回记录；越权、摘要、状态或集群门禁失败返回标准 status。
    async fn change_definition(
        &self,
        request: nagrpc::Request<nasaga_runtime::orchestrator_proto::DefinitionOperationRequest>,
        target: nasaga_runtime::DefinitionLifecycle,
    ) -> Result<
        nagrpc::Response<nasaga_runtime::orchestrator_proto::DefinitionRecord>,
        nagrpc::Status,
    > {
        self.state
            .ensure_running()
            .map_err(|_| nagrpc::Status::unavailable("Saga runtime is not ready"))?;
        let caller = managed_grpc_api_caller(&request, &self.callers)?;
        let request = request.into_inner();
        let key = request
            .key
            .ok_or_else(|| nagrpc::Status::invalid_argument("Saga definition key is required"))?;
        authorize_managed_grpc_registry(&caller, &key.tenant_id, &key.workflow)?;
        let operation = nasaga_runtime::DefinitionLifecycleOperation::new(
            request.expected_sha256,
            request.operation_id,
            request.reason,
        )
        .map_err(|_| nagrpc::Status::invalid_argument("Saga definition operation is invalid"))?;
        let runtime = self.state.managed_orchestrator().ok_or_else(|| {
            nagrpc::Status::failed_precondition("Saga Orchestrator is unavailable")
        })?;
        let changed = runtime
            .change_definition_record(
                &caller,
                (&key.tenant_id, &key.workflow, key.definition_version),
                target,
                &operation,
                &self.orchestrator_service_identity,
                &self.activation,
            )
            .await
            .map_err(|error| managed_grpc_catalog_status(&error, "lifecycle change"))?;
        Ok(nagrpc::Response::new(managed_grpc_definition_record(
            &changed,
        )))
    }
}

#[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
impl nafka::SingleConsumer for ManagedKafkaCommandConsumer {
    type Message = nasaga_runtime::SagaCommandEnvelope;

    /// 业务作用：返回当前参与方 descriptor 唯一派生的 command topics。
    ///
    /// 参数说明：无。
    ///
    /// 返回：冻结路由表中的全部 command topic，不接受运行期任意订阅扩张。
    fn topics(&self) -> Vec<String> {
        self.routes.keys().cloned().collect()
    }

    /// 业务作用：只接收 Saga command 事件类型。
    ///
    /// 参数说明：无。
    ///
    /// 返回：受管 Saga command 的固定 CloudEvents 类型。
    fn event(&self) -> String {
        nasaga_runtime::COMMAND_EVENT_TYPE.to_owned()
    }

    /// 业务作用：返回重启稳定的参与方 command consumer group。
    ///
    /// 参数说明：无。
    ///
    /// 返回：由参与方身份冻结得到的命名消费组。
    fn group(&self) -> nafka::GroupSpec {
        nafka::GroupSpec::Named(self.group.clone())
    }

    /// 业务作用：提供受管 command consumer 的唯一注册身份。
    ///
    /// 参数说明：无。
    ///
    /// 返回：进程内稳定且唯一的 consumer id。
    fn id(&self) -> &'static str {
        "napp::managed-saga-kafka-command"
    }

    /// 业务作用：强制本地事务提交后再手动确认 Kafka offset。
    ///
    /// 参数说明：无。
    ///
    /// 返回：固定返回手动确认模式，避免业务事务前移之前丢失源事件。
    fn ack_mode(&self) -> nafka::AckMode {
        nafka::AckMode::Manual
    }

    /// 业务作用：按 topic 与 envelope 身份选择唯一 handler，并在本地提交后确认消息。
    ///
    /// 参数说明：`record` 携带已解码命令、可信投递次数和回调期 ACK 能力。
    ///
    /// 返回：提交或 Inbox 重复时确认成功；瞬态/不确定结果保留，确定性合同错误进入 DLT。
    async fn consume(&self, record: nafka::KafkaRecord<Self::Message>) -> nafka::Result<()> {
        let envelope = &record.value;
        let route_matches =
            self.routes
                .get(&record.ctx.topic)
                .is_some_and(|(_, workflow, version, step)| {
                    workflow == &envelope.workflow
                        && *version == envelope.definition_version
                        && step == &envelope.step
                });
        if !route_matches {
            return Err(nafka::NafkaError::HandlerDeadLetter(
                "saga_command_topic_route_unauthorized".into(),
            ));
        }
        if self.state.ensure_ready().is_err() {
            // Runtime 未 Ready 或动态目录正在切代时不能消耗 offset；保留消息等待同一权威恢复。
            return Err(nafka::NafkaError::HandlerDeferred(
                "saga_runtime_not_ready".into(),
            ));
        }
        let result = match self.state.command_handler(envelope) {
            Some(handler) => {
                handler
                    .handle(
                        envelope,
                        &self.producer,
                        record.ctx.trace_context().as_ref(),
                    )
                    .await
            }
            None => Err(nasaga_runtime::SagaCommandProcessingError::RouteUnauthorized.into()),
        };
        match result {
            Ok(_) => record.ack(),
            Err(error) => match self
                .delivery_policy
                .decide(&error, record.ctx.retry_attempt)
            {
                nasaga_runtime::CommandDeliveryDisposition::Retry => Err(
                    nafka::NafkaError::Broker("Saga command transaction is not committed".into()),
                ),
                nasaga_runtime::CommandDeliveryDisposition::Defer => {
                    Err(nafka::NafkaError::HandlerDeferred(
                        "saga_command_transaction_outcome_unresolved".into(),
                    ))
                }
                nasaga_runtime::CommandDeliveryDisposition::DeadLetter => {
                    Err(nafka::NafkaError::HandlerDeadLetter(
                        nasaga_runtime::command_dead_letter_reason(&error)
                            .unwrap_or("saga_command_retry_exhausted")
                            .into(),
                    ))
                }
            },
        }
    }
}

#[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
impl nafka::SingleConsumer for ManagedKafkaResultConsumer {
    type Message = nasaga_runtime::SagaResultEnvelope;

    /// 业务作用：返回 result owner topic 的 broker 正则订阅，支持新 owner 无重启加入。
    ///
    /// 参数说明：无。
    ///
    /// 返回：由租户和 workflow 前缀冻结得到的唯一 topic 正则。
    fn topics(&self) -> Vec<String> {
        vec![self.topic_pattern.clone()]
    }

    /// 业务作用：只接收 Saga result 事件类型。
    ///
    /// 参数说明：无。
    ///
    /// 返回：受管 Saga result 的固定 CloudEvents 类型。
    fn event(&self) -> String {
        nasaga_runtime::RESULT_EVENT_TYPE.to_owned()
    }

    /// 业务作用：返回重启稳定的 Orchestrator result consumer group。
    ///
    /// 参数说明：无。
    ///
    /// 返回：由 Orchestrator 身份冻结得到的命名消费组。
    fn group(&self) -> nafka::GroupSpec {
        nafka::GroupSpec::Named(self.group.clone())
    }

    /// 业务作用：提供受管 result consumer 的唯一注册身份。
    ///
    /// 参数说明：无。
    ///
    /// 返回：进程内稳定且唯一的 consumer id。
    fn id(&self) -> &'static str {
        "napp::managed-saga-kafka-result"
    }

    /// 业务作用：强制 Orchestrator 推进事务提交后再确认 Kafka offset。
    ///
    /// 参数说明：无。
    ///
    /// 返回：固定返回手动确认模式，避免状态事务前移之前丢失源事件。
    fn ack_mode(&self) -> nafka::AckMode {
        nafka::AckMode::Manual
    }

    /// 业务作用：从实际 owner topic 派生可信 producer，并在结果事务提交后确认消息。
    ///
    /// 参数说明：`record` 携带已解码结果、实际 topic 与回调期 ACK 能力。
    ///
    /// 返回：推进或重复吸收时确认；暂时失败保留，确定性拒绝进入 DLT。
    async fn consume(&self, record: nafka::KafkaRecord<Self::Message>) -> nafka::Result<()> {
        if record
            .ctx
            .headers
            .last(MANAGED_KAFKA_RESULT_PROBE_HEADER)
            .and_then(|header| header.value.as_deref())
            == Some(b"1")
        {
            // 探针只证明 participant 对独占 result topic 拥有 broker 写权限，不进入业务状态机。
            return record.ack();
        }
        let permit = self.state.result_permit().map_err(|_| {
            // 定义或身份信任尚未确认时保留结果，不消耗消息隔离预算。
            nafka::NafkaError::HandlerDeferred("saga_runtime_not_ready".into())
        })?;
        let encoded_owner = record
            .ctx
            .topic
            .strip_prefix(&self.topic_prefix)
            .filter(|owner| !owner.is_empty())
            .ok_or_else(|| {
                nafka::NafkaError::HandlerDeadLetter("saga_result_topic_owner_unbound".into())
            })?;
        use base64::Engine as _;
        let owner = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded_owner)
            .ok()
            .and_then(|owner| String::from_utf8(owner).ok())
            .ok_or_else(|| {
                nafka::NafkaError::HandlerDeadLetter("saga_result_topic_owner_invalid".into())
            })?;
        let producer = nasaga_runtime::ServiceIdentity::new(&owner).map_err(|_| {
            nafka::NafkaError::HandlerDeadLetter("saga_result_topic_owner_invalid".into())
        })?;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
            .ok_or_else(|| nafka::NafkaError::Broker("Saga clock is unavailable".into()))?;
        let result = self
            .orchestrator
            .handle_result(
                &record.value,
                &producer,
                record.ctx.trace_context().as_ref(),
                now_ms,
                &self.state,
                &permit,
            )
            .await;
        match result {
            Ok(_) => record.ack(),
            Err(error) => match self
                .delivery_policy
                .decide(&error, record.ctx.retry_attempt)
            {
                nasaga_runtime::ResultDeliveryDisposition::Retry => Err(nafka::NafkaError::Broker(
                    "Saga result transaction is not committed".into(),
                )),
                nasaga_runtime::ResultDeliveryDisposition::Defer => Err(
                    nafka::NafkaError::HandlerDeferred("saga_result_deferred".into()),
                ),
                nasaga_runtime::ResultDeliveryDisposition::DeadLetter => Err(
                    nafka::NafkaError::HandlerDeadLetter("saga_result_deterministic_reject".into()),
                ),
            },
        }
    }
}

/// 业务作用：冻结一个 Saga HTTP API 主体的签名凭据、租户范围和最小权限。
#[cfg(feature = "web")]
#[derive(Clone)]
struct ManagedHttpApiCaller {
    authenticator: nasaga_runtime::SagaHttpMessageAuthenticator,
    actor: SagaApiActor,
}

/// 业务作用：保存 Web listener 合并前已经完成校验的 Saga HTTP 独立安全链状态。
#[cfg(feature = "web")]
struct ManagedHttpServer {
    datasource: String,
    driver: natx_core::DatabaseDriver,
    base_path: String,
    body_limit_bytes: usize,
    concurrency_limit: usize,
    request_timeout: Duration,
    command_authenticators:
        BTreeMap<nasaga_runtime::ServiceIdentity, nasaga_runtime::SagaHttpMessageAuthenticator>,
    result_authenticators:
        BTreeMap<nasaga_runtime::ServiceIdentity, nasaga_runtime::SagaHttpMessageAuthenticator>,
    api_callers: BTreeMap<nasaga_runtime::ServiceIdentity, ManagedHttpApiCaller>,
    api_enabled: bool,
    expose_admin: bool,
    expose_definition_registry: bool,
    definition_signing_keys: BTreeMap<String, String>,
    orchestrator_service_identity: Option<nasaga_runtime::ServiceIdentity>,
    activation: ManagedDefinitionActivationContract,
    page_token_key: [u8; 32],
}

#[cfg(feature = "web")]
impl ManagedHttpPublisher {
    /// 业务作用：根据 Outbox 事件类型与冻结 definition 解析唯一 HTTP 接收端，不信任 payload 自报地址。
    ///
    /// 参数说明：`event` 是 dispatcher 从持久 Outbox 按序领取的 Saga 事件。
    ///
    /// 返回：合同中存在唯一 owner route 时返回目标；载荷或路由不合法时返回确定性拒绝。
    fn target_for(
        &self,
        event: &naoutbox_core::OutboxEvent,
    ) -> Result<ManagedHttpTarget, OutboxPublishError> {
        match event.event_type.as_str() {
            nasaga_runtime::COMMAND_EVENT_TYPE => {
                let envelope: nasaga_runtime::SagaCommandEnvelope =
                    serde_json::from_slice(&event.payload)
                        .map_err(|_| OutboxPublishError::new("Saga command payload is invalid"))?;
                let routing = self
                    .routing
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let targets = routing
                    .command_targets
                    .get(&(
                        envelope.tenant_id.clone(),
                        envelope.workflow.clone(),
                        envelope.definition_version,
                        envelope.step.clone(),
                    ))
                    .or_else(|| {
                        routing.command_targets.get(&(
                            String::new(),
                            envelope.workflow,
                            envelope.definition_version,
                            envelope.step,
                        ))
                    })
                    .ok_or_else(|| {
                        OutboxPublishError::new("Saga command definition route is absent")
                    })?;
                let route_index = event.event_id.bytes().fold(0usize, |value, byte| {
                    value.wrapping_mul(31).wrapping_add(byte.into())
                }) % targets.len().max(1);
                targets
                    .get(route_index)
                    .cloned()
                    .ok_or_else(|| OutboxPublishError::new("Saga command owner route is absent"))
            }
            nasaga_runtime::RESULT_EVENT_TYPE => {
                self.result_target.as_ref().cloned().ok_or_else(|| {
                    OutboxPublishError::new("Saga result orchestrator route is absent")
                })
            }
            _ => Err(OutboxPublishError::new(
                "managed Saga HTTP received an unrelated Outbox event",
            )),
        }
    }
}

#[cfg(feature = "web")]
#[async_trait::async_trait]
impl OutboxPublisher for ManagedHttpPublisher {
    /// 业务作用：对持久事件原始字节签名并投递，只有接收方返回 `Committed` 或 `Duplicate` 收据才允许 Outbox 前移。
    ///
    /// 参数说明：`event` 携带稳定 event id、原始 JSON 和可选 traceparent。
    ///
    /// 返回：远端持久收据明确时成功；确定性拒绝返回可持久死信的封闭类别，其它结论保留原事件。
    async fn publish(&self, event: &naoutbox_core::OutboxEvent) -> Result<(), OutboxPublishError> {
        let mut selected = self.target_for(event)?;
        selected.timeout = self.request_timeout;
        let (target, remaining) = selected
            .resolve()
            .await
            .map_err(|_| OutboxPublishError::transient("Saga HTTP discovery is unavailable"))?;
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| OutboxPublishError::transient("Saga HTTP clock is unavailable"))?
            .as_millis();
        let timestamp_ms = u64::try_from(timestamp_ms)
            .map_err(|_| OutboxPublishError::transient("Saga HTTP clock is unavailable"))?;
        let nonce = nasaga_runtime::SagaHttpMessageAuthenticator::issue_nonce();
        let signature = target.authenticator.sign(
            &self.producer,
            &target.signed_path,
            timestamp_ms,
            &nonce,
            &event.payload,
        );
        let mut request = self
            .client
            .post(target.url.clone())
            .timeout(remaining)
            .header("content-type", "application/json")
            .header("x-saga-event-id", event.event_id.as_str())
            .header("x-saga-producer", self.producer.as_str())
            .header("x-saga-timestamp", timestamp_ms.to_string())
            .header("x-saga-nonce", nonce)
            .header("x-saga-signature", signature);
        if let Some(traceparent) = event.traceparent.as_deref() {
            request = request.header("traceparent", traceparent);
        }
        let response = request
            .body(event.payload.clone())
            .send()
            .await
            .map_err(|_| OutboxPublishError::transient("Saga HTTP delivery is uncertain"))?;
        let status = response.status();
        if status.is_server_error() {
            return Err(OutboxPublishError::transient(
                "Saga HTTP receiver did not return a commit receipt",
            ));
        }
        let receipt: serde_json::Value = response
            .json()
            .await
            .map_err(|_| OutboxPublishError::transient("Saga HTTP receipt body is unavailable"))?;
        match (
            status.is_success(),
            receipt.get("status").and_then(serde_json::Value::as_str),
        ) {
            (true, Some("Committed" | "Duplicate" | "committed" | "duplicate")) => Ok(()),
            (false, Some("DeterministicReject" | "deterministic_reject")) => Err(
                OutboxPublishError::new("Saga HTTP receiver deterministically rejected the event"),
            ),
            (_, Some("Retryable" | "retryable")) => Err(OutboxPublishError::transient(
                "Saga HTTP receiver requested redelivery",
            )),
            _ => Err(OutboxPublishError::transient(
                "Saga HTTP receiver returned an uncommitted receipt",
            )),
        }
    }
}

#[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
#[async_trait::async_trait]
impl OutboxPublisher for ManagedKafkaPublisher {
    /// 业务作用：把 command/result 原始 JSON 投递到冻结 topic，并只在 broker delivery report 成功后确认。
    ///
    /// 参数说明：`event` 是 dispatcher 从指定 datasource 按顺序领取的持久事实。
    ///
    /// 返回：broker ACK 成功时完成；路由或载荷非法为确定性失败，网络及 ACK 不明保留原事件。
    async fn publish(&self, event: &naoutbox_core::OutboxEvent) -> Result<(), OutboxPublishError> {
        let (topic, key) = match event.event_type.as_str() {
            nasaga_runtime::COMMAND_EVENT_TYPE => {
                let envelope: nasaga_runtime::SagaCommandEnvelope =
                    serde_json::from_slice(&event.payload).map_err(|_| {
                        OutboxPublishError::new("Saga Kafka command payload is invalid")
                    })?;
                let routing = self
                    .routing
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let topic = routing
                    .command_topics
                    .get(&(
                        envelope.tenant_id.clone(),
                        envelope.workflow.clone(),
                        envelope.definition_version,
                        envelope.step.clone(),
                    ))
                    .or_else(|| {
                        routing.command_topics.get(&(
                            String::new(),
                            envelope.workflow.clone(),
                            envelope.definition_version,
                            envelope.step.clone(),
                        ))
                    })
                    .cloned()
                    .ok_or_else(|| OutboxPublishError::new("Saga Kafka command route is absent"))?;
                (topic, envelope.saga_id)
            }
            nasaga_runtime::RESULT_EVENT_TYPE => {
                let envelope: nasaga_runtime::SagaResultEnvelope =
                    serde_json::from_slice(&event.payload).map_err(|_| {
                        OutboxPublishError::new("Saga Kafka result payload is invalid")
                    })?;
                let topic = self
                    .result_topic
                    .clone()
                    .ok_or_else(|| OutboxPublishError::new("Saga Kafka result route is absent"))?;
                (topic, envelope.saga_id)
            }
            _ => {
                return Err(OutboxPublishError::new(
                    "managed Saga Kafka received an unrelated Outbox event",
                ))
            }
        };
        let mut publish = self
            .lane
            .publish_raw(&topic, &event.payload)
            .event(&event.event_type)
            .key(key);
        if let Some(trace) = event
            .traceparent
            .as_deref()
            .and_then(nasaga_runtime::TraceContext::parse_traceparent)
        {
            publish = publish.trace_context(&trace);
        }
        publish
            .send()
            .await
            .map(|_| ())
            .map_err(|_| OutboxPublishError::transient("Saga Kafka broker receipt is uncertain"))
    }
}

#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
#[async_trait::async_trait]
impl OutboxPublisher for ManagedRedisPublisher {
    /// 业务作用：按受信 envelope 身份选择唯一 Redis stream 发布端。
    ///
    /// 参数说明：`event` 是 dispatcher 按数据源与 lane 顺序领取的持久事实。
    ///
    /// 返回：`XADD` 明确确认时成功；路由或载荷非法为确定性失败，往返不明保留原事件。
    async fn publish(&self, event: &naoutbox_core::OutboxEvent) -> Result<(), OutboxPublishError> {
        let publisher = match event.event_type.as_str() {
            nasaga_runtime::COMMAND_EVENT_TYPE => {
                let envelope: nasaga_runtime::SagaCommandEnvelope =
                    serde_json::from_slice(&event.payload).map_err(|_| {
                        OutboxPublishError::new("Saga Redis command payload is invalid")
                    })?;
                let routing = self
                    .routing
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                routing
                    .command_publishers
                    .get(&(
                        envelope.tenant_id.clone(),
                        envelope.workflow.clone(),
                        envelope.definition_version,
                        envelope.step.clone(),
                    ))
                    .or_else(|| {
                        routing.command_publishers.get(&(
                            String::new(),
                            envelope.workflow.clone(),
                            envelope.definition_version,
                            envelope.step.clone(),
                        ))
                    })
                    .cloned()
                    .ok_or_else(|| OutboxPublishError::new("Saga Redis command route is absent"))?
            }
            nasaga_runtime::RESULT_EVENT_TYPE => self
                .result_publisher
                .clone()
                .ok_or_else(|| OutboxPublishError::new("Saga Redis result route is absent"))?,
            _ => {
                return Err(OutboxPublishError::new(
                    "managed Saga Redis received an unrelated Outbox event",
                ));
            }
        };
        publisher.publish(event).await
    }
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
#[async_trait::async_trait]
impl OutboxPublisher for ManagedGrpcPublisher {
    /// 业务作用：把 command/result Outbox 交给冻结 generated client，只有明确提交或重复才前移。
    ///
    /// 参数说明：`event` 是 dispatcher 从持久 datasource 顺序领取的原始事件。
    ///
    /// 返回：Committed/Duplicate 成功，确定性拒绝交给 DLT，deadline、断连、Retryable 与非法回包均保留重投。
    async fn publish(&self, event: &naoutbox_core::OutboxEvent) -> Result<(), OutboxPublishError> {
        let (target, command) = match event.event_type.as_str() {
            nasaga_runtime::COMMAND_EVENT_TYPE => {
                let envelope: nasaga_runtime::SagaCommandEnvelope =
                    serde_json::from_slice(&event.payload).map_err(|_| {
                        OutboxPublishError::new("Saga gRPC command payload is invalid")
                    })?;
                let routing = self
                    .routing
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let target = routing
                    .command_targets
                    .get(&(
                        envelope.tenant_id.clone(),
                        envelope.workflow.clone(),
                        envelope.definition_version,
                        envelope.step.clone(),
                    ))
                    .or_else(|| {
                        routing.command_targets.get(&(
                            String::new(),
                            envelope.workflow.clone(),
                            envelope.definition_version,
                            envelope.step.clone(),
                        ))
                    })
                    .cloned()
                    .ok_or_else(|| {
                        OutboxPublishError::transient("Saga gRPC command route is absent")
                    })?;
                (target, true)
            }
            nasaga_runtime::RESULT_EVENT_TYPE => (
                self.result_target
                    .clone()
                    .ok_or_else(|| OutboxPublishError::new("Saga gRPC result route is absent"))?,
                false,
            ),
            _ => {
                return Err(OutboxPublishError::new(
                    "managed Saga gRPC received an unrelated Outbox event",
                ))
            }
        };
        let mut request = nagrpc::Request::new(nasaga_runtime::grpc_proto::SagaDeliveryRequest {
            envelope_json: event.payload.clone(),
        });
        if let Some(traceparent) = event.traceparent.as_deref() {
            let metadata = traceparent
                .parse()
                .map_err(|_| OutboxPublishError::new("Saga gRPC traceparent is invalid"))?;
            request.metadata_mut().insert("traceparent", metadata);
        }
        let response = if command {
            let mut client = nasaga_runtime::grpc_proto::saga_command_transport_client::SagaCommandTransportClient::new(
                target.channel,
            );
            client.deliver(request).await.map(|response| response.into_inner())
        } else {
            let mut client = nasaga_runtime::grpc_proto::saga_result_transport_client::SagaResultTransportClient::new(
                target.channel,
            );
            client.deliver(request).await.map(|response| response.into_inner())
        }
        .map_err(|_| {
            OutboxPublishError::transient("Saga gRPC delivery receipt is uncertain")
        })?;
        match nasaga_runtime::grpc_proto::SagaReceiptKind::try_from(response.kind) {
            Ok(nasaga_runtime::grpc_proto::SagaReceiptKind::Committed)
            | Ok(nasaga_runtime::grpc_proto::SagaReceiptKind::Duplicate) => Ok(()),
            Ok(nasaga_runtime::grpc_proto::SagaReceiptKind::DeterministicReject) => {
                let reason = if response.reason.is_empty() {
                    "saga_grpc_deterministic_reject".to_owned()
                } else {
                    response.reason
                };
                Err(OutboxPublishError::new(reason))
            }
            Ok(nasaga_runtime::grpc_proto::SagaReceiptKind::Retryable)
            | Ok(nasaga_runtime::grpc_proto::SagaReceiptKind::Unspecified)
            | Err(_) => Err(OutboxPublishError::transient(
                "Saga gRPC delivery outcome is unresolved",
            )),
        }
    }
}

/// 业务作用：保存已选 transport 的具体发布端，使 Outbox 生命周期不依赖协议分支。
enum ManagedProtocolPublisher {
    Awaiting(AwaitingCatalogPublisher),
    #[cfg(feature = "web")]
    Http(Box<ManagedHttpPublisher>),
    #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
    Kafka(ManagedKafkaPublisher),
    #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
    Redis(ManagedRedisPublisher),
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    Grpc(ManagedGrpcPublisher),
}

/// 业务作用：把 Outbox 发布端与动态 Catalog watcher 需要更新的 HTTP route 快照一并移交计划。
struct ManagedPublisherBuild {
    publisher: ManagedProtocolPublisher,
    #[cfg(feature = "web")]
    dynamic_http_routing: Option<Arc<std::sync::RwLock<ManagedHttpRoutingSnapshot>>>,
    #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
    dynamic_kafka_routing: Option<Arc<std::sync::RwLock<ManagedKafkaRoutingSnapshot>>>,
    #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
    dynamic_redis_routing: Option<Arc<std::sync::RwLock<ManagedRedisRoutingSnapshot>>>,
    #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
    redis_transport: Option<SagaRedisTransportPlan>,
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    dynamic_grpc_routing: Option<Arc<std::sync::RwLock<ManagedGrpcRoutingSnapshot>>>,
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    dynamic_grpc_credential: Option<Arc<ManagedGrpcCredentialMaterial>>,
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    dynamic_grpc_timeout: Option<Duration>,
}

#[async_trait::async_trait]
impl OutboxPublisher for ManagedProtocolPublisher {
    /// 业务作用：把单条持久事件交给 Ready 时冻结的唯一 transport 实现。
    ///
    /// 参数说明：`event` 是不得被改写身份和载荷的 Outbox 事实。
    ///
    /// 返回：具体协议收据明确时成功；其它结论保留原事件并返回封闭失败分类。
    async fn publish(&self, event: &naoutbox_core::OutboxEvent) -> Result<(), OutboxPublishError> {
        match self {
            Self::Awaiting(publisher) => publisher.publish(event).await,
            #[cfg(feature = "web")]
            Self::Http(publisher) => publisher.publish(event).await,
            #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
            Self::Kafka(publisher) => publisher.publish(event).await,
            #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
            Self::Redis(publisher) => publisher.publish(event).await,
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            Self::Grpc(publisher) => publisher.publish(event).await,
        }
    }
}

#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
macro_rules! saga_stream_metric {
    ($ident:ident, $name:literal, $help:literal, $kind:expr, $labels:expr) => {
        static $ident: nametrics_core::MetricDescriptor = nametrics_core::MetricDescriptor {
            name: $name,
            help: $help,
            unit: "",
            kind: $kind,
            label_names: $labels,
            histogram_bounds: &[],
        };
    };
}

#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
saga_stream_metric!(
    STREAM_ACKED,
    "napp_saga_stream_acked_total",
    "Redis Stream entries acknowledged after durable handling.",
    nametrics_core::MetricKind::Counter,
    &["stream", "group", "consumer"]
);
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
saga_stream_metric!(
    STREAM_DEAD_LETTERED,
    "napp_saga_stream_dead_lettered_total",
    "Redis Stream entries durably moved to the dead-letter destination.",
    nametrics_core::MetricKind::Counter,
    &["stream", "group", "consumer"]
);
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
saga_stream_metric!(
    STREAM_RETAINED,
    "napp_saga_stream_retained_total",
    "Redis Stream entries retained for a later retry.",
    nametrics_core::MetricKind::Counter,
    &["stream", "group", "consumer"]
);
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
saga_stream_metric!(
    STREAM_RECLAIMED,
    "napp_saga_stream_reclaimed_total",
    "Pending Redis Stream entries reclaimed by the managed consumer.",
    nametrics_core::MetricKind::Counter,
    &["stream", "group", "consumer"]
);
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
saga_stream_metric!(
    STREAM_DELETED_PENDING,
    "napp_saga_stream_deleted_pending_total",
    "Pending entries found deleted before acknowledgement.",
    nametrics_core::MetricKind::Counter,
    &["stream", "group", "consumer"]
);
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
saga_stream_metric!(
    STREAM_AUTH_REJECTED,
    "napp_saga_stream_auth_rejected_total",
    "Stream messages rejected by the transport authorization boundary.",
    nametrics_core::MetricKind::Counter,
    &["stream", "group", "consumer"]
);
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
saga_stream_metric!(
    STREAM_FAILED_ROUNDS,
    "napp_saga_stream_failed_rounds_total",
    "Managed Redis Stream polling rounds ending in failure.",
    nametrics_core::MetricKind::Counter,
    &["stream", "group", "consumer"]
);
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
saga_stream_metric!(
    STREAM_HANDLED,
    "napp_saga_stream_handled_total",
    "Redis Stream messages whose handler reached a classified result.",
    nametrics_core::MetricKind::Counter,
    &["stream", "group", "consumer"]
);
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
saga_stream_metric!(
    STREAM_HANDLER_MICROS,
    "napp_saga_stream_handler_micros_sum",
    "Cumulative handler duration in microseconds.",
    nametrics_core::MetricKind::Counter,
    &["stream", "group", "consumer"]
);
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
saga_stream_metric!(
    STREAM_PENDING,
    "napp_saga_stream_pending",
    "Current pending-entry count for the managed consumer.",
    nametrics_core::MetricKind::Gauge,
    &["stream", "group", "consumer"]
);
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
saga_stream_metric!(
    STREAM_OLDEST_PEL_AGE,
    "napp_saga_stream_oldest_pel_age_ms",
    "Age of the oldest pending entry in milliseconds.",
    nametrics_core::MetricKind::Gauge,
    &["stream", "group", "consumer"]
);
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
saga_stream_metric!(
    STREAM_HEALTHY,
    "napp_saga_stream_healthy",
    "Whether the latest managed polling round was healthy.",
    nametrics_core::MetricKind::Gauge,
    &["stream", "group", "consumer"]
);
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
saga_stream_metric!(
    STREAM_PUBLISHER_DUPLICATES,
    "napp_saga_stream_publisher_duplicate_hints_total",
    "Publisher responses classified as duplicate delivery hints.",
    nametrics_core::MetricKind::Counter,
    &[]
);

/// 每个 stream runtime 的带标识 family 数;快照对每个 runtime 恒产十二条,预算按 poller 数线性扩展。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
const SAGA_STREAM_SERIES_PER_RUNTIME: usize = 12;

#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
static SAGA_STREAM_DESCRIPTORS: [&nametrics_core::MetricDescriptor; 13] = [
    &STREAM_ACKED,
    &STREAM_DEAD_LETTERED,
    &STREAM_RETAINED,
    &STREAM_RECLAIMED,
    &STREAM_DELETED_PENDING,
    &STREAM_AUTH_REJECTED,
    &STREAM_FAILED_ROUNDS,
    &STREAM_HANDLED,
    &STREAM_HANDLER_MICROS,
    &STREAM_PENDING,
    &STREAM_OLDEST_PEL_AGE,
    &STREAM_HEALTHY,
    &STREAM_PUBLISHER_DUPLICATES,
];

/// 业务作用：选择 Saga 默认数据源在 Application 生命周期中的建立时机。
///
/// 常规服务使用 `Application`，由 DB 组件在 Start 阶段按统一配置建池；需要先创建隔离库的工具型
/// 进程使用 `UserHook`，由业务启动钩子注入默认池后，DB 组件在 Ready 前接管探针、监督和关闭。
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SagaDatabaseBootstrap {
    /// DB 组件依据 `database` 或 `datasources` 配置建立默认池。
    #[default]
    Application,
    /// 业务启动钩子先通过事务运行时注入默认池，DB 组件随后接管生命周期。
    UserHook,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：承载 Saga 受管运行时的数据库引导、轮询、退避、超时与摘流配置。
struct SagaSettings {
    role: Option<SagaRole>,
    plan_mode: SagaPlanMode,
    allow_combined_role: bool,
    service_identity: Option<String>,
    replica_identity: Option<String>,
    discovery: BTreeMap<String, discovery::SagaDiscoveryTargetSettings>,
    credential_overlap_ms: u64,
    orchestrator: SagaOrchestratorSettings,
    participant: SagaParticipantSettings,
    client: SagaClientSettings,
    definition_catalog: SagaDefinitionCatalogSettings,
    transport: SagaTransportSettings,
    api: SagaApiSettings,
    http: SagaHttpSettings,
    database_bootstrap: SagaDatabaseBootstrap,
    datasource_ref: Option<String>,
    timer_poll_interval_ms: u64,
    timer_error_backoff_ms: u64,
    timer_operation_timeout_ms: u64,
    timer_failure_threshold: u32,
}

/// 业务作用：为全部协议提供唯一 Orchestrator 业务 API，并把 MySQL 与 PostgreSQL 投影为相同事务合同。
#[derive(Clone)]
enum SagaOrchestratorApi {
    #[cfg(feature = "saga")]
    MySql(Arc<nasaga_runtime::Orchestrator>),
    #[cfg(feature = "saga-pgsql")]
    PostgreSql(Arc<nasaga_runtime_pgsql::PgOrchestrator>),
}

/// 业务作用：封闭标准管理路由可触发的状态变化，协议字符串不能直接选择任意运行时方法。
#[cfg(any(feature = "web", feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
#[derive(Clone, Copy)]
enum ManagedAdminAction {
    Pause,
    Resume,
    RetryCompensation,
    RetryResolution,
    ManualClose,
}

/// 业务作用：冻结一次已授权管理动作的主体、实例键、幂等身份、时间与 CAS 前置条件。
#[cfg(any(feature = "web", feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
struct ManagedAdminRequest<'a> {
    action: ManagedAdminAction,
    management: &'a nasaga_runtime::SagaManagementContext,
    tenant: &'a nasaga_runtime::__private::core::TenantId,
    saga_id: &'a nasaga_runtime::__private::core::SagaId,
    operation_id: &'a str,
    now_ms: i64,
    expectation: nasaga_runtime::SagaManagementExpectation,
}

/// 业务作用：把运行核心的类型化管理拒绝收敛为 HTTP 与 gRPC 共用的协议无关类别。
#[derive(Clone, Copy)]
enum ManagedApiFailure {
    Invalid,
    PermissionDenied,
    NotFound,
    RateLimited,
    Concurrent,
    Conflict,
    Unavailable,
}

/// 业务作用：把 Definition Registry 存储裁决收敛为 HTTP 与 gRPC 共用的封闭协议类别。
#[derive(Clone, Copy)]
enum ManagedCatalogApiFailure {
    Invalid,
    PermissionDenied,
    NotFound,
    FailedPrecondition,
    DigestConflict,
    DigestPrecondition,
    OperationConflict,
    Unavailable,
}

/// 业务作用：只依据类型化 Catalog 错误分类公开响应，数据库与事务失败统一保持可重试。
///
/// 参数说明：`error` 是 Catalog 入口返回且可能携带上下文的完整错误链。
///
/// 返回：确定性参数、权限、存在性与冲突分别返回对应类别；未知失败视为暂时不可用。
fn classify_managed_catalog_api_failure(error: &anyhow::Error) -> ManagedCatalogApiFailure {
    match nasaga_runtime::DefinitionCatalogError::from_error(error) {
        Some(nasaga_runtime::DefinitionCatalogError::InvalidArgument) => {
            ManagedCatalogApiFailure::Invalid
        }
        Some(nasaga_runtime::DefinitionCatalogError::PermissionDenied) => {
            ManagedCatalogApiFailure::PermissionDenied
        }
        Some(nasaga_runtime::DefinitionCatalogError::NotFound) => {
            ManagedCatalogApiFailure::NotFound
        }
        Some(nasaga_runtime::DefinitionCatalogError::FailedPrecondition) => {
            ManagedCatalogApiFailure::FailedPrecondition
        }
        Some(nasaga_runtime::DefinitionCatalogError::DigestConflict) => {
            ManagedCatalogApiFailure::DigestConflict
        }
        Some(nasaga_runtime::DefinitionCatalogError::PreconditionFailed) => {
            ManagedCatalogApiFailure::DigestPrecondition
        }
        Some(nasaga_runtime::DefinitionCatalogError::OperationConflict) => {
            ManagedCatalogApiFailure::OperationConflict
        }
        Some(_) | None => ManagedCatalogApiFailure::Unavailable,
    }
}

/// 业务作用：把 Registry 封闭错误类别转换为 HTTP 状态，暂时性存储失败始终保持可重试。
///
/// 参数说明：`error` 是 Catalog 写入口返回的完整错误链。
///
/// 返回：参数、权限、不存在、前置条件或冲突返回对应 4xx，未知失败返回 503。
#[cfg(feature = "web")]
fn managed_http_catalog_status(error: &anyhow::Error) -> axum::http::StatusCode {
    match classify_managed_catalog_api_failure(error) {
        ManagedCatalogApiFailure::Invalid => axum::http::StatusCode::BAD_REQUEST,
        ManagedCatalogApiFailure::PermissionDenied => axum::http::StatusCode::FORBIDDEN,
        ManagedCatalogApiFailure::NotFound => axum::http::StatusCode::NOT_FOUND,
        ManagedCatalogApiFailure::FailedPrecondition
        | ManagedCatalogApiFailure::DigestConflict
        | ManagedCatalogApiFailure::DigestPrecondition
        | ManagedCatalogApiFailure::OperationConflict => axum::http::StatusCode::CONFLICT,
        ManagedCatalogApiFailure::Unavailable => axum::http::StatusCode::SERVICE_UNAVAILABLE,
    }
}

/// 业务作用：分类管理查询或动作错误，不读取可变错误文本决定协议状态。
///
/// 参数说明：`error` 是运行核心返回且可能带事务上下文的完整错误链。
///
/// 返回：确定性上下文、权限、存在性、限额和并发拒绝分别返回封闭类别；其它失败视为暂时不可用。
fn classify_managed_api_failure(error: &anyhow::Error) -> ManagedApiFailure {
    match nasaga_runtime::SagaManagementError::from_error(error) {
        Some(nasaga_runtime::SagaManagementError::InvalidContext) => ManagedApiFailure::Invalid,
        Some(nasaga_runtime::SagaManagementError::PermissionDenied) => {
            ManagedApiFailure::PermissionDenied
        }
        Some(nasaga_runtime::SagaManagementError::NotFound) => ManagedApiFailure::NotFound,
        Some(nasaga_runtime::SagaManagementError::RateLimitExceeded) => {
            ManagedApiFailure::RateLimited
        }
        Some(
            nasaga_runtime::SagaManagementError::PreconditionFailed
            | nasaga_runtime::SagaManagementError::OperationConflict,
        ) => ManagedApiFailure::Conflict,
        _ if nasaga_runtime::SagaConcurrencyError::from_error(error).is_some() => {
            ManagedApiFailure::Concurrent
        }
        _ => ManagedApiFailure::Unavailable,
    }
}

impl SagaOrchestratorApi {
    /// 业务作用：读取运行时固定绑定的 datasource，供计划原子边界与配置引用复验。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不含 endpoint 的规范 datasource 引用。
    fn datasource_ref(&self) -> &natx_core::DatasourceRef {
        match self {
            #[cfg(feature = "saga")]
            Self::MySql(runtime) => runtime.datasource_ref(),
            #[cfg(feature = "saga-pgsql")]
            Self::PostgreSql(runtime) => runtime.datasource_ref(),
        }
    }

    /// 业务作用：返回运行时后端身份，供 catalog 类型化 getter 与 Outbox 同源门禁选择正确 driver。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：构造运行时的数据库 driver。
    fn driver(&self) -> natx_core::DatabaseDriver {
        match self {
            #[cfg(feature = "saga")]
            Self::MySql(_) => natx_core::DatabaseDriver::MySql,
            #[cfg(feature = "saga-pgsql")]
            Self::PostgreSql(_) => natx_core::DatabaseDriver::PostgreSql,
        }
    }

    /// 业务作用：在能力发布前执行 definition、descriptor 与历史实例兼容门禁。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：后端状态与当前合同兼容时成功；任何持久或合同失败拒绝 Ready。
    async fn verify_startup(&self) -> anyhow::Result<()> {
        match self {
            #[cfg(feature = "saga")]
            Self::MySql(runtime) => runtime.verify_startup().await,
            #[cfg(feature = "saga-pgsql")]
            Self::PostgreSql(runtime) => runtime.verify_startup().await,
        }
    }

    /// 业务作用：把一轮到期 timer 裁决委托给运行时构造时绑定的数据库后端。
    ///
    /// 参数说明：`owner` 是本轮 timer 租约持有者，`now_ms` 是数据库裁决使用的当前毫秒时刻。
    ///
    /// 返回：成功时返回本轮已处理 timer 数量；失权或持久化失败时返回错误且不冒充处理成功。
    async fn run_due_timers(&self, owner: &str, now_ms: i64) -> anyhow::Result<u32> {
        match self {
            #[cfg(feature = "saga")]
            Self::MySql(runtime) => runtime.run_due_timers(owner, now_ms).await,
            #[cfg(feature = "saga-pgsql")]
            Self::PostgreSql(runtime) => runtime.run_due_timers(owner, now_ms).await,
        }
    }

    /// 业务作用：在具体数据库后端中以同一领域请求创建 Saga，不让 HTTP 协议适配器复制状态机逻辑。
    ///
    /// 参数说明：`request` 是已完成身份与字段校验的创建请求，`trace` 是可选受信链路上下文，`state` 提供本次创建的冻结 Catalog 资格。
    ///
    /// 返回：透传运行核心的已创建或幂等命中结论；任何失败均保持事务原子性。
    async fn start_saga(
        &self,
        request: &nasaga_runtime::StartSagaRequest<'_>,
        trace: Option<&nasaga_runtime::TraceContext>,
        state: &SagaRuntimeState,
    ) -> anyhow::Result<nasaga_runtime::StartOutcome> {
        state.ensure_running()?;
        // 冻结原期限和撤销身份，使创建事务不能借等待期间的续租或新快照延长执行权。
        let permit = state.catalog_authority.permit().ok_or_else(|| {
            saga_error(
                ApplicationPhase::Running,
                "Saga Catalog snapshot is not authoritative on this replica",
            )
        })?;
        let authorize = || -> anyhow::Result<()> {
            state.ensure_running()?;
            // 连接池或数据库等待后可能已经失权，必须由事务层回滚全部暂写事实。
            if !permit.is_valid() {
                return Err(saga_error(
                    ApplicationPhase::Running,
                    "Saga creation authority has expired or been revoked",
                )
                .into());
            }
            Ok(())
        };
        match self {
            #[cfg(feature = "saga")]
            Self::MySql(runtime) => {
                runtime
                    .start_saga_authorized_traced(request, trace, &authorize)
                    .await
            }
            #[cfg(feature = "saga-pgsql")]
            Self::PostgreSql(runtime) => {
                runtime
                    .start_saga_authorized_traced(request, trace, &authorize)
                    .await
            }
        }
    }

    /// 业务作用：在创建前读取当前 active definition 摘要，防止调用方以不兼容合同发起实例。
    ///
    /// 参数说明：`workflow` 和 `version` 是调用方明确选择的流程版本。
    ///
    /// 返回：已激活版本返回 canonical 摘要；未知版本返回空。
    fn definition_digest(
        &self,
        tenant: &nasaga_runtime::__private::core::TenantId,
        workflow: &nasaga_runtime::__private::core::WorkflowName,
        version: nasaga_runtime::__private::core::DefinitionVersion,
    ) -> Option<String> {
        match self {
            #[cfg(feature = "saga")]
            Self::MySql(runtime) => runtime.definition_digest_for_tenant(tenant, workflow, version),
            #[cfg(feature = "saga-pgsql")]
            Self::PostgreSql(runtime) => {
                runtime.definition_digest_for_tenant(tenant, workflow, version)
            }
        }
    }

    /// 业务作用：原子发布已通过 Catalog generation 门禁的 definition 快照。
    ///
    /// 参数说明：`registry` 同时保留 active 与在途实例仍需的 deprecated 定义。
    ///
    /// 返回：无；进行中的状态机操作继续使用进入事务前取得的旧快照。
    fn replace_registry(&self, registry: DefinitionRegistry) {
        match self {
            #[cfg(feature = "saga")]
            Self::MySql(runtime) => runtime.replace_registry(registry),
            #[cfg(feature = "saga-pgsql")]
            Self::PostgreSql(runtime) => runtime.replace_registry(registry),
        }
    }

    /// 业务作用：在热切换前以持久非终态实例复验候选 registry，防止旧实例失去定义或摘要漂移。
    ///
    /// 参数说明：`registry` 是共享 Catalog watcher 装载的完整候选快照。
    ///
    /// 返回：全部在途实例仍能由同摘要定义驱动时成功；否则保持当前 generation。
    async fn verify_registry_snapshot(&self, registry: &DefinitionRegistry) -> anyhow::Result<()> {
        match self {
            #[cfg(feature = "saga")]
            Self::MySql(runtime) => runtime.verify_registry_snapshot(registry).await,
            #[cfg(feature = "saga-pgsql")]
            Self::PostgreSql(runtime) => runtime.verify_registry_snapshot(registry).await,
        }
    }

    /// 业务作用：吸收一个已认证参与方结果，并由唯一状态机决定去重、推进或补偿。
    ///
    /// 参数说明：
    /// - `envelope`: 已通过 HTTP 原始字节认证的结果。
    /// - `producer`: 凭据绑定的参与方身份。
    /// - `trace`: 可选收据链路上下文。
    /// - `now_ms`: 当前 Unix 毫秒。
    /// - `state`: 提供生命周期门禁，停机后不得继续推进。
    /// - `permit`: 入站时冻结的结果资格，等待期间续期不能延长该次操作。
    ///
    /// 返回：本地事务已提交时返回应用或重复结论；其它错误不得返回成功收据。
    async fn handle_result(
        &self,
        envelope: &nasaga_runtime::SagaResultEnvelope,
        producer: &nasaga_runtime::ServiceIdentity,
        trace: Option<&nasaga_runtime::TraceContext>,
        now_ms: i64,
        state: &SagaRuntimeState,
        permit: &catalog_authority::CatalogPermit<'_>,
    ) -> anyhow::Result<nasaga_runtime::HandleOutcome> {
        let authorize = || -> anyhow::Result<()> {
            // 同一 permit 贯穿数据库等待和提交裁决；新快照不能复活已撤销或到期的旧请求。
            if state.ensure_running().is_err() || !permit.is_valid() {
                return Err(nasaga_runtime::SagaResultProcessingError::AuthorityUnavailable.into());
            }
            Ok(())
        };
        match self {
            #[cfg(feature = "saga")]
            Self::MySql(runtime) => {
                runtime
                    .handle_authenticated_result_authorized_traced(
                        envelope, producer, trace, now_ms, &authorize,
                    )
                    .await
            }
            #[cfg(feature = "saga-pgsql")]
            Self::PostgreSql(runtime) => {
                runtime
                    .handle_authenticated_result_authorized_traced(
                        envelope, producer, trace, now_ms, &authorize,
                    )
                    .await
            }
        }
    }

    /// 业务作用：按租户读取一个已提交实例快照，避免协议层直接访问数据库。
    ///
    /// 参数说明：`tenant` 是已授权租户，`saga_id` 是实例身份。
    ///
    /// 返回：同租户实例存在时返回快照；不存在或跨租户统一返回空。
    async fn load_instance(
        &self,
        tenant: &nasaga_runtime::__private::core::TenantId,
        saga_id: &nasaga_runtime::__private::core::SagaId,
    ) -> anyhow::Result<Option<nasaga_runtime::SagaInstanceRow>> {
        match self {
            #[cfg(feature = "saga")]
            Self::MySql(runtime) => runtime.load_instance(tenant, saga_id).await,
            #[cfg(feature = "saga-pgsql")]
            Self::PostgreSql(runtime) => runtime.load_instance(tenant, saga_id).await,
        }
    }

    /// 业务作用：执行一个已认证且已授权的标准管理动作，复用运行核心的权限、审计、幂等与 CAS。
    ///
    /// 参数说明：`request` 把封闭动作、获权主体、实例键、幂等身份、时间与事务内版本绑定为单一输入。
    ///
    /// 返回：动作与同事务审计提交时成功；权限、状态、CAS 或数据库失败返回错误。
    #[cfg(any(feature = "web", feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    async fn administer(&self, request: ManagedAdminRequest<'_>) -> anyhow::Result<()> {
        let ManagedAdminRequest {
            action,
            management,
            tenant,
            saga_id,
            operation_id,
            now_ms,
            expectation,
        } = request;
        macro_rules! dispatch {
            ($runtime:expr) => {{
                match action {
                    ManagedAdminAction::Pause => {
                        $runtime
                            .pause_with_expectation(
                                management,
                                tenant,
                                saga_id,
                                operation_id,
                                expectation,
                            )
                            .await
                    }
                    ManagedAdminAction::Resume => {
                        $runtime
                            .resume_with_expectation(
                                management,
                                tenant,
                                saga_id,
                                operation_id,
                                expectation,
                            )
                            .await
                    }
                    ManagedAdminAction::RetryCompensation => $runtime
                        .retry_compensation_with_expectation(
                            management,
                            tenant,
                            saga_id,
                            operation_id,
                            now_ms,
                            expectation,
                        )
                        .await
                        .map(|_| ()),
                    ManagedAdminAction::RetryResolution => $runtime
                        .retry_resolution_with_expectation(
                            management,
                            tenant,
                            saga_id,
                            operation_id,
                            now_ms,
                            expectation,
                        )
                        .await
                        .map(|_| ()),
                    ManagedAdminAction::ManualClose => $runtime
                        .manual_close_with_expectation(
                            management,
                            tenant,
                            saga_id,
                            operation_id,
                            expectation,
                        )
                        .await
                        .map(|_| ()),
                }
            }};
        }
        match self {
            #[cfg(feature = "saga")]
            Self::MySql(runtime) => dispatch!(runtime),
            #[cfg(feature = "saga-pgsql")]
            Self::PostgreSql(runtime) => dispatch!(runtime),
        }
    }

    /// 业务作用：执行租户受限的有界实例检索，不让 HTTP 适配器接触后端连接。
    ///
    /// 参数说明：`management` 提供只读权限，`query` 含租户、状态、时间窗与 keyset 游标。
    ///
    /// 返回：按 saga_id 升序返回摘要；权限、参数或数据库失败返回错误。
    #[cfg(any(feature = "web", feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    async fn list_instances(
        &self,
        management: &nasaga_runtime::SagaManagementContext,
        query: &nasaga_runtime::SagaInstanceQuery<'_>,
    ) -> anyhow::Result<Vec<nasaga_runtime::SagaInstanceSummary>> {
        match self {
            #[cfg(feature = "saga")]
            Self::MySql(runtime) => runtime.list_instances(management, query).await,
            #[cfg(feature = "saga-pgsql")]
            Self::PostgreSql(runtime) => runtime.list_instances(management, query).await,
        }
    }

    /// 业务作用：读取不含租户、payload 与实例高基数标签的 Saga 运行指标。
    ///
    /// 参数说明：`now_ms` 用于统计已到期 timer。
    ///
    /// 返回：数据库聚合成功时返回快照；失败时指标端点不得伪造旧值为当前值。
    #[cfg(feature = "web")]
    async fn operational_metrics(
        &self,
        now_ms: i64,
    ) -> anyhow::Result<nasaga_runtime::SagaOperationalMetrics> {
        match self {
            #[cfg(feature = "saga")]
            Self::MySql(runtime) => runtime.load_operational_metrics(now_ms).await,
            #[cfg(feature = "saga-pgsql")]
            Self::PostgreSql(runtime) => runtime.load_operational_metrics(now_ms).await,
        }
    }

    /// 业务作用：读取租户受限、跨类别 keyset 有界的统一实例审计页。
    ///
    /// 参数说明：`management` 提供审计权限，租户与实例定位目标，`cursor` 与 limit 控制返回位置和规模。
    ///
    /// 返回：固定类别顺序的事实和继续游标；越权、非法游标或数据库失败返回错误。
    #[cfg(any(feature = "web", feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    async fn audit_page(
        &self,
        management: &nasaga_runtime::SagaManagementContext,
        tenant: &nasaga_runtime::__private::core::TenantId,
        saga_id: &nasaga_runtime::__private::core::SagaId,
        cursor: Option<&nasaga_runtime::SagaAuditPageCursor>,
        limit: u32,
    ) -> anyhow::Result<nasaga_runtime::SagaAuditPage> {
        match self {
            #[cfg(feature = "saga")]
            Self::MySql(runtime) => {
                runtime
                    .load_audit_page(management, tenant, saga_id, cursor, limit)
                    .await
            }
            #[cfg(feature = "saga-pgsql")]
            Self::PostgreSql(runtime) => {
                runtime
                    .load_audit_page(management, tenant, saga_id, cursor, limit)
                    .await
            }
        }
    }
}

/// 业务作用：保存命名参与方的具体数据库运行时，并为共同生命周期提供 datasource 身份。
#[derive(Clone)]
enum ManagedParticipant {
    #[cfg(feature = "saga")]
    MySql(Arc<nasaga_runtime::ParticipantRuntime>),
    #[cfg(feature = "saga-pgsql")]
    PostgreSql(Arc<nasaga_runtime_pgsql::PgParticipantRuntime>),
}

/// 业务作用：统一保存不同数据库后端的受管步骤 handler，供 transport 按冻结命令身份分派。
#[derive(Clone)]
enum ManagedParticipantCommandHandler {
    #[cfg(feature = "saga")]
    MySql(Arc<dyn nasaga_runtime::ManagedSagaCommandHandler>),
    #[cfg(feature = "saga-pgsql")]
    PostgreSql(Arc<dyn nasaga_runtime_pgsql::ManagedSagaCommandHandler>),
}

impl ManagedParticipantCommandHandler {
    /// 业务作用：把已认证命令交给构造时冻结的唯一业务步骤实现。
    ///
    /// 参数说明：
    /// - `envelope`: 已通过 transport 身份与路由门禁的命令。
    /// - `producer`: 从受信凭据映射出的协调者身份。
    /// - `receipt_trace`: 可选链路上下文。
    ///
    /// 返回：本地事务提交后返回可确认结论；其它结果返回错误并保留源事件。
    async fn handle(
        &self,
        envelope: &nasaga_runtime::SagaCommandEnvelope,
        producer: &nasaga_runtime::ServiceIdentity,
        receipt_trace: Option<&nasaga_runtime::TraceContext>,
    ) -> anyhow::Result<nasaga_runtime::ParticipantHandled> {
        match self {
            #[cfg(feature = "saga")]
            Self::MySql(handler) => handler.handle(envelope, producer, receipt_trace).await,
            #[cfg(feature = "saga-pgsql")]
            Self::PostgreSql(handler) => handler.handle(envelope, producer, receipt_trace).await,
        }
    }
}

impl ManagedParticipant {
    /// 业务作用：读取参与方事实、Inbox 与结果 Outbox 共同绑定的 datasource。
    ///
    /// 参数说明：无。
    ///
    /// 返回：构造参与方运行时时冻结的数据源引用。
    fn datasource_ref(&self) -> &natx_core::DatasourceRef {
        match self {
            #[cfg(feature = "saga")]
            Self::MySql(runtime) => runtime.datasource_ref(),
            #[cfg(feature = "saga-pgsql")]
            Self::PostgreSql(runtime) => runtime.datasource_ref(),
        }
    }

    /// 业务作用：返回参与方运行时后端身份，禁止一份计划混入不同 driver。
    ///
    /// 参数说明：无。
    ///
    /// 返回：构造参与方运行时时冻结的数据库 driver。
    fn driver(&self) -> natx_core::DatabaseDriver {
        match self {
            #[cfg(feature = "saga")]
            Self::MySql(_) => natx_core::DatabaseDriver::MySql,
            #[cfg(feature = "saga-pgsql")]
            Self::PostgreSql(_) => natx_core::DatabaseDriver::PostgreSql,
        }
    }
}

impl Default for SagaSettings {
    /// 业务作用：提供有界、偏保守的 timer 轮询与故障摘流默认值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：500ms 正常轮询、1s 故障退避、5s 单轮上限、连续三次失败后摘流的设置。
    fn default() -> Self {
        Self {
            role: None,
            plan_mode: SagaPlanMode::Managed,
            allow_combined_role: false,
            service_identity: None,
            replica_identity: None,
            discovery: BTreeMap::new(),
            credential_overlap_ms: 60_000,
            orchestrator: SagaOrchestratorSettings::default(),
            participant: SagaParticipantSettings::default(),
            client: SagaClientSettings::default(),
            definition_catalog: SagaDefinitionCatalogSettings::default(),
            transport: SagaTransportSettings::default(),
            api: SagaApiSettings::default(),
            http: SagaHttpSettings::default(),
            database_bootstrap: SagaDatabaseBootstrap::Application,
            datasource_ref: None,
            timer_poll_interval_ms: DEFAULT_TIMER_POLL_INTERVAL_MS,
            timer_error_backoff_ms: DEFAULT_TIMER_ERROR_BACKOFF_MS,
            timer_operation_timeout_ms: DEFAULT_TIMER_OPERATION_TIMEOUT_MS,
            timer_failure_threshold: 3,
        }
    }
}

impl SagaSettings {
    /// 业务作用：返回协调角色唯一允许使用的 datasource，并兼容旧 custom 计划的顶层引用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：显式角色字段优先，其次返回旧顶层字段；两者都缺失时为空。
    fn orchestrator_datasource_ref(&self) -> Option<&str> {
        self.orchestrator
            .datasource_ref
            .as_deref()
            .or(self.datasource_ref.as_deref())
    }

    /// 业务作用：返回单事务域参与角色唯一允许使用的 datasource。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：参与角色显式字段优先，其次返回旧顶层字段；多 binding 由独立门禁处理。
    fn participant_datasource_ref(&self) -> Option<&str> {
        self.participant
            .datasource_ref
            .as_deref()
            .or(self.datasource_ref.as_deref())
    }

    /// 业务作用：读取当前角色主事务域，供 Outbox 默认绑定与 custom 计划同源复验。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：单事务域角色返回规范名称候选；多 binding 或无本地事务域时为空。
    fn primary_datasource_ref(&self) -> Option<&str> {
        match self.role {
            Some(SagaRole::Orchestrator) => self.orchestrator_datasource_ref(),
            Some(SagaRole::Participant) if self.participant.bindings.is_empty() => {
                self.participant_datasource_ref()
            }
            Some(SagaRole::Combined) => self.orchestrator_datasource_ref(),
            Some(SagaRole::Client) if self.client.reliable_start => {
                self.client.datasource_ref.as_deref()
            }
            _ => self.datasource_ref.as_deref(),
        }
    }

    /// 业务作用：把嵌套协调预算投影成运行核心的封闭配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：未覆盖字段采用运行核心安全默认值。
    fn orchestrator_config(&self) -> nasaga_runtime::OrchestratorConfig {
        let mut config = nasaga_runtime::OrchestratorConfig::default();
        if let Some(value) = self.orchestrator.inbox_consumer.as_ref() {
            config.inbox_consumer.clone_from(value);
        }
        if let Some(value) = self.orchestrator.cancel_max_attempts {
            config.cancel_max_attempts = value;
        }
        if let Some(value) = self.orchestrator.compensate_max_attempts {
            config.compensate_max_attempts = value;
        }
        if let Some(value) = self.orchestrator.resolve_max_attempts {
            config.resolve_max_attempts = value;
        }
        if let Some(value) = self.orchestrator.timer_claim_limit {
            config.timer_claim_limit = value;
        }
        if let Some(value) = self.orchestrator.timer_lease_ms {
            config.timer_lease_ms = value;
        }
        if let Some(value) = self.orchestrator.pause_backoff_ms {
            config.pause_backoff_ms = value;
        }
        if let Some(value) = self.orchestrator.startup_scan_limit {
            config.startup_scan_limit = value;
        }
        if let Some(value) = self.orchestrator.enable_manual_close {
            config.enable_manual_close = value;
        }
        config
    }

    /// 业务作用：读取嵌套协调 timer 预算，并让旧 custom 配置保持相同行为。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按嵌套字段覆盖后的 timer 运行设置。
    fn timer_settings(&self) -> (u64, u64, u64, u32) {
        (
            self.orchestrator
                .timer_poll_interval_ms
                .unwrap_or(self.timer_poll_interval_ms),
            self.orchestrator
                .timer_error_backoff_ms
                .unwrap_or(self.timer_error_backoff_ms),
            self.orchestrator
                .timer_operation_timeout_ms
                .unwrap_or(self.timer_operation_timeout_ms),
            self.orchestrator
                .timer_failure_threshold
                .unwrap_or(self.timer_failure_threshold),
        )
    }
}

/// 业务作用：读取 Saga 数据库引导策略，供隐式 DB 组件选择正常建池或延后接管。
///
/// 参数说明：
/// - `application`：提供已经完成合并与校验的不可变配置快照。
/// - `phase`：读取失败应归属的生命周期阶段。
///
/// 返回：配置合法时返回显式或默认策略；结构错误返回 Saga 组件错误并阻止含混启动。
pub(crate) fn database_bootstrap(
    application: &Application,
    phase: ApplicationPhase,
) -> ApplicationResult<SagaDatabaseBootstrap> {
    Ok(read_saga_settings(application, phase)?.database_bootstrap)
}

/// 业务作用：在业务未单独声明 Outbox 配置时，把 managed Saga 的角色事务域作为内部 dispatcher 唯一数据源。
///
/// 参数说明：`application` 提供最终 Saga 快照，`phase` 是读取失败应归属的阶段。
///
/// 返回：managed 单事务域角色返回 qualifier；custom、无本地事务域或多 binding 角色返回空。
pub(crate) fn managed_outbox_datasource_ref(
    application: &Application,
    phase: ApplicationPhase,
) -> ApplicationResult<Option<String>> {
    let settings = read_saga_settings(application, phase)?;
    if settings.plan_mode != SagaPlanMode::Managed {
        return Ok(None);
    }
    Ok(settings.primary_datasource_ref().map(str::to_owned))
}

/// 业务作用：识别无需本地事务域的 managed direct client，使隐式 DB 与 Outbox 生命周期保持无副作用。
///
/// 参数说明：`application` 提供最终 Saga 配置，`phase` 标记配置错误所属生命周期阶段。
///
/// 返回：已声明配置的角色为 client、计划受管且可靠发起关闭时返回 true；未配置 Saga 或其它角色返回 false，非法配置返回错误。
pub(crate) fn is_managed_direct_client(
    application: &Application,
    phase: ApplicationPhase,
) -> ApplicationResult<bool> {
    if application.config().value().get("saga").is_none() {
        return Ok(false);
    }
    let settings = read_saga_settings(application, phase)?;
    Ok(settings.plan_mode == SagaPlanMode::Managed
        && settings.role == Some(SagaRole::Client)
        && !settings.client.reliable_start)
}

/// 业务作用：在任何计划资源移交前确认业务侧装配获得了 custom 授权，并复验角色边界。
///
/// 参数说明：
/// - `application`：提供已合并的受信 Saga 配置。
/// - `plan`：业务 UserHook 拟提交的完整运行计划。
///
/// 返回：`plan_mode=custom` 且计划角色与 `saga.role` 一致时成功；managed 双重权威或角色越界时拒绝。
pub(crate) fn validate_custom_plan_submission(
    application: &Application,
    plan: &SagaApplicationPlan,
) -> ApplicationResult<()> {
    let settings = read_saga_settings(application, ApplicationPhase::UserHook)?;
    if settings.plan_mode != SagaPlanMode::Custom {
        return Err(saga_error(
            ApplicationPhase::UserHook,
            "configure_saga is available only when saga.plan_mode=custom",
        ));
    }
    let role = settings
        .role
        .ok_or_else(|| saga_error(ApplicationPhase::UserHook, "saga.role is required"))?;
    plan.validate_role(role)
}

/// 业务作用：把受管 Orchestrator 与 durable timer 的唯一所有者身份绑定为不可拆分计划。
struct OrchestratorPlan {
    runtime: SagaOrchestratorApi,
    timer_owner: String,
}

/// 业务作用：保存动态 Catalog watcher 的数据库绑定、运行时发布目标和同代 HTTP route 更新能力。
struct ManagedCatalogWatchPlan {
    datasource: String,
    driver: natx_core::DatabaseDriver,
    interval_ms: u64,
    generation: u64,
    snapshot_digest: String,
    result_registry: DefinitionRegistry,
    service_identity: nasaga_runtime::ServiceIdentity,
    replica_identity: String,
    runtime: SagaOrchestratorApi,
    #[cfg(feature = "web")]
    http_routing: Option<Arc<std::sync::RwLock<ManagedHttpRoutingSnapshot>>>,
    #[cfg(feature = "web")]
    http_authenticator: Option<nasaga_runtime::SagaHttpMessageAuthenticator>,
    #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
    kafka_routing: Option<Arc<std::sync::RwLock<ManagedKafkaRoutingSnapshot>>>,
    #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
    kafka_command_prefix: Option<String>,
    #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
    redis_routing: Option<Arc<std::sync::RwLock<ManagedRedisRoutingSnapshot>>>,
    #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
    redis_client: Option<Arc<nadis::RedisClient>>,
    #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
    redis_command_auth: Option<nasaga_runtime::SagaStreamAuth>,
    #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
    redis_key_tag: Option<String>,
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    grpc_routing: Option<Arc<std::sync::RwLock<ManagedGrpcRoutingSnapshot>>>,
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    grpc_credential: Option<Arc<ManagedGrpcCredentialMaterial>>,
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    grpc_timeout: Option<Duration>,
    definition_publisher: Option<nasaga_runtime::ServiceIdentity>,
    definition_artifacts: Vec<nasaga_runtime::DefinitionArtifact>,
    activation_policy: String,
    activation: ManagedDefinitionActivationContract,
}

/// 业务作用：保存参与方向受信 Catalog HTTP 入口自动登记并续租的全部冻结事实。
struct ManagedCapabilityPublishPlan {
    result_source: security::CapabilityResultSource,
    client: reqwest::Client,
    target: ManagedHttpTarget,
    producer: nasaga_runtime::ServiceIdentity,
    descriptors: Vec<nasaga_runtime::CapabilityDescriptor>,
    advertised_endpoint: Option<String>,
    lease_ms: u64,
}

/// 业务作用：冻结 workflow owner 对 canonical definition 字节形成的独立 Ed25519 seal。
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManagedSignedDefinitionArtifact {
    format: String,
    canonical_document: String,
    sha256: String,
    signing_key_id: String,
    signature: String,
}

/// 业务作用：选择 definition 发布使用 HTTP HMAC 控制面还是 gRPC mTLS 控制面。
enum ManagedDefinitionPublishTransport {
    Http {
        client: reqwest::Client,
        target: Box<ManagedHttpTarget>,
    },
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    Grpc { target: ManagedGrpcTarget },
}

/// 业务作用：保存 workflow owner 自动发布全部本地 definition 所需的身份、seal 与唯一控制面。
struct ManagedDefinitionPublishPlan {
    transport: ManagedDefinitionPublishTransport,
    producer: nasaga_runtime::ServiceIdentity,
    artifacts: Vec<ManagedSignedDefinitionArtifact>,
    require_active: bool,
}

/// 业务作用：保存参与方向受信 Catalog gRPC 入口自动登记并续租的身份、路由与完整能力集合。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
struct ManagedGrpcCapabilityPublishPlan {
    result_source: security::CapabilityResultSource,
    target: ManagedGrpcTarget,
    descriptors: Vec<nasaga_runtime::CapabilityDescriptor>,
    advertised_endpoint: Option<String>,
    lease_seconds: u32,
}

type ManagedCommandHandlerMap = BTreeMap<(String, u32, String), ManagedParticipantCommandHandler>;

/// 业务作用：描述一个进程在 Saga 生命周期中托管的 Orchestrator 与参与方运行时。
///
/// 计划只能在 Application UserHook 内提交一次。Orchestrator 可选；参与方按稳定名称索引，允许一个
/// 服务同时承载多个独立参与方适配器，但不允许同名覆盖。
pub struct SagaApplicationPlan {
    orchestrator: Option<OrchestratorPlan>,
    participants: BTreeMap<String, ManagedParticipant>,
    remote_client: Option<Arc<SagaRemoteClient>>,
    command_handlers: ManagedCommandHandlerMap,
    catalog_watch: Option<ManagedCatalogWatchPlan>,
    capability_publish: Option<ManagedCapabilityPublishPlan>,
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    grpc_capability_publish: Option<ManagedGrpcCapabilityPublishPlan>,
    definition_publish: Option<ManagedDefinitionPublishPlan>,
    outboxes: Vec<crate::outbox::OutboxApplicationPlan>,
    #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
    redis_transport: Option<SagaRedisTransportPlan>,
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    grpc_services: Vec<Box<dyn nagrpc::ManagedGrpcService>>,
}

/// 业务作用：Saga 的 Redis Streams 受管消费子计划——把已构造的 result/command 消费者
/// 交给 Application 生命周期:Ready 前统一探测拓扑/group/ACL/route owner,运行期由
/// Runner 监督消费循环,停机先关领取、排空在途轮次,Redis 连接由更早启动的 Redis
/// 组件在其后释放(`DB -> Redis -> Saga -> Outbox` 的逆序)。
///
/// 发布端不在本计划内:command/result 事件仍经由受管 Outbox 的发布端合同投递,
/// 本计划只托管消费侧。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
pub struct SagaRedisTransportPlan {
    client_name: String,
    pollers: Vec<Arc<dyn SagaStreamPoller>>,
    poll_idle_ms: u64,
    error_backoff_ms: u64,
}

#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
impl SagaRedisTransportPlan {
    /// 业务作用：创建绑定某个受管 Redis 客户端、尚无消费者的传输子计划。
    ///
    /// 参数说明：
    /// - `client_name`: 受管 Redis 实例 qualifier(单实例配置固定 `default`)。
    ///
    /// 返回：必须继续加入至少一个消费者后才能提交的空子计划。
    pub fn new(client_name: impl Into<String>) -> Self {
        Self {
            client_name: client_name.into(),
            pollers: Vec::new(),
            poll_idle_ms: 10,
            error_backoff_ms: 1_000,
        }
    }

    /// 业务作用：加入一个已构造的消费者(result 或 command)。
    ///
    /// 消费身份 `(stream, group, consumer)` 在计划内必须唯一:同一身份重复轮询会把
    /// 同一份 PEL 交给两个循环,重领与确认互相踩踏。
    ///
    /// 参数说明：
    /// - `poller`: 已通过构造期配置校验的消费者。
    ///
    /// 返回：身份唯一时返回自身;重复身份返回 UserHook 配置错误。
    pub fn with_poller(mut self, poller: Arc<dyn SagaStreamPoller>) -> ApplicationResult<Self> {
        let config = poller.config();
        let identity = (
            config.stream.clone(),
            config.group.clone(),
            config.consumer.clone(),
        );
        if self.pollers.iter().any(|existing| {
            let existing = existing.config();
            (
                existing.stream.as_str(),
                existing.group.as_str(),
                existing.consumer.as_str(),
            ) == (
                identity.0.as_str(),
                identity.1.as_str(),
                identity.2.as_str(),
            )
        }) {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga redis transport pollers must have unique (stream, group, consumer)",
            ));
        }
        self.pollers.push(poller);
        Ok(self)
    }

    /// 业务作用：调整轮询间歇与故障退避(默认 10ms/1s)。
    ///
    /// 间歇只是让位调度的下限——XREADGROUP 的 BLOCK 预算才是等待新消息的主体;
    /// 退避防止 Redis 故障期忙循环。两者都必须有界。
    ///
    /// 参数说明：
    /// - `poll_idle_ms`: 相邻两轮之间的间歇毫秒(1..=60_000)。
    /// - `error_backoff_ms`: 单轮失败后的退避毫秒(1..=60_000)。
    ///
    /// 返回：预算有界时返回自身;越界返回 UserHook 配置错误。
    pub fn with_budgets(
        mut self,
        poll_idle_ms: u64,
        error_backoff_ms: u64,
    ) -> ApplicationResult<Self> {
        if !(1..=60_000).contains(&poll_idle_ms) || !(1..=60_000).contains(&error_backoff_ms) {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga redis transport budgets must be within 1ms..=60s",
            ));
        }
        self.poll_idle_ms = poll_idle_ms;
        self.error_backoff_ms = error_backoff_ms;
        Ok(self)
    }

    /// 业务作用：校验子计划完整性——空消费者集合的传输计划没有业务意义。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：客户端名与消费者集合合法返回 `Ok`。
    fn validate(&self) -> ApplicationResult<()> {
        if !crate::redis::is_canonical_redis_qualifier(&self.client_name) {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga redis transport requires a canonical Redis qualifier",
            ));
        }
        if self.pollers.is_empty() {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga redis transport requires at least one poller",
            ));
        }
        Ok(())
    }
}

/// 业务作用：单条受管 stream 消费的进程级观测状态——区分"某条流停摆"与"整个
/// 消费任务退出";标签值来自 Ready 时冻结的 (stream, group),基数有界。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
pub(crate) struct StreamRuntime {
    stream: String,
    group: String,
    consumer: String,
    acked: std::sync::atomic::AtomicU64,
    dead_lettered: std::sync::atomic::AtomicU64,
    retained: std::sync::atomic::AtomicU64,
    reclaimed: std::sync::atomic::AtomicU64,
    deleted_pending: std::sync::atomic::AtomicU64,
    auth_rejected: std::sync::atomic::AtomicU64,
    failed_rounds: std::sync::atomic::AtomicU64,
    handled: std::sync::atomic::AtomicU64,
    handler_micros_sum: std::sync::atomic::AtomicU64,
    pending: std::sync::atomic::AtomicU64,
    oldest_pel_age_ms: std::sync::atomic::AtomicU64,
    healthy: AtomicBool,
}

impl SagaApplicationPlan {
    /// 业务作用：创建尚未包含任何运行角色的 Saga 计划。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：必须继续加入 Orchestrator 或至少一个参与方后才能提交的空计划。
    pub fn new() -> Self {
        Self {
            orchestrator: None,
            participants: BTreeMap::new(),
            remote_client: None,
            command_handlers: BTreeMap::new(),
            catalog_watch: None,
            capability_publish: None,
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            grpc_capability_publish: None,
            definition_publish: None,
            outboxes: Vec::new(),
            #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
            redis_transport: None,
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            grpc_services: Vec::new(),
        }
    }

    /// 业务作用：创建只包含一个 Orchestrator 的受管计划。
    ///
    /// 参数说明：
    /// - `runtime`：已经冻结 definition 注册表和推进预算的 Orchestrator。
    /// - `timer_owner`：当前副本稳定且唯一的 timer 租约身份，重启后保持不变、不同副本不得共享。
    ///
    /// 返回：owner 满足 canonical 标识合同则返回计划，否则拒绝把含混身份用于 fencing。
    pub fn orchestrator(
        runtime: Arc<Orchestrator>,
        timer_owner: impl Into<String>,
    ) -> ApplicationResult<Self> {
        Self::new().with_orchestrator(runtime, timer_owner)
    }

    /// 业务作用：创建只包含一个命名参与方的受管计划。
    ///
    /// 参数说明：
    /// - `name`：应用内唯一、只用于能力查找和低基数诊断的 canonical 名称。
    /// - `runtime`：已冻结 Inbox consumer 与受信 command 投影的参与方运行时。
    ///
    /// 返回：名称合法时返回计划；空白、超长或非 canonical 名称返回 UserHook 错误。
    pub fn participant(
        name: impl Into<String>,
        runtime: Arc<ParticipantRuntime>,
    ) -> ApplicationResult<Self> {
        Self::new().with_participant(name, runtime)
    }

    /// 业务作用：为计划设置唯一 Orchestrator 及其逐副本 timer fencing 身份。
    ///
    /// 参数说明：
    /// - `runtime`：已经完成构造、尚未对外发布的 Orchestrator。
    /// - `timer_owner`：用于 durable timer claim 的逐副本稳定身份。
    ///
    /// 返回：首次设置且 owner 合法时返回更新后的计划；重复设置或身份非法时返回错误。
    pub fn with_orchestrator(
        mut self,
        runtime: Arc<Orchestrator>,
        timer_owner: impl Into<String>,
    ) -> ApplicationResult<Self> {
        if self.orchestrator.is_some() {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga plan declares more than one orchestrator",
            ));
        }
        let timer_owner = timer_owner.into();
        validate_runtime_name(&timer_owner, "timer owner", ApplicationPhase::UserHook)?;
        #[cfg(feature = "saga")]
        let runtime = SagaOrchestratorApi::MySql(runtime);
        #[cfg(all(not(feature = "saga"), feature = "saga-pgsql"))]
        let runtime = SagaOrchestratorApi::PostgreSql(runtime);
        self.orchestrator = Some(OrchestratorPlan {
            runtime,
            timer_owner,
        });
        Ok(self)
    }

    /// 业务作用：向计划加入一个具名参与方，避免多个 adapter 在能力入口发生身份覆盖。
    ///
    /// 参数说明：
    /// - `name`：应用内唯一的 canonical 参与方名称。
    /// - `runtime`：已经冻结信任投影的参与方运行时。
    ///
    /// 返回：名称合法且未重复时返回更新后的计划；否则返回 UserHook 错误。
    pub fn with_participant(
        mut self,
        name: impl Into<String>,
        runtime: Arc<ParticipantRuntime>,
    ) -> ApplicationResult<Self> {
        let name = name.into();
        validate_runtime_name(&name, "participant name", ApplicationPhase::UserHook)?;
        #[cfg(feature = "saga")]
        let runtime = ManagedParticipant::MySql(runtime);
        #[cfg(all(not(feature = "saga"), feature = "saga-pgsql"))]
        let runtime = ManagedParticipant::PostgreSql(runtime);
        if self.participants.insert(name, runtime).is_some() {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga plan contains a duplicate participant name",
            ));
        }
        Ok(self)
    }

    /// 业务作用：创建只包含 PostgreSQL Orchestrator 的受管计划，供双后端同时编入时显式选择后端。
    ///
    /// 参数说明：`runtime` 是 PostgreSQL 状态机入口，`timer_owner` 是逐副本 fencing 身份。
    ///
    /// 返回：身份规范时返回 PostgreSQL 计划；非法身份返回 UserHook 错误。
    #[cfg(feature = "saga-pgsql")]
    pub fn pgsql_orchestrator(
        runtime: Arc<nasaga_runtime_pgsql::PgOrchestrator>,
        timer_owner: impl Into<String>,
    ) -> ApplicationResult<Self> {
        Self::new().with_pgsql_orchestrator(runtime, timer_owner)
    }

    /// 业务作用：创建只包含一个 PostgreSQL 参与方的受管计划。
    ///
    /// 参数说明：`name` 是应用内身份，`runtime` 固定绑定 PostgreSQL datasource。
    ///
    /// 返回：名称规范时返回计划；重复或非法名称返回 UserHook 错误。
    #[cfg(feature = "saga-pgsql")]
    pub fn pgsql_participant(
        name: impl Into<String>,
        runtime: Arc<nasaga_runtime_pgsql::PgParticipantRuntime>,
    ) -> ApplicationResult<Self> {
        Self::new().with_pgsql_participant(name, runtime)
    }

    /// 业务作用：向计划设置唯一 PostgreSQL Orchestrator，并保留其 timer fencing 身份。
    ///
    /// 参数说明：`runtime` 是 PostgreSQL Orchestrator，`timer_owner` 是逐副本稳定身份。
    ///
    /// 返回：计划尚无 Orchestrator 且身份规范时成功；否则拒绝覆盖。
    #[cfg(feature = "saga-pgsql")]
    pub fn with_pgsql_orchestrator(
        mut self,
        runtime: Arc<nasaga_runtime_pgsql::PgOrchestrator>,
        timer_owner: impl Into<String>,
    ) -> ApplicationResult<Self> {
        if self.orchestrator.is_some() {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga plan declares more than one orchestrator",
            ));
        }
        let timer_owner = timer_owner.into();
        validate_runtime_name(&timer_owner, "timer owner", ApplicationPhase::UserHook)?;
        self.orchestrator = Some(OrchestratorPlan {
            runtime: SagaOrchestratorApi::PostgreSql(runtime),
            timer_owner,
        });
        Ok(self)
    }

    /// 业务作用：向计划加入一个命名 PostgreSQL 参与方，供混配构建保持显式后端选择。
    ///
    /// 参数说明：`name` 是应用内唯一名称，`runtime` 是 PostgreSQL 参与方入口。
    ///
    /// 返回：名称未占用时返回更新计划；非法或重复名称返回 UserHook 错误。
    #[cfg(feature = "saga-pgsql")]
    pub fn with_pgsql_participant(
        mut self,
        name: impl Into<String>,
        runtime: Arc<nasaga_runtime_pgsql::PgParticipantRuntime>,
    ) -> ApplicationResult<Self> {
        let name = name.into();
        validate_runtime_name(&name, "participant name", ApplicationPhase::UserHook)?;
        if self
            .participants
            .insert(name, ManagedParticipant::PostgreSql(runtime))
            .is_some()
        {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga plan contains a duplicate participant name",
            ));
        }
        Ok(self)
    }

    /// 业务作用：为 Saga 内部必需的 command/result Outbox 绑定唯一受管发布端。
    ///
    /// 业务只声明 `saga`，不再重复声明或配置独立 Outbox 组件；Application 会把本计划中的发布端
    /// 移交给隐式 Outbox 生命周期。发布端可使用 Kafka、Redis Streams、HTTP 或其它可靠 transport。
    ///
    /// 参数说明：
    /// - `publisher`：下游确认成功后才返回成功的线程安全发布端。
    ///
    /// 返回：首次绑定返回更新后的 Saga 计划；重复绑定返回 UserHook 配置错误。
    pub fn with_event_publisher<P>(mut self, publisher: Arc<P>) -> ApplicationResult<Self>
    where
        P: OutboxPublisher + Send + Sync + 'static,
    {
        if !self.outboxes.is_empty() {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga event publisher can be configured only once",
            ));
        }
        self.outboxes
            .push(crate::outbox::OutboxApplicationPlan::new(publisher));
        Ok(self)
    }

    /// 业务作用：为 Saga 内部必需的 Outbox 绑定一份**已完整配置**的发布计划——发布端之外
    /// 还要保留清理、多通道分片等能力时使用本入口。
    ///
    /// [`with_event_publisher`](Self::with_event_publisher) 只绑定发布端，是最简形式；隐式
    /// Outbox 的其余能力（`with_retention`、`with_channel_lanes` 等）都构建在
    /// [`OutboxApplicationPlan`](crate::OutboxApplicationPlan) 上，因此本入口直接接收整份
    /// 计划，避免每新增一个 Outbox 能力就要在 Saga 侧复制一个透传方法、也不会让受管 Saga
    /// 的业务够不到已有能力。两个入口互斥，只能选其一且只能调用一次。
    ///
    /// 参数说明：
    /// - `plan`：已绑定发布端并按需附加保留清理、通道分片的 Outbox 计划。
    ///
    /// 返回：首次绑定返回更新后的 Saga 计划；重复绑定返回 UserHook 配置错误。
    pub fn with_event_publisher_plan(
        mut self,
        plan: crate::outbox::OutboxApplicationPlan,
    ) -> ApplicationResult<Self> {
        if !self.outboxes.is_empty() {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga event publisher can be configured only once",
            ));
        }
        self.outboxes.push(plan);
        Ok(self)
    }

    /// 业务作用：确认计划至少托管一个 Saga 角色，且全部角色共享同一 datasource 原子边界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：含角色且单 binding 原子边界一致时成功；空计划或同一 binding 跨源时返回错误。
    pub(crate) fn validate(&self) -> ApplicationResult<()> {
        if self.orchestrator.is_none()
            && self.participants.is_empty()
            && self.remote_client.is_none()
        {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga plan must contain an orchestrator, participant or remote client",
            ));
        }
        let expected = self
            .orchestrator
            .as_ref()
            .map(|plan| plan.runtime.datasource_ref())
            .or_else(|| {
                self.participants
                    .values()
                    .next()
                    .map(|runtime| runtime.datasource_ref())
            });
        let expected_driver = self
            .orchestrator
            .as_ref()
            .map(|plan| plan.runtime.driver())
            .or_else(|| {
                self.participants
                    .values()
                    .next()
                    .map(ManagedParticipant::driver)
            });
        if self.participants.len() <= 1 {
            if let Some(expected) = expected {
                if self
                    .orchestrator
                    .as_ref()
                    .is_some_and(|plan| plan.runtime.datasource_ref() != expected)
                    || self
                        .participants
                        .values()
                        .any(|runtime| runtime.datasource_ref() != expected)
                {
                    return Err(saga_error(
                        ApplicationPhase::UserHook,
                        "saga runtimes in one managed plan must bind the same datasource",
                    ));
                }
            }
            if let Some(expected_driver) = expected_driver {
                if self
                    .orchestrator
                    .as_ref()
                    .is_some_and(|plan| plan.runtime.driver() != expected_driver)
                    || self
                        .participants
                        .values()
                        .any(|runtime| runtime.driver() != expected_driver)
                {
                    return Err(saga_error(
                        ApplicationPhase::UserHook,
                        "saga runtimes in one managed plan must bind the same database driver",
                    ));
                }
            }
        }
        Ok(())
    }

    /// 业务作用：复验 custom 计划的实际运行角色与配置授权完全一致。
    ///
    /// 参数说明：`role` 是受信配置中的封闭角色，不从计划内容反向推断。
    ///
    /// 返回：计划只持有该角色允许的权威时成功；多出或缺失角色时拒绝提交。
    fn validate_role(&self, role: SagaRole) -> ApplicationResult<()> {
        let has_orchestrator = self.orchestrator.is_some();
        let has_participant = !self.participants.is_empty();
        let has_remote_client = self.remote_client.is_some();
        let matches = match role {
            SagaRole::Orchestrator => has_orchestrator && !has_participant && !has_remote_client,
            SagaRole::Participant => !has_orchestrator && has_participant && !has_remote_client,
            SagaRole::Combined => has_orchestrator && has_participant && !has_remote_client,
            SagaRole::Client => !has_orchestrator && !has_participant && has_remote_client,
        };
        if matches {
            Ok(())
        } else {
            Err(saga_error(
                ApplicationPhase::UserHook,
                "custom Saga plan roles do not match saga.role",
            ))
        }
    }

    /// 业务作用：提交 Redis Streams 受管消费子计划——消费循环交给 Application 监督。
    ///
    /// 需要组合声明包含受管 Redis 组件(`redis` 角色);Ready 前用真实客户端统一探测
    /// 拓扑、group 幂等创建与 ACL,失败拒绝 Ready。发布端不受影响,仍走受管 Outbox。
    ///
    /// 参数说明：
    /// - `transport`: 已装配消费者的传输子计划。
    ///
    /// 返回：首次提交且子计划自洽时返回自身;重复提交或计划不完整返回 UserHook 错误。
    #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
    pub fn with_redis_stream_transport(
        mut self,
        transport: SagaRedisTransportPlan,
    ) -> ApplicationResult<Self> {
        if self.redis_transport.is_some() {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga redis transport can be configured only once",
            ));
        }
        transport.validate()?;
        self.redis_transport = Some(transport);
        Ok(self)
    }

    /// 业务作用：从计划中的唯一参与方与单一受信 producer 自动生成 Saga command gRPC service。
    ///
    /// 业务只提交 `#[saga]` Service 实例与获准 client leaf principal；Application 复用计划中的
    /// Participant runtime，自动构造 handler、generated server 并登记到唯一 registry。多参与方或
    /// 多 producer 进程不能安全推断映射，必须使用自定义 handler 入口显式路由。
    ///
    /// 参数说明：
    /// - `service`: 实现宏生成 `SagaCommandService` 的本地业务 Service。
    /// - `peer_principal`: nagrpc 从该 Orchestrator client leaf certificate 派生的 SHA-256 指纹。
    ///
    /// 返回：角色、单一 producer 与身份绑定都明确时返回自身；多角色、多 producer、重复 service 或
    /// 非法指纹返回 UserHook 错误。
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    pub fn with_grpc_command_service<S>(
        self,
        service: S,
        peer_principal: impl Into<Arc<str>>,
    ) -> ApplicationResult<Self>
    where
        S: nasaga_runtime::SagaCommandService,
    {
        if self.participants.len() != 1 {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "automatic Saga gRPC command service requires exactly one participant",
            ));
        }
        let runtime = self.participants.values().next().ok_or_else(|| {
            saga_error(
                ApplicationPhase::UserHook,
                "automatic Saga gRPC command service requires a participant",
            )
        })?;
        #[cfg(feature = "saga")]
        let runtime = match runtime {
            ManagedParticipant::MySql(runtime) => Arc::clone(runtime),
            #[cfg(feature = "saga-pgsql")]
            ManagedParticipant::PostgreSql(_) => {
                return Err(saga_error(
                    ApplicationPhase::UserHook,
                    "automatic MySQL Saga gRPC command service cannot bind a PostgreSQL participant",
                ));
            }
        };
        #[cfg(all(not(feature = "saga"), feature = "saga-pgsql"))]
        let runtime = match runtime {
            ManagedParticipant::PostgreSql(runtime) => Arc::clone(runtime),
        };
        let trusted_producer = runtime.single_trusted_command_producer().map_err(|error| {
            saga_source_error(
                ApplicationPhase::UserHook,
                "automatic Saga gRPC command producer is ambiguous",
                error,
            )
        })?;
        let handler = Arc::new(nasaga_runtime::ParticipantCommandHandler::new(
            runtime,
            Arc::new(service),
        ));
        self.with_grpc_command_transport(handler, trusted_producer, peer_principal)
    }

    /// 业务作用：从计划中的唯一 PostgreSQL 参与方生成受管 Saga command gRPC service。
    ///
    /// 参数说明：`service` 实现 PostgreSQL 宏合同，`peer_principal` 是可信 Orchestrator 证书指纹。
    ///
    /// 返回：角色与单一 producer 明确时登记 generated service；后端错配或身份含混时失败。
    #[cfg(feature = "saga-grpc-pgsql")]
    pub fn with_pgsql_grpc_command_service<S>(
        self,
        service: S,
        peer_principal: impl Into<Arc<str>>,
    ) -> ApplicationResult<Self>
    where
        S: nasaga_runtime_pgsql::PgSagaCommandService,
    {
        if self.participants.len() != 1 {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "automatic PostgreSQL Saga gRPC command service requires exactly one participant",
            ));
        }
        let runtime = match self.participants.values().next() {
            Some(ManagedParticipant::PostgreSql(runtime)) => Arc::clone(runtime),
            _ => {
                return Err(saga_error(
                    ApplicationPhase::UserHook,
                    "automatic PostgreSQL Saga gRPC command service requires a PostgreSQL participant",
                ));
            }
        };
        let trusted_producer = runtime.single_trusted_command_producer().map_err(|error| {
            saga_source_error(
                ApplicationPhase::UserHook,
                "automatic PostgreSQL Saga gRPC command producer is ambiguous",
                error,
            )
        })?;
        let handler = Arc::new(nasaga_runtime_pgsql::ParticipantCommandHandler::new(
            runtime,
            Arc::new(service),
        ));
        self.with_grpc_command_transport(handler, trusted_producer, peer_principal)
    }

    /// 业务作用：从计划中的唯一 Orchestrator 自动生成 Saga result gRPC service。
    ///
    /// Application 复用计划已经持有的 Orchestrator，不要求业务再构造 handler 或 generated server；
    /// producer 与证书 principal 的授权映射仍必须显式给出，避免多参与方身份被容器猜测。
    ///
    /// 参数说明：
    /// - `trusted_producer`: 获准向本 Orchestrator 投递结果的参与方逻辑身份。
    /// - `peer_principal`: nagrpc 从该参与方 client leaf certificate 派生的 SHA-256 指纹。
    ///
    /// 返回：计划含唯一 Orchestrator 且身份绑定合法时返回自身；缺少角色、重复 service 或非法指纹
    /// 返回 UserHook 错误。
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    pub fn with_grpc_result_service(
        self,
        trusted_producer: nasaga_runtime::ServiceIdentity,
        peer_principal: impl Into<Arc<str>>,
    ) -> ApplicationResult<Self> {
        let runtime = self
            .orchestrator
            .as_ref()
            .map(|plan| &plan.runtime)
            .ok_or_else(|| {
                saga_error(
                    ApplicationPhase::UserHook,
                    "automatic Saga gRPC result service requires an orchestrator",
                )
            })?;
        #[cfg(feature = "saga")]
        let handler = match runtime {
            SagaOrchestratorApi::MySql(runtime) => Arc::clone(runtime),
            #[cfg(feature = "saga-pgsql")]
            SagaOrchestratorApi::PostgreSql(_) => {
                return Err(saga_error(
                    ApplicationPhase::UserHook,
                    "automatic MySQL Saga gRPC result service cannot bind a PostgreSQL orchestrator",
                ));
            }
        };
        #[cfg(all(not(feature = "saga"), feature = "saga-pgsql"))]
        let handler = match runtime {
            SagaOrchestratorApi::PostgreSql(runtime) => Arc::clone(runtime),
        };
        self.with_grpc_result_transport(handler, trusted_producer, peer_principal)
    }

    /// 业务作用：从计划中的 PostgreSQL Orchestrator 生成受管 Saga result gRPC service。
    ///
    /// 参数说明：`trusted_producer` 是参与方逻辑身份，`peer_principal` 是对应证书指纹。
    ///
    /// 返回：计划后端为 PostgreSQL 且身份绑定合法时登记 service；角色缺失或错配时失败。
    #[cfg(feature = "saga-grpc-pgsql")]
    pub fn with_pgsql_grpc_result_service(
        self,
        trusted_producer: nasaga_runtime_pgsql::ServiceIdentity,
        peer_principal: impl Into<Arc<str>>,
    ) -> ApplicationResult<Self> {
        let handler = match self.orchestrator.as_ref().map(|plan| &plan.runtime) {
            Some(SagaOrchestratorApi::PostgreSql(runtime)) => Arc::clone(runtime),
            _ => {
                return Err(saga_error(
                    ApplicationPhase::UserHook,
                    "automatic PostgreSQL Saga gRPC result service requires a PostgreSQL orchestrator",
                ));
            }
        };
        self.with_grpc_result_transport(handler, trusted_producer, peer_principal)
    }

    /// 业务作用：为参与方加入自定义 Saga command handler 的框架 generated gRPC service。
    ///
    /// 常规单参与方服务应优先使用 [`Self::with_grpc_command_service`]，由 Application 从既有计划生成
    /// handler。本入口只用于一个进程内多参与方路由或其它自定义提交边界。
    ///
    /// 参数说明：
    /// - `handler`: 本地事务提交成功后才返回可确认结果的 command handler。
    /// - `trusted_producer`: 唯一获准向该参与方投递命令的 Orchestrator 逻辑身份。
    /// - `peer_principal`: nagrpc 从该 Orchestrator client leaf certificate 派生的 SHA-256 指纹。
    ///
    /// 返回：身份绑定合法且 command service 尚未加入时返回自身；重复或非法指纹返回 UserHook 错误。
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    pub fn with_grpc_command_transport<H>(
        mut self,
        handler: Arc<H>,
        trusted_producer: nasaga_runtime::ServiceIdentity,
        peer_principal: impl Into<Arc<str>>,
    ) -> ApplicationResult<Self>
    where
        H: nasaga_runtime::SagaCommandHandler,
    {
        if self
            .grpc_services
            .iter()
            .any(|service| service.service_name() == "nasa.saga.transport.v1.SagaCommandTransport")
        {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga gRPC command transport can be configured only once",
            ));
        }
        let peer = nasaga_runtime::SagaGrpcPeerBinding::new(peer_principal, trusted_producer)
            .map_err(|error| {
                saga_source_error(
                    ApplicationPhase::UserHook,
                    "saga gRPC command peer binding is invalid",
                    error,
                )
            })?;
        let service = nasaga_runtime::SagaGrpcCommandTransportService::new(handler, peer);
        self.grpc_services.push(Box::new(
            nasaga_runtime::grpc_proto::saga_command_transport_server::SagaCommandTransportServer::new(
                service,
            ),
        ));
        Ok(self)
    }

    /// 业务作用：为自定义 result handler 加入框架 generated Saga result gRPC service。
    ///
    /// 常规 Orchestrator 应优先使用 [`Self::with_grpc_result_service`]，复用计划中已提交的 runtime。
    /// 本入口只用于替代本地结果提交边界的高级场景。
    ///
    /// 参数说明：
    /// - `handler`: 在本地事务中吸收并推进 result 的 Orchestrator handler。
    /// - `trusted_producer`: 唯一获准向本 Orchestrator 投递结果的参与方逻辑身份。
    /// - `peer_principal`: nagrpc 从该参与方 client leaf certificate 派生的 SHA-256 指纹。
    ///
    /// 返回：身份绑定合法且 result service 尚未加入时返回自身；重复或非法指纹返回 UserHook 错误。
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    pub fn with_grpc_result_transport<H>(
        mut self,
        handler: Arc<H>,
        trusted_producer: nasaga_runtime::ServiceIdentity,
        peer_principal: impl Into<Arc<str>>,
    ) -> ApplicationResult<Self>
    where
        H: nasaga_runtime::SagaResultHandler,
    {
        if self
            .grpc_services
            .iter()
            .any(|service| service.service_name() == "nasa.saga.transport.v1.SagaResultTransport")
        {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga gRPC result transport can be configured only once",
            ));
        }
        let peer = nasaga_runtime::SagaGrpcPeerBinding::new(peer_principal, trusted_producer)
            .map_err(|error| {
                saga_source_error(
                    ApplicationPhase::UserHook,
                    "saga gRPC result peer binding is invalid",
                    error,
                )
            })?;
        let service = nasaga_runtime::SagaGrpcResultTransportService::new(handler, peer);
        self.grpc_services.push(Box::new(
            nasaga_runtime::grpc_proto::saga_result_transport_server::SagaResultTransportServer::new(
                service,
            ),
        ));
        Ok(self)
    }

    /// 业务作用：把已冻结的 Saga gRPC generated service 线性移交给 Application 唯一 registry。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：command/result service 的唯一所有权；未启用入站 gRPC transport 时为空集合。
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    pub(crate) fn take_grpc_services(&mut self) -> Vec<Box<dyn nagrpc::ManagedGrpcService>> {
        std::mem::take(&mut self.grpc_services)
    }

    /// 业务作用：把 Saga 组合声明内的发布计划移交给隐式 Outbox 组件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已绑定发布端时返回唯一计划；缺失时返回 UserHook 配置错误。
    pub(crate) fn take_outbox_plans(
        &mut self,
    ) -> ApplicationResult<Vec<crate::outbox::OutboxApplicationPlan>> {
        if self.outboxes.is_empty() {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga requires an event publisher for its managed Outbox",
            ));
        }
        Ok(std::mem::take(&mut self.outboxes))
    }
}

impl Default for SagaApplicationPlan {
    /// 业务作用：提供便于按角色逐步装配的空 Saga 计划。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与 [`SagaApplicationPlan::new`] 相同的空计划。
    fn default() -> Self {
        Self::new()
    }
}

/// 业务作用：提供受管 Saga 的业务发起、查询与授权暂停能力，运行定义和调度控制仍由 Application 独占。
#[derive(Clone)]
pub struct SagaOrchestratorHandle {
    state: Arc<SagaRuntimeState>,
    runtime: SagaOrchestratorApi,
}

impl SagaOrchestratorHandle {
    /// 业务作用：读取句柄冻结的数据源身份，供业务核对本地事务绑定。
    /// 参数说明: 无。
    /// 返回：不含连接凭据的规范数据源引用，不授予连接或调度控制权。
    pub fn datasource_ref(&self) -> &natx_core::DatasourceRef {
        self.runtime.datasource_ref()
    }

    /// 业务作用：在当前受管 Catalog 与生命周期约束下发起 Saga，业务不能自行替换运行定义。
    /// 参数说明：`request` 是业务已校验的创建请求。
    /// 返回：创建或幂等命中返回持久收据；失权、停机或数据库失败保留事务错误语义。
    pub async fn start_saga(
        &self,
        request: &nasaga_runtime::StartSagaRequest<'_>,
    ) -> anyhow::Result<nasaga_runtime::StartOutcome> {
        self.start_saga_traced(request, None).await
    }

    /// 业务作用：携带受信链路上下文执行受管创建，缓存句柄不能绕过后续失权门禁。
    /// 参数说明：`request` 是创建请求，`trace` 是入口已验证的可选链路上下文。
    /// 返回：事务确认的创建或幂等收据；错误不会被包装成已提交。
    pub async fn start_saga_traced(
        &self,
        request: &nasaga_runtime::StartSagaRequest<'_>,
        trace: Option<&nasaga_runtime::TraceContext>,
    ) -> anyhow::Result<nasaga_runtime::StartOutcome> {
        // 每次调用重新取得执行资格，不能把获取句柄时的 Ready 状态当作长期授权。
        self.runtime.start_saga(request, trace, &self.state).await
    }

    /// 业务作用：读取指定租户的已提交实例，业务查询不取得底层 Orchestrator 的控制方法。
    /// 参数说明：`tenant` 是业务授权范围，`saga_id` 是目标实例身份。
    /// 返回：当前组件可用时返回同租户快照或空；失权、停机或数据库失败返回错误。
    pub async fn load_instance(
        &self,
        tenant: &nasaga_runtime::__private::core::TenantId,
        saga_id: &nasaga_runtime::__private::core::SagaId,
    ) -> anyhow::Result<Option<nasaga_runtime::SagaInstanceRow>> {
        self.state.ensure_ready()?;
        self.runtime.load_instance(tenant, saga_id).await
    }
    /// 业务作用：通过已授权管理上下文暂停实例，不授予 timer owner 或 Catalog 替换权限。
    /// 参数说明：`management` 包含主体、原因和权限，`tenant`/`saga_id` 定位实例，`operation_id` 保持管理重试幂等。
    /// 返回：暂停及审计事务提交后成功；组件失权、权限不足、并发冲突或数据库失败返回错误。
    pub async fn pause(
        &self,
        management: &nasaga_runtime::SagaManagementContext,
        tenant: &nasaga_runtime::__private::core::TenantId,
        saga_id: &nasaga_runtime::__private::core::SagaId,
        operation_id: &str,
    ) -> anyhow::Result<()> {
        self.state.ensure_ready()?;
        match &self.runtime {
            #[cfg(feature = "saga")]
            SagaOrchestratorApi::MySql(runtime) => {
                runtime
                    .pause(management, tenant, saga_id, operation_id)
                    .await
            }
            #[cfg(feature = "saga-pgsql")]
            SagaOrchestratorApi::PostgreSql(runtime) => {
                runtime
                    .pause(management, tenant, saga_id, operation_id)
                    .await
            }
        }
    }

    /// 业务作用：读取受管数据库的持久运行指标，供业务运维出口观测积压与状态。
    /// 参数说明：`now_ms` 是用于判定到期 timer 的当前 epoch 毫秒。
    /// 返回：组件可用且聚合成功时返回快照；失权、停机或查询失败返回错误。
    pub async fn load_operational_metrics(
        &self,
        now_ms: i64,
    ) -> anyhow::Result<nasaga_runtime::SagaOperationalMetrics> {
        self.state.ensure_ready()?;
        match &self.runtime {
            #[cfg(feature = "saga")]
            SagaOrchestratorApi::MySql(runtime) => runtime.load_operational_metrics(now_ms).await,
            #[cfg(feature = "saga-pgsql")]
            SagaOrchestratorApi::PostgreSql(runtime) => {
                runtime.load_operational_metrics(now_ms).await
            }
        }
    }
}

/// 业务作用：提供 Ready 后的 Saga 业务能力，不暴露停机、替换 definition 或重置 fencing 权限。
#[derive(Clone)]
pub struct SagaHandle {
    pub(crate) state: Arc<SagaRuntimeState>,
}

impl SagaHandle {
    /// 业务作用：取得 Ready 后受管的远程 Saga client，供 client 角色发起和查询独立 Orchestrator。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前计划发布远程 client 且组件仍 Ready 时返回共享句柄；其它角色或停机后返回错误。
    pub fn remote_client(&self) -> ApplicationResult<Arc<SagaRemoteClient>> {
        self.state.ensure_ready()?;
        self.state.remote_client.get().cloned().ok_or_else(|| {
            saga_error(
                ApplicationPhase::Running,
                "this application does not host a managed remote Saga client",
            )
        })
    }

    /// 业务作用：取得已经通过 Ready 门禁的 Orchestrator，用于业务入口推进 Saga。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前计划含 Orchestrator 且组件仍 Ready 时返回共享句柄；角色缺失或停机后返回错误。
    pub fn orchestrator(&self) -> ApplicationResult<SagaOrchestratorHandle> {
        self.state.ensure_ready()?;
        match self.state.orchestrator.get() {
            #[cfg(feature = "saga")]
            Some(runtime @ SagaOrchestratorApi::MySql(_)) => Ok(SagaOrchestratorHandle {
                state: Arc::clone(&self.state),
                runtime: runtime.clone(),
            }),
            #[cfg(all(not(feature = "saga"), feature = "saga-pgsql"))]
            Some(runtime @ SagaOrchestratorApi::PostgreSql(_)) => Ok(SagaOrchestratorHandle {
                state: Arc::clone(&self.state),
                runtime: runtime.clone(),
            }),
            _ => Err(saga_error(
                ApplicationPhase::Running,
                "this application does not host an orchestrator for the requested backend",
            )),
        }
    }

    /// 业务作用：按稳定名称取得已经通过 Ready 门禁的参与方运行时。
    ///
    /// 参数说明：
    /// - `name`：提交计划时使用的参与方名称。
    ///
    /// 返回：名称存在且组件仍 Ready 时返回共享句柄；未知名称或停机后返回错误。
    pub fn participant(&self, name: &str) -> ApplicationResult<Arc<ParticipantRuntime>> {
        self.state.ensure_ready()?;
        let runtime = self
            .state
            .participants
            .get()
            .and_then(|participants| participants.get(name))
            .ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Running,
                    "requested saga participant is not hosted by this application",
                )
            })?;
        match runtime {
            #[cfg(feature = "saga")]
            ManagedParticipant::MySql(runtime) => Ok(Arc::clone(runtime)),
            #[cfg(all(not(feature = "saga"), feature = "saga-pgsql"))]
            ManagedParticipant::PostgreSql(runtime) => Ok(Arc::clone(runtime)),
            #[cfg(all(feature = "saga", feature = "saga-pgsql"))]
            _ => Err(saga_error(
                ApplicationPhase::Running,
                "requested saga participant uses a different database backend",
            )),
        }
    }

    /// 业务作用：取得 Ready 后发布的 PostgreSQL Orchestrator，不把 MySQL 角色误转成同名能力。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：计划托管 PostgreSQL Orchestrator 时返回共享入口；角色缺失或后端不同则失败。
    #[cfg(feature = "saga-pgsql")]
    pub fn pgsql_orchestrator(&self) -> ApplicationResult<SagaOrchestratorHandle> {
        self.state.ensure_ready()?;
        match self.state.orchestrator.get() {
            Some(runtime @ SagaOrchestratorApi::PostgreSql(_)) => Ok(SagaOrchestratorHandle {
                state: Arc::clone(&self.state),
                runtime: runtime.clone(),
            }),
            _ => Err(saga_error(
                ApplicationPhase::Running,
                "this application does not host a PostgreSQL saga orchestrator",
            )),
        }
    }

    /// 业务作用：按稳定名称取得 Ready 后发布的 PostgreSQL 参与方运行时。
    ///
    /// 参数说明：`name` 是计划提交时使用的参与方名称。
    ///
    /// 返回：名称存在且后端为 PostgreSQL 时返回共享入口；否则失败。
    #[cfg(feature = "saga-pgsql")]
    pub fn pgsql_participant(
        &self,
        name: &str,
    ) -> ApplicationResult<Arc<nasaga_runtime_pgsql::PgParticipantRuntime>> {
        self.state.ensure_ready()?;
        match self
            .state
            .participants
            .get()
            .and_then(|participants| participants.get(name))
        {
            Some(ManagedParticipant::PostgreSql(runtime)) => Ok(Arc::clone(runtime)),
            _ => Err(saga_error(
                ApplicationPhase::Running,
                "requested PostgreSQL saga participant is not hosted by this application",
            )),
        }
    }
}

/// 业务作用：保存 Saga 计划、生命周期门禁和 Ready 后发布的只读运行时能力。
pub(crate) struct SagaRuntimeState {
    pending: Mutex<Option<SagaApplicationPlan>>,
    sealed: AtomicBool,
    lifecycle: AtomicU8,
    catalog_authority: catalog_authority::CatalogAuthority,
    result_authority: catalog_authority::CatalogAuthority,
    security: OnceLock<Arc<security::SagaSecurityState>>,
    orchestrator: OnceLock<SagaOrchestratorApi>,
    participants: OnceLock<Arc<BTreeMap<String, ManagedParticipant>>>,
    remote_client: OnceLock<Arc<SagaRemoteClient>>,
    command_handlers: OnceLock<Arc<ManagedCommandHandlerMap>>,
    #[cfg(feature = "web")]
    http_server: OnceLock<Arc<ManagedHttpServer>>,
    #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
    streams: OnceLock<Arc<Vec<Arc<StreamRuntime>>>>,
}

impl SagaRuntimeState {
    /// 业务作用：创建开放 UserHook 计划入口、尚未发布任何 Saga 权限的状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：生命周期处于 configuring 的新状态。
    pub(crate) fn new() -> Self {
        Self {
            pending: Mutex::new(None),
            sealed: AtomicBool::new(false),
            lifecycle: AtomicU8::new(0),
            catalog_authority: catalog_authority::CatalogAuthority::new(),
            result_authority: catalog_authority::CatalogAuthority::new(),
            security: OnceLock::new(),
            orchestrator: OnceLock::new(),
            participants: OnceLock::new(),
            remote_client: OnceLock::new(),
            command_handlers: OnceLock::new(),
            #[cfg(feature = "web")]
            http_server: OnceLock::new(),
            #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
            streams: OnceLock::new(),
        }
    }

    /// 业务作用：线性化接收唯一 Saga 计划，禁止晚到或重复装配静默覆盖运行角色。
    ///
    /// 参数说明：
    /// - `plan`：已经通过角色与名称校验的完整计划。
    ///
    /// 返回：首次提交成功；Ready 已封口或已有计划时返回 UserHook 错误。
    pub(crate) fn configure(&self, plan: SagaApplicationPlan) -> ApplicationResult<()> {
        plan.validate()?;
        if self.sealed.load(Ordering::Acquire) {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga configuration is sealed before Ready",
            ));
        }
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.sealed.load(Ordering::Acquire) || pending.is_some() {
            return Err(saga_error(
                ApplicationPhase::UserHook,
                "saga plan can be configured only once",
            ));
        }
        *pending = Some(plan);
        Ok(())
    }

    /// 业务作用：在 Ready 入口永久封口计划并移交唯一所有权，后续调用不能改变运行拓扑。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：UserHook 已提交计划时返回该计划；缺失时返回 Ready 错误。
    fn take_configured_plan(&self) -> Option<SagaApplicationPlan> {
        self.sealed.store(true, Ordering::Release);
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    /// 业务作用：只在全部启动门禁通过后一次性发布运行角色与 Ready 权限。
    ///
    /// 参数说明：
    /// - `plan`：已完成 descriptor 与历史实例校验的封口计划。
    ///
    /// 返回：首次发布成功；内部重复发布返回 Ready 错误。
    fn publish(
        &self,
        mut plan: SagaApplicationPlan,
    ) -> ApplicationResult<Option<OrchestratorPlan>> {
        let orchestrator = plan.orchestrator.take();
        if let Some(client) = plan.remote_client.take() {
            self.remote_client.set(client).map_err(|_| {
                saga_error(
                    ApplicationPhase::Ready,
                    "Saga remote client was already published",
                )
            })?;
        }
        if let Some(runtime) = orchestrator.as_ref().map(|entry| entry.runtime.clone()) {
            self.orchestrator.set(runtime).map_err(|_| {
                saga_error(
                    ApplicationPhase::Ready,
                    "saga orchestrator was already published",
                )
            })?;
        }
        self.participants
            .set(Arc::new(plan.participants))
            .map_err(|_| {
                saga_error(
                    ApplicationPhase::Ready,
                    "saga participants were already published",
                )
            })?;
        self.command_handlers
            .set(Arc::new(plan.command_handlers))
            .map_err(|_| {
                saga_error(
                    ApplicationPhase::Ready,
                    "saga command handlers were already published",
                )
            })?;
        self.lifecycle.store(1, Ordering::Release);
        Ok(orchestrator)
    }

    /// 业务作用：在 Saga 角色能力发布后登记唯一 HTTP 安全链快照，供随后启动的 Web 组件合并路由。
    ///
    /// 参数说明：`server` 已冻结路径、凭据、角色、权限和共享 replay 数据源。
    ///
    /// 返回：首次发布成功；重复发布返回 Ready 错误。
    #[cfg(feature = "web")]
    fn publish_http_server(&self, server: Arc<ManagedHttpServer>) -> ApplicationResult<()> {
        self.http_server.set(server).map_err(|_| {
            saga_error(
                ApplicationPhase::Ready,
                "managed Saga HTTP server was already published",
            )
        })
    }

    /// 业务作用：读取已经通过 Saga Ready 门禁的 HTTP 服务端安全快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前角色启用 HTTP 入站时返回快照，否则为空。
    #[cfg(feature = "web")]
    fn http_server(&self) -> Option<Arc<ManagedHttpServer>> {
        self.http_server.get().cloned()
    }

    /// 业务作用：按冻结 workflow、definition version 与 step 读取唯一参与方 handler。
    ///
    /// 参数说明：`envelope` 是已经完成 transport 认证、尚未进入本地事务的命令。
    ///
    /// 返回：本地二进制明确托管该步骤时返回 handler；未知路由返回空且不得猜测替代步骤。
    fn command_handler(
        &self,
        envelope: &nasaga_runtime::SagaCommandEnvelope,
    ) -> Option<ManagedParticipantCommandHandler> {
        self.command_handlers.get().and_then(|handlers| {
            handlers
                .get(&(
                    envelope.workflow.clone(),
                    envelope.definition_version,
                    envelope.step.clone(),
                ))
                .cloned()
        })
    }

    /// 业务作用：读取当前 Ready 计划的具体 Orchestrator，供受管协议适配器复用唯一推进权威。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：协调角色返回共享 runtime；participant/client 角色返回空。
    fn managed_orchestrator(&self) -> Option<SagaOrchestratorApi> {
        self.orchestrator.get().cloned()
    }

    /// 业务作用：确认组件已发布且未停止，供不依赖完整业务路由的 capability 恢复入口使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：运行期间允许继续认证和持久登记；启动未完成或停机时拒绝，不授予业务快照执行权。
    fn ensure_running(&self) -> ApplicationResult<()> {
        match self.lifecycle.load(Ordering::Acquire) {
            1 => Ok(()),
            2 => Err(saga_error(
                ApplicationPhase::Stopping,
                "saga runtime is stopping and rejects new work",
            )),
            _ => Err(saga_error(
                ApplicationPhase::Ready,
                "saga runtime has not passed its Ready gate",
            )),
        }
    }

    /// 业务作用：复验生命周期与 Catalog 确认期限，防止失去快照资格或停机后继续接收新推进请求。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已发布且快照资格仍有效时成功；未确认、租约过期或停机时返回对应阶段错误。
    pub(crate) fn ensure_ready(&self) -> ApplicationResult<()> {
        self.ensure_running()?;
        if self.catalog_authority.is_authoritative() {
            Ok(())
        } else {
            Err(saga_error(
                ApplicationPhase::Running,
                "Saga Catalog snapshot is not authoritative on this replica",
            ))
        }
    }

    /// 业务作用：为单次结果请求冻结定义与身份信任资格，避免等待期间借续期或重新确认恢复旧操作。
    /// 参数说明：无。
    /// 返回：运行中且结果合同有效时返回原期限与撤销代际；其它情况返回须保留原事件的延后分类。
    fn result_permit(
        &self,
    ) -> Result<catalog_authority::CatalogPermit<'_>, nasaga_runtime::SagaResultProcessingError>
    {
        self.ensure_running()
            .map_err(|_| nasaga_runtime::SagaResultProcessingError::AuthorityUnavailable)?;
        // 结果资格独立于 command 路由，仍须由实际状态事务持续复验，不能只保留入口布尔值。
        self.result_authority
            .permit()
            .ok_or(nasaga_runtime::SagaResultProcessingError::AuthorityUnavailable)
    }

    /// 业务作用：在共享 Catalog 变化或确认失败时关闭本副本的新请求与推进资格。
    /// 参数说明：无。
    /// 返回：资格立即关闭，直到完整快照获得有效确认。
    fn revoke_catalog_authority(&self) {
        self.catalog_authority.revoke();
    }

    /// 业务作用：进入停机保护态并永久关闭新的 Saga 能力访问。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；Release 发布保证后续能力读取观察到停机态。
    fn stop(&self) {
        self.lifecycle.store(2, Ordering::Release);
        self.result_authority.revoke();
    }
}

/// 业务作用：在 Application 生命周期内建立、监督并逆序关闭 Saga 运行时。
pub(crate) struct SagaComponent {
    settings: Option<SagaSettings>,
    contributor: Option<ReadinessContributor>,
    catalog_contributor: Option<ReadinessContributor>,
    capability_contributor: Option<ReadinessContributor>,
    definition_publish_contributor: Option<ReadinessContributor>,
    #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
    stream_contributor: Option<ReadinessContributor>,
    critical_task: Option<ApplicationFuture<'static>>,
}

impl SagaComponent {
    /// 业务作用：创建尚未读取配置、未接收计划的 Saga 生命周期组件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：由 Runner 独占并依次推进 Start、Ready 与停机阶段的组件。
    pub(crate) fn new() -> Self {
        Self {
            settings: None,
            contributor: None,
            catalog_contributor: None,
            capability_contributor: None,
            definition_publish_contributor: None,
            #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
            stream_contributor: None,
            critical_task: None,
        }
    }

    /// 业务作用：按受信角色选择互斥的 managed 构造路径，不先取得 Orchestrator 权威再裁剪。
    ///
    /// 参数说明：
    /// - `application`：提供冻结 datasource catalog 与已建立的类型化连接池。
    /// - `settings`：Start 阶段已完成封闭校验的 Saga 配置。
    ///
    /// 返回：精确角色的 schema、definition 与运行时全部构建成功时返回内部计划；任一门禁失败则不发布能力。
    async fn build_managed_plan(
        application: &Application,
        settings: &SagaSettings,
    ) -> ApplicationResult<SagaApplicationPlan> {
        validate_managed_component_capabilities(application, settings)?;
        match settings
            .role
            .ok_or_else(|| saga_error(ApplicationPhase::Ready, "saga.role is required"))?
        {
            SagaRole::Orchestrator => build_managed_orchestrator_plan(application, settings).await,
            SagaRole::Participant => build_managed_participant_plan(application, settings).await,
            SagaRole::Client => build_managed_client_plan(application, settings).await,
            SagaRole::Combined => build_managed_combined_plan(application, settings).await,
        }
    }
}

/// 业务作用：为纯 client 角色建立只含远程 start/query 权限的受管计划，不创建本地状态机或参与方表。
///
/// 参数说明：`application` 提供受信 secret 快照，`settings` 提供调用身份、Orchestrator 地址和可靠发起等级。
///
/// 返回：所选 HTTP/gRPC 地址、身份和凭据完整时返回远程 client 计划；可靠发起的写入与投递共享指定事务域，缺少 start-intent 合同时拒绝 Ready。
async fn build_managed_client_plan(
    application: &Application,
    settings: &SagaSettings,
) -> ApplicationResult<SagaApplicationPlan> {
    let producer = nasaga_runtime::ServiceIdentity::new(
        settings.client.service_identity.as_deref().ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "managed Saga client service identity is missing",
            )
        })?,
    )
    .map_err(|error| {
        saga_source_error(
            ApplicationPhase::Ready,
            "managed Saga client service identity is invalid",
            error,
        )
    })?;
    let credential = settings.client.credential_ref.as_deref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "managed Saga client credential reference is missing",
        )
    })?;
    let route = settings
        .client
        .orchestrator_discovery_ref
        .as_deref()
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "managed Saga client Orchestrator route is missing",
            )
        })?;
    let transport = match settings.client.protocol.unwrap_or(SagaClientProtocol::Http) {
        SagaClientProtocol::Http => {
            let authenticator = load_managed_http_authenticator(application, credential)?;
            let instances_target = managed_http_discovery_target(
                application,
                settings,
                route,
                "instances",
                authenticator,
                Duration::from_secs(5),
            )?;
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|error| {
                    saga_source_error(
                        ApplicationPhase::Ready,
                        "managed Saga remote HTTP client construction failed",
                        error,
                    )
                })?;
            SagaRemoteClientTransport::Http(Arc::new(SagaRemoteHttpClient {
                client,
                producer: producer.clone(),
                instances_target,
                response_limit_bytes: 1024 * 1024,
            }))
        }
        SagaClientProtocol::Grpc => {
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            {
                let material = security::grpc_credential(application, credential)?;
                let target = managed_grpc_discovery_target(
                    application,
                    settings,
                    route,
                    Duration::from_secs(5),
                    &material,
                )?;
                probe_managed_grpc_target(&target, ApplicationPhase::Ready).await?;
                SagaRemoteClientTransport::Grpc(target)
            }
            #[cfg(not(any(feature = "saga-grpc", feature = "saga-grpc-pgsql")))]
            return Err(saga_error(
                ApplicationPhase::Ready,
                "managed Saga gRPC client capability is not compiled",
            ));
        }
    };
    let mut start_intent: Option<Arc<dyn naoutbox_core::DurableOutboxAppend>> = None;
    let mut plan = SagaApplicationPlan::new();
    if settings.client.reliable_start {
        let datasource = settings.client.datasource_ref.as_deref().ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "reliable Saga client datasource reference is missing",
            )
        })?;
        let driver = managed_datasource_driver(application, datasource).await?;
        let append: Arc<dyn naoutbox_core::DurableOutboxAppend> = match driver {
            natx_core::DatabaseDriver::MySql => {
                #[cfg(feature = "saga")]
                {
                    naoutbox_mysql::MySqlOutbox::ensure_schema_for(datasource)
                        .await
                        .map_err(|error| {
                            saga_source_error(
                                ApplicationPhase::Ready,
                                "reliable Saga client Outbox schema is unavailable",
                                error,
                            )
                        })?;
                    Arc::new(
                        naoutbox_mysql::MySqlOutbox::with_datasource(datasource).map_err(
                            |error| {
                                saga_source_error(
                                    ApplicationPhase::Ready,
                                    "reliable Saga client Outbox binding is invalid",
                                    error,
                                )
                            },
                        )?,
                    )
                }
                #[cfg(not(feature = "saga"))]
                return Err(saga_error(
                    ApplicationPhase::Ready,
                    "reliable Saga client requires the MySQL Outbox capability",
                ));
            }
            natx_core::DatabaseDriver::PostgreSql => {
                #[cfg(feature = "saga-pgsql")]
                {
                    naoutbox_pgsql::PgOutbox::ensure_schema_for(datasource)
                        .await
                        .map_err(|error| {
                            saga_source_error(
                                ApplicationPhase::Ready,
                                "reliable Saga client Outbox schema is unavailable",
                                error,
                            )
                        })?;
                    Arc::new(
                        naoutbox_pgsql::PgOutbox::with_datasource(datasource).map_err(|error| {
                            saga_source_error(
                                ApplicationPhase::Ready,
                                "reliable Saga client Outbox binding is invalid",
                                error,
                            )
                        })?,
                    )
                }
                #[cfg(not(feature = "saga-pgsql"))]
                return Err(saga_error(
                    ApplicationPhase::Ready,
                    "reliable Saga client requires the PostgreSQL Outbox capability",
                ));
            }
        };
        start_intent = Some(Arc::clone(&append));
        // start-intent 与 dispatcher 必须使用同一事务域，不能因全局 Outbox 默认值而留下无人扫描的已受理事件。
        plan = plan.with_event_publisher_plan(
            crate::outbox::OutboxApplicationPlan::new(Arc::new(SagaStartIntentPublisher {
                transport: transport.clone(),
            }))
            .with_datasource_ref(datasource)?
            .dead_letter_after(1)?,
        )?;
    }
    plan.remote_client = Some(Arc::new(SagaRemoteClient {
        transport,
        start_intent,
    }));
    plan.definition_publish =
        build_managed_definition_publish_plan(application, settings, &producer)?;
    Ok(plan)
}

/// 业务作用：在一个明确共享事务域内合并 Orchestrator 与 participant 权威，只保留一个受管 Outbox 发布端。
///
/// 参数说明：`application` 提供唯一 datasource catalog，`settings` 已显式批准 combined 角色。
///
/// 返回：两个角色的 driver 与 datasource 完全相同时返回合并计划；任意跨库或重复发布权威拒绝 Ready。
async fn build_managed_combined_plan(
    application: &Application,
    settings: &SagaSettings,
) -> ApplicationResult<SagaApplicationPlan> {
    let orchestrator_datasource = settings.orchestrator_datasource_ref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "combined role requires an orchestrator datasource",
        )
    })?;
    let participant_datasource = settings.participant_datasource_ref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "combined role requires a participant datasource",
        )
    })?;
    if orchestrator_datasource != participant_datasource {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed combined role requires one shared transaction datasource",
        ));
    }
    let mut orchestrator = build_managed_orchestrator_plan(application, settings).await?;
    let mut participant = build_managed_participant_plan(application, settings).await?;
    orchestrator.participants = std::mem::take(&mut participant.participants);
    orchestrator.command_handlers = std::mem::take(&mut participant.command_handlers);
    orchestrator.capability_publish = participant.capability_publish.take();
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    {
        orchestrator.grpc_capability_publish = participant.grpc_capability_publish.take();
    }
    // combined 的本地 definition 由共享 Catalog watcher 直接持久化，不能再保留参与方的远程发布权威。
    let _ = participant.definition_publish.take();
    Ok(orchestrator)
}

/// 业务作用：复验 managed 配置选择的协议已纳入同一 Application 生命周期。
///
/// 参数说明：`application` 提供已声明组件集，`settings` 提供数据面与 API 暴露选择。
///
/// 返回：HTTP、gRPC、Redis 或 Kafka 所需组件全部已声明时成功；缺失任一依赖时拒绝 Ready。
fn validate_managed_component_capabilities(
    application: &Application,
    settings: &SagaSettings,
) -> ApplicationResult<()> {
    let transport = settings
        .transport
        .command_result
        .as_ref()
        .and_then(|settings| settings.kind);
    if settings.api.http.enabled || transport == Some(SagaTransportKind::Http) {
        application.ensure_component_declared(
            ComponentId::Web,
            ApplicationPhase::Ready,
            "managed Saga HTTP",
        )?;
    }
    if settings.api.grpc.enabled || transport == Some(SagaTransportKind::Grpc) {
        application.ensure_component_declared(
            ComponentId::Grpc,
            ApplicationPhase::Ready,
            "managed Saga gRPC",
        )?;
    }
    if transport == Some(SagaTransportKind::RedisStream) {
        application.ensure_component_declared(
            ComponentId::Redis,
            ApplicationPhase::Ready,
            "managed Saga Redis Streams transport",
        )?;
    }
    if transport == Some(SagaTransportKind::Kafka) {
        application.ensure_component_declared(
            ComponentId::Kafka,
            ApplicationPhase::Ready,
            "managed Saga Kafka transport",
        )?;
    }
    if managed_uses_capability_registry(settings) {
        match managed_registry_protocol(settings) {
            Some(SagaClientProtocol::Http) => application.ensure_component_declared(
                ComponentId::Web,
                ApplicationPhase::Ready,
                "managed Saga HTTP registry client",
            )?,
            Some(SagaClientProtocol::Grpc) => application.ensure_component_declared(
                ComponentId::Grpc,
                ApplicationPhase::Ready,
                "managed Saga gRPC registry client",
            )?,
            None => {
                return Err(saga_error(
                    ApplicationPhase::Ready,
                    "capability-registry routing requires registry_client protocol",
                ));
            }
        }
    }
    if settings.role == Some(SagaRole::Client) {
        match settings.client.protocol.unwrap_or(SagaClientProtocol::Http) {
            SagaClientProtocol::Http => application.ensure_component_declared(
                ComponentId::Web,
                ApplicationPhase::Ready,
                "managed Saga HTTP client",
            )?,
            SagaClientProtocol::Grpc => application.ensure_component_declared(
                ComponentId::Grpc,
                ApplicationPhase::Ready,
                "managed Saga gRPC client",
            )?,
        }
    }
    Ok(())
}

/// 业务作用：把冻结协议配置与当前 definition 快照组合为唯一 Outbox 发布端。
///
/// 参数说明：`application` 提供同代 secret，`settings` 提供角色与协议，`registry` 与 `capabilities` 来自同一 Catalog generation。
///
/// 返回：静态 route 完整时返回可投递发布端；空动态 Catalog 返回不会虚假确认的等待端；其它缺口拒绝 Ready。
async fn build_managed_protocol_publisher(
    application: &Application,
    settings: &SagaSettings,
    registry: &DefinitionRegistry,
    capabilities: &[nasaga_runtime::RegisteredCapability],
) -> ApplicationResult<ManagedPublisherBuild> {
    let role = settings
        .role
        .ok_or_else(|| saga_error(ApplicationPhase::Ready, "saga.role is required"))?;
    let transport = settings
        .transport
        .command_result
        .as_ref()
        .and_then(|transport| transport.kind)
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "managed Saga data role has no command_result transport",
            )
        })?;
    if matches!(role, SagaRole::Orchestrator | SagaRole::Combined) {
        let activation =
            managed_definition_activation_contract(application, settings, ApplicationPhase::Ready)?;
        validate_managed_registry_result_contract(&activation, registry, ApplicationPhase::Ready)?;
    }
    if transport == SagaTransportKind::Kafka {
        #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
        return build_managed_kafka_publisher(application, settings, role, registry, capabilities)
            .await;
        #[cfg(not(any(feature = "saga-kafka", feature = "saga-kafka-pgsql")))]
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed Saga Kafka transport is not compiled into this application",
        ));
    }
    if transport == SagaTransportKind::Grpc {
        #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
        return build_managed_grpc_publisher(application, settings, role, registry, capabilities)
            .await;
        #[cfg(not(any(feature = "saga-grpc", feature = "saga-grpc-pgsql")))]
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed Saga gRPC transport is not compiled into this application",
        ));
    }
    if transport == SagaTransportKind::RedisStream {
        #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
        return build_managed_redis_publisher(application, settings, role, registry, capabilities)
            .await;
        #[cfg(not(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql")))]
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed Saga Redis Streams transport is not compiled into this application",
        ));
    }
    if transport != SagaTransportKind::Http {
        if settings.definition_catalog.mode == DefinitionCatalogMode::Dynamic && registry.is_empty()
        {
            return Ok(ManagedPublisherBuild {
                publisher: ManagedProtocolPublisher::Awaiting(AwaitingCatalogPublisher),
                #[cfg(feature = "web")]
                dynamic_http_routing: None,
                #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
                dynamic_kafka_routing: None,
                #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
                dynamic_redis_routing: None,
                #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
                redis_transport: None,
                #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
                dynamic_grpc_routing: None,
                #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
                dynamic_grpc_credential: None,
                #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
                dynamic_grpc_timeout: None,
            });
        }
        return Err(saga_error(
            ApplicationPhase::Ready,
            "selected managed Saga transport adapter is not available in this build",
        ));
    }
    #[cfg(not(feature = "web"))]
    {
        let _ = (application, role);
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed Saga HTTP requires the Web capability",
        ));
    }
    #[cfg(feature = "web")]
    {
        let http = settings
            .transport
            .command_result
            .as_ref()
            .and_then(|transport| transport.http.as_ref())
            .ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "managed Saga HTTP settings are missing",
                )
            })?;
        let dynamic_routing = http.routing.mode.as_deref() == Some("capability-registry");
        let owns_command_routing = matches!(role, SagaRole::Orchestrator | SagaRole::Combined);
        // 只有命令路由所有者会消费 capability endpoint；参与方结果地址由独立配置固定，不能误套
        // owner 的出站地址政策，否则合法参与方会在发布自身 capability 之前被拒绝进入 Ready。
        let address_policy = (owns_command_routing && dynamic_routing)
            .then(|| {
                resolve_managed_address_policy(
                    settings,
                    &http.routing,
                    SagaTransportKind::Http,
                    ApplicationPhase::Ready,
                )
            })
            .transpose()?;
        let producer_text = match role {
            SagaRole::Orchestrator | SagaRole::Combined => settings.service_identity.as_deref(),
            SagaRole::Participant => settings.participant.service_identity.as_deref(),
            SagaRole::Client => settings.client.service_identity.as_deref(),
        }
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "managed Saga HTTP producer identity is missing",
            )
        })?;
        let producer = nasaga_runtime::ServiceIdentity::new(producer_text).map_err(|error| {
            saga_source_error(
                ApplicationPhase::Ready,
                "managed Saga HTTP producer identity is invalid",
                error,
            )
        })?;
        let timeout = Duration::from_millis(http.request_timeout_ms.unwrap_or(5_000));
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Ready,
                    "managed Saga HTTP client construction failed",
                    error,
                )
            })?;
        let mut command_targets = BTreeMap::new();
        if owns_command_routing {
            if dynamic_routing {
                let credential_ref = http.command_credential_ref.as_deref().ok_or_else(|| {
                    saga_error(
                        ApplicationPhase::Ready,
                        "dynamic Saga HTTP command credential reference is missing",
                    )
                })?;
                let authenticator = load_managed_http_authenticator(application, credential_ref)?;
                // 编排端重启时参与方租约可能已经过期，而续租入口必须等 Web listener 开放后才可达。
                // 启动代允许先发布空缺 route；Outbox 在缺少目标时保留命令，Catalog 监督循环随后以
                // 严格模式复验全部 route，并在能力重新登记前保持 readiness 摘流。
                command_targets = build_dynamic_http_routing(
                    registry,
                    capabilities,
                    &authenticator,
                    address_policy.expect("dynamic HTTP address policy was validated"),
                    true,
                )?;
            } else {
                for definition in registry.definitions() {
                    for step in definition.steps() {
                        let owner = step.owner().as_str().to_owned();
                        let route = http.routing.static_routes.get(&owner).ok_or_else(|| {
                            saga_error(
                                ApplicationPhase::Ready,
                                "static Saga HTTP routing is missing a definition owner",
                            )
                        })?;
                        let credential_ref =
                            http.producer_credentials.get(&owner).ok_or_else(|| {
                                saga_error(
                                    ApplicationPhase::Ready,
                                    "static Saga HTTP owner credential is missing",
                                )
                            })?;
                        let authenticator =
                            load_managed_http_authenticator(application, credential_ref)?;
                        command_targets.insert(
                            (
                                String::new(),
                                definition.name().as_str().to_owned(),
                                definition.version().get(),
                                step.name().as_str().to_owned(),
                            ),
                            vec![managed_http_target(route, "commands", authenticator)?],
                        );
                    }
                }
            }
        }
        let result_target = if matches!(role, SagaRole::Participant | SagaRole::Combined) {
            let orchestrator = settings
                .participant
                .orchestrator_identity
                .as_deref()
                .ok_or_else(|| {
                    saga_error(
                        ApplicationPhase::Ready,
                        "participant orchestrator identity is missing",
                    )
                })?;
            let route = if dynamic_routing {
                http.orchestrator_discovery_ref.as_ref().ok_or_else(|| {
                    saga_error(
                        ApplicationPhase::Ready,
                        "participant HTTP transport requires an orchestrator route reference",
                    )
                })?
            } else {
                http.routing
                    .static_routes
                    .get(orchestrator)
                    .ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "static Saga HTTP routing is missing the orchestrator",
                        )
                    })?
            };
            let credential_ref = http.result_credential_ref.as_deref().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "participant result credential reference is missing",
                )
            })?;
            let authenticator = load_managed_http_authenticator(application, credential_ref)?;
            Some(managed_http_discovery_target(
                application,
                settings,
                route,
                "results",
                authenticator,
                Duration::from_millis(http.request_timeout_ms.unwrap_or(5_000)),
            )?)
        } else {
            None
        };
        let routing = Arc::new(std::sync::RwLock::new(ManagedHttpRoutingSnapshot {
            command_targets,
        }));
        Ok(ManagedPublisherBuild {
            publisher: ManagedProtocolPublisher::Http(Box::new(ManagedHttpPublisher {
                client,
                request_timeout: Duration::from_millis(http.request_timeout_ms.unwrap_or(5_000)),
                producer,
                routing: Arc::clone(&routing),
                result_target,
            })),
            dynamic_http_routing: (owns_command_routing && dynamic_routing).then_some(routing),
            #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
            dynamic_kafka_routing: None,
            #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
            dynamic_redis_routing: None,
            #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
            redis_transport: None,
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            dynamic_grpc_routing: None,
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            dynamic_grpc_credential: None,
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            dynamic_grpc_timeout: None,
        })
    }
}

/// 业务作用：从受管 Kafka client、冻结 definition 与 capability 路由构造 command/result 发布端。
///
/// 参数说明：Application 提供 producer lane，其余参数固定角色、topic 前缀与当前 Catalog 快照。
///
/// 返回：所有静态 route 完整或动态快照可等待时返回发布端；client、topic 或 broker 写门禁失败时拒绝 Ready。
#[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
async fn build_managed_kafka_publisher(
    application: &Application,
    settings: &SagaSettings,
    role: SagaRole,
    registry: &DefinitionRegistry,
    capabilities: &[nasaga_runtime::RegisteredCapability],
) -> ApplicationResult<ManagedPublisherBuild> {
    let kafka = settings
        .transport
        .command_result
        .as_ref()
        .and_then(|transport| transport.kafka.as_ref())
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "managed Saga Kafka settings are missing",
            )
        })?;
    let client_ref = kafka.client_ref.as_deref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "managed Saga Kafka client_ref is missing",
        )
    })?;
    let lane = application.kafka(client_ref)?.producer_lane("default")?;
    let command_prefix = kafka.command_topic.as_deref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "managed Saga Kafka command_topic is missing",
        )
    })?;
    let result_prefix = kafka.result_topic.as_deref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "managed Saga Kafka result_topic is missing",
        )
    })?;
    validate_managed_kafka_name(command_prefix, "command topic")?;
    validate_managed_kafka_name(result_prefix, "result topic")?;
    let dynamic = kafka.routing.mode.as_deref() == Some("capability-registry");
    let owns_command_routing = matches!(role, SagaRole::Orchestrator | SagaRole::Combined);
    // capability endpoint 只进入 command 路由；参与方的 result topic 由稳定 owner topic 决定。
    let address_policy = (owns_command_routing && dynamic)
        .then(|| {
            resolve_managed_address_policy(
                settings,
                &kafka.routing,
                SagaTransportKind::Kafka,
                ApplicationPhase::Ready,
            )
        })
        .transpose()?;
    let command_topics = build_managed_kafka_command_topics(
        registry,
        capabilities,
        command_prefix,
        dynamic,
        address_policy,
        true,
    )?;
    let routing = Arc::new(std::sync::RwLock::new(ManagedKafkaRoutingSnapshot {
        command_topics,
    }));
    let result_topic = if matches!(role, SagaRole::Participant | SagaRole::Combined) {
        let owner = settings
            .participant
            .service_identity
            .as_deref()
            .ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "managed Saga Kafka participant identity is missing",
                )
            })?;
        Some(managed_kafka_owner_topic(result_prefix, owner)?)
    } else {
        None
    };
    if let Some(topic) = result_topic.as_deref() {
        let probe = nasaga_runtime::SagaResultEnvelope {
            event_id: String::new(),
            saga_id: String::new(),
            tenant_id: String::new(),
            workflow: String::new(),
            definition_version: 0,
            definition_digest: String::new(),
            step: String::new(),
            phase: String::new(),
            attempt: 0,
            effect_id: String::new(),
            command_id: String::new(),
            status: String::new(),
            terminal_status: None,
            reason_code: None,
        };
        let payload = serde_json::to_vec(&probe).map_err(|error| {
            saga_source_error(
                ApplicationPhase::Ready,
                "managed Saga Kafka readiness probe cannot be encoded",
                error,
            )
        })?;
        // capability 只有在 broker 明确确认 owner result topic 写入后才能续租；Kafka client
        // 的认证、broker 选择与 topic WRITE ACL 因而成为发布能力的前置证据。
        lane.publish_raw(topic, &payload)
            .event(nasaga_runtime::RESULT_EVENT_TYPE)
            .key("managed-saga-result-readiness")
            .header(MANAGED_KAFKA_RESULT_PROBE_HEADER, Some(b"1"))
            .send()
            .await
            .map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Ready,
                    "managed Saga Kafka result topic is not writable",
                    error,
                )
            })?;
    }
    Ok(ManagedPublisherBuild {
        publisher: ManagedProtocolPublisher::Kafka(ManagedKafkaPublisher {
            lane,
            routing: Arc::clone(&routing),
            result_topic,
        }),
        #[cfg(feature = "web")]
        dynamic_http_routing: None,
        dynamic_kafka_routing: (owns_command_routing && dynamic).then_some(routing),
        #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
        dynamic_redis_routing: None,
        #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
        redis_transport: None,
        #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
        dynamic_grpc_routing: None,
        #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
        dynamic_grpc_credential: None,
        #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
        dynamic_grpc_timeout: None,
    })
}

/// 业务作用：从 secret 冻结 Redis stream 生产者签名与消费者验签合同。
///
/// 参数说明：`application` 提供当前 `SecretSnapshot`，`credential_ref` 是受信 secret 引用。
///
/// 返回：JSON 形态、服务身份和密钥长度均合法时返回冻结材料；其它情况拒绝 Ready。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
fn load_managed_redis_credentials(
    application: &Application,
    credential_ref: &str,
) -> ApplicationResult<ManagedRedisCredentialMaterial> {
    let secrets = application.secrets();
    let secret = secrets.get(credential_ref).ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "managed Saga Redis credential is unavailable",
        )
    })?;
    let material: ManagedRedisCredentialMaterial = serde_json::from_slice(secret.expose())
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Ready,
                "managed Saga Redis credential is invalid",
                error,
            )
        })?;
    if material.signing_keys.is_empty() && material.verification_keys.is_empty() {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed Saga Redis credential contains no signing or verification keys",
        ));
    }
    Ok(material)
}

/// 业务作用：选择当前逻辑服务唯一的 Redis stream 出站签名密钥。
///
/// 参数说明：`material` 是 secret 快照，`identity` 是当前 command 或 result 生产者。
///
/// 返回：key id 合法且密钥至少 256 bit 时返回 HMAC 签名器；缺失绑定时拒绝 Ready。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
fn managed_redis_signing_auth(
    material: &ManagedRedisCredentialMaterial,
    identity: &nasaga_runtime::ServiceIdentity,
) -> ApplicationResult<nasaga_runtime::SagaStreamAuth> {
    let signing = material
        .signing_keys
        .get(identity.as_str())
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "managed Saga Redis producer has no signing key",
            )
        })?;
    validate_runtime_name(
        &signing.key_id,
        "Redis signing key id",
        ApplicationPhase::Ready,
    )?;
    let key = hex::decode(&signing.key_hex).map_err(|error| {
        saga_source_error(
            ApplicationPhase::Ready,
            "managed Saga Redis signing key is not hexadecimal",
            error,
        )
    })?;
    if key.len() < 32 {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed Saga Redis signing key is shorter than 256 bits",
        ));
    }
    Ok(nasaga_runtime::SagaStreamAuth::Hmac {
        key_id: signing.key_id.clone(),
        key,
    })
}

/// 业务作用：把 Redis stream key id 验签表冻结为不可伪造的 `ServiceIdentity` 映射。
///
/// 参数说明：`material` 来自不进入普通配置树的 secret。
///
/// 返回：所有 key id、服务身份和 256 bit 密钥均合法时返回 keyring；空表或非法项拒绝 Ready。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
fn managed_redis_verification_auth(
    material: &ManagedRedisCredentialMaterial,
) -> ApplicationResult<nasaga_runtime::SagaStreamAuth> {
    if material.verification_keys.is_empty() {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed Saga Redis consumer has no verification keys",
        ));
    }
    let mut keys = BTreeMap::new();
    for (key_id, configured) in &material.verification_keys {
        validate_runtime_name(key_id, "Redis verification key id", ApplicationPhase::Ready)?;
        let producer = nasaga_runtime::ServiceIdentity::new(&configured.service_identity).map_err(
            |error| {
                saga_source_error(
                    ApplicationPhase::Ready,
                    "managed Saga Redis verification identity is invalid",
                    error,
                )
            },
        )?;
        let key = hex::decode(&configured.key_hex).map_err(|error| {
            saga_source_error(
                ApplicationPhase::Ready,
                "managed Saga Redis verification key is not hexadecimal",
                error,
            )
        })?;
        if key.len() < 32 {
            return Err(saga_error(
                ApplicationPhase::Ready,
                "managed Saga Redis verification key is shorter than 256 bits",
            ));
        }
        keys.insert(
            key_id.clone(),
            nasaga_runtime::SagaStreamVerificationKey::new(producer, key).map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Ready,
                    "managed Saga Redis verification key is invalid",
                    error,
                )
            })?,
        );
    }
    Ok(nasaga_runtime::SagaStreamAuth::HmacKeyring { keys })
}

/// 业务作用：为一个明确 Redis stream 构造只接受指定 Saga 事件的 `XADD` 发布端。
///
/// 参数说明：客户端、事件类型、stream、签名与 hash tag 来自同代受信配置。
///
/// 返回：stream 名称与 cluster 同槽合同成立时返回发布端；其它情况拒绝 Ready。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
fn managed_redis_event_publisher(
    client: Arc<nadis::RedisClient>,
    event_type: &str,
    stream: &str,
    auth: nasaga_runtime::SagaStreamAuth,
    key_tag: Option<&str>,
) -> ApplicationResult<Arc<nasaga_runtime::SagaRedisStreamPublisher>> {
    let mut streams = BTreeMap::new();
    streams.insert(event_type.to_owned(), stream.to_owned());
    nasaga_runtime::SagaRedisStreamPublisher::new(client, streams, auth, key_tag)
        .map(Arc::new)
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Ready,
                "managed Saga Redis publisher route is invalid",
                error,
            )
        })
}

/// 业务作用：从同代 definition/capability 快照构造 Redis command 发布路由。
///
/// 参数说明：路由快照、Redis 客户端、签名、同槽标识与地址政策必须来自同一 Ready 配置。
///
/// 返回：每个 active 步骤有且仅有一个 stream 时返回映射；缺失或歧义路由拒绝切代。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
#[allow(clippy::too_many_arguments)]
fn build_dynamic_redis_command_publishers(
    registry: &DefinitionRegistry,
    capabilities: &[nasaga_runtime::RegisteredCapability],
    client: Arc<nadis::RedisClient>,
    auth: &nasaga_runtime::SagaStreamAuth,
    key_tag: Option<&str>,
    address_policy: &SagaAddressPolicySettings,
    allow_missing: bool,
) -> ApplicationResult<ManagedRedisCommandRoutes> {
    let mut routes = BTreeMap::new();
    for (tenant, definition) in registry.definitions_with_tenants() {
        let tenant = tenant.unwrap_or("");
        for step in definition.steps() {
            let descriptor = nasaga_runtime::select_capability_route(
                capabilities,
                "redis-stream",
                tenant,
                definition,
                step,
            )
            .map_err(|_| {
                saga_error(
                    ApplicationPhase::Ready,
                    "active Saga definition step has an invalid or ambiguous Redis capability route",
                )
            })?;
            let descriptor = match descriptor {
                Some(descriptor) => descriptor,
                None if allow_missing => continue,
                None => {
                    return Err(saga_error(
                        ApplicationPhase::Ready,
                        "active Saga definition step has no live Redis capability route",
                    ));
                }
            };
            validate_capability_address(address_policy, descriptor, ApplicationPhase::Running)?;
            routes.insert(
                (
                    tenant.to_owned(),
                    definition.name().as_str().to_owned(),
                    definition.version().get(),
                    step.name().as_str().to_owned(),
                ),
                managed_redis_event_publisher(
                    Arc::clone(&client),
                    nasaga_runtime::COMMAND_EVENT_TYPE,
                    &descriptor.endpoint,
                    auth.clone(),
                    key_tag,
                )?,
            );
        }
    }
    Ok(routes)
}

/// 业务作用：仅依据配置、本地 descriptor 和 Catalog 快照构造 Redis command/result 闭环。
///
/// 参数说明：Application 提供 Redis/secret/runtime，其余参数冻结角色与当前目录代际。
///
/// 返回：publisher、动态路由和角色允许的消费计划全部就绪时返回；任一原子或身份边界缺失时拒绝 Ready。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
async fn build_managed_redis_publisher(
    application: &Application,
    settings: &SagaSettings,
    role: SagaRole,
    registry: &DefinitionRegistry,
    capabilities: &[nasaga_runtime::RegisteredCapability],
) -> ApplicationResult<ManagedPublisherBuild> {
    let stream = settings
        .transport
        .command_result
        .as_ref()
        .and_then(|transport| transport.redis_stream.as_ref())
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "managed Saga Redis Streams settings are missing",
            )
        })?;
    let client_ref = stream.client_ref.as_deref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "managed Saga Redis Streams client_ref is missing",
        )
    })?;
    let credential_ref = stream.credential_ref.as_deref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "managed Saga Redis Streams credential_ref is missing",
        )
    })?;
    let client = application.redis(client_ref).await?;
    let credentials = load_managed_redis_credentials(application, credential_ref)?;
    let dynamic = stream.routing.mode.as_deref() == Some("capability-registry");
    let owns_command_routing = matches!(role, SagaRole::Orchestrator | SagaRole::Combined);
    // 参与方只向显式 result stream 发布，不应读取或持有 owner command 地址政策。
    let address_policy = (owns_command_routing && dynamic)
        .then(|| {
            resolve_managed_address_policy(
                settings,
                &stream.routing,
                SagaTransportKind::RedisStream,
                ApplicationPhase::Ready,
            )
        })
        .transpose()?;
    let command_identity = settings
        .service_identity
        .as_deref()
        .map(nasaga_runtime::ServiceIdentity::new)
        .transpose()
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Ready,
                "managed Saga Redis Orchestrator identity is invalid",
                error,
            )
        })?;
    let result_identity = settings
        .participant
        .service_identity
        .as_deref()
        .map(nasaga_runtime::ServiceIdentity::new)
        .transpose()
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Ready,
                "managed Saga Redis participant identity is invalid",
                error,
            )
        })?;
    let command_auth = if owns_command_routing {
        Some(managed_redis_signing_auth(
            &credentials,
            command_identity.as_ref().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "managed Saga Redis command producer identity is missing",
                )
            })?,
        )?)
    } else {
        None
    };
    let command_publishers = if let Some(auth) = command_auth.as_ref() {
        if dynamic {
            build_dynamic_redis_command_publishers(
                registry,
                capabilities,
                Arc::clone(&client),
                auth,
                stream.key_tag.as_deref(),
                address_policy.expect("dynamic Redis address policy was validated"),
                true,
            )?
        } else {
            let mut routes = BTreeMap::new();
            for definition in registry.definitions() {
                for step in definition.steps() {
                    let prefix = stream
                        .routing
                        .static_routes
                        .get(step.owner().as_str())
                        .ok_or_else(|| {
                            saga_error(
                                ApplicationPhase::Ready,
                                "static Saga Redis routing is missing a definition owner",
                            )
                        })?;
                    let endpoint = managed_redis_step_stream(
                        prefix,
                        step.owner().as_str(),
                        definition.name().as_str(),
                        definition.version().get(),
                        step.name().as_str(),
                    )?;
                    routes.insert(
                        (
                            String::new(),
                            definition.name().as_str().to_owned(),
                            definition.version().get(),
                            step.name().as_str().to_owned(),
                        ),
                        managed_redis_event_publisher(
                            Arc::clone(&client),
                            nasaga_runtime::COMMAND_EVENT_TYPE,
                            &endpoint,
                            auth.clone(),
                            stream.key_tag.as_deref(),
                        )?,
                    );
                }
            }
            routes
        }
    } else {
        BTreeMap::new()
    };
    let routing = Arc::new(std::sync::RwLock::new(ManagedRedisRoutingSnapshot {
        command_publishers,
    }));
    let result_publisher = if matches!(role, SagaRole::Participant | SagaRole::Combined) {
        let identity = result_identity.as_ref().ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "managed Saga Redis result producer identity is missing",
            )
        })?;
        let target = stream.result_stream.as_deref().ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "managed Saga Redis result stream is missing",
            )
        })?;
        Some(managed_redis_event_publisher(
            Arc::clone(&client),
            nasaga_runtime::RESULT_EVENT_TYPE,
            target,
            managed_redis_signing_auth(&credentials, identity)?,
            stream.key_tag.as_deref(),
        )?)
    } else {
        None
    };
    let verification_auth = managed_redis_verification_auth(&credentials)?;
    let mut transport = SagaRedisTransportPlan::new(client_ref);
    if matches!(role, SagaRole::Orchestrator | SagaRole::Combined) {
        let producer = command_identity.clone().ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "managed Saga Redis result consumer identity is missing",
            )
        })?;
        let config = managed_redis_consumer_config(
            stream.result_stream.as_deref().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "managed Saga Redis result_stream is missing",
                )
            })?,
            stream.result_dlt_stream.as_deref().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "managed Saga Redis result_dlt_stream is missing",
                )
            })?,
            stream.result_group.as_deref().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "managed Saga Redis result_group is missing",
                )
            })?,
            stream.result_consumer.as_deref().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "managed Saga Redis result_consumer is missing",
                )
            })?,
            verification_auth.clone(),
            producer,
            stream.key_tag.clone(),
        );
        let poller = nasaga_runtime::SagaRedisStreamResultConsumer::new(
            Arc::new(ManagedRedisResultHandler {
                state: application.saga_runtime(),
            }),
            config,
        )
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Ready,
                "managed Saga Redis result consumer is invalid",
                error,
            )
        })?;
        transport = transport.with_poller(Arc::new(poller))?;
    }
    if matches!(role, SagaRole::Participant | SagaRole::Combined) {
        let producer = nasaga_runtime::ServiceIdentity::new(
            settings
                .participant
                .orchestrator_identity
                .as_deref()
                .ok_or_else(|| {
                    saga_error(
                        ApplicationPhase::Ready,
                        "managed Saga Redis command producer identity is missing",
                    )
                })?,
        )
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Ready,
                "managed Saga Redis command producer identity is invalid",
                error,
            )
        })?;
        let prefix = stream.command_stream.as_deref().ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "managed Saga Redis command_stream is missing",
            )
        })?;
        let handler = Arc::new(ManagedRedisCommandHandler {
            state: application.saga_runtime(),
        });
        for descriptor in nasaga_runtime::COLLECTED_SAGA_STEPS {
            let endpoint = managed_redis_step_stream(
                prefix,
                result_identity
                    .as_ref()
                    .expect("participant identity was validated")
                    .as_str(),
                descriptor.workflow,
                descriptor.definition_version,
                descriptor.step,
            )?;
            let config = managed_redis_consumer_config(
                &endpoint,
                stream.command_dlt_stream.as_deref().ok_or_else(|| {
                    saga_error(
                        ApplicationPhase::Ready,
                        "managed Saga Redis command_dlt_stream is missing",
                    )
                })?,
                stream.command_group.as_deref().ok_or_else(|| {
                    saga_error(
                        ApplicationPhase::Ready,
                        "managed Saga Redis command_group is missing",
                    )
                })?,
                stream.command_consumer.as_deref().ok_or_else(|| {
                    saga_error(
                        ApplicationPhase::Ready,
                        "managed Saga Redis command_consumer is missing",
                    )
                })?,
                verification_auth.clone(),
                producer.clone(),
                stream.key_tag.clone(),
            );
            let poller = nasaga_runtime::SagaRedisStreamCommandConsumer::for_managed_capability(
                Arc::clone(&handler),
                config,
                nasaga_runtime::__private::core::WorkflowName::new(descriptor.workflow)
                    .map_err(|error| anyhow::anyhow!(error.code()))
                    .map_err(|error| {
                        saga_source_error(
                            ApplicationPhase::Ready,
                            "managed Saga Redis command workflow is invalid",
                            error,
                        )
                    })?,
                nasaga_runtime::__private::core::DefinitionVersion::new(
                    descriptor.definition_version,
                )
                .map_err(|error| anyhow::anyhow!(error.code()))
                .map_err(|error| {
                    saga_source_error(
                        ApplicationPhase::Ready,
                        "managed Saga Redis command version is invalid",
                        error,
                    )
                })?,
                nasaga_runtime::__private::core::StepName::new(descriptor.step)
                    .map_err(|error| anyhow::anyhow!(error.code()))
                    .map_err(|error| {
                        saga_source_error(
                            ApplicationPhase::Ready,
                            "managed Saga Redis command step is invalid",
                            error,
                        )
                    })?,
            )
            .map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Ready,
                    "managed Saga Redis command consumer is invalid",
                    error,
                )
            })?;
            transport = transport.with_poller(Arc::new(poller))?;
        }
    }
    Ok(ManagedPublisherBuild {
        publisher: ManagedProtocolPublisher::Redis(ManagedRedisPublisher {
            routing: Arc::clone(&routing),
            result_publisher,
        }),
        #[cfg(feature = "web")]
        dynamic_http_routing: None,
        #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
        dynamic_kafka_routing: None,
        dynamic_redis_routing: (owns_command_routing && dynamic).then_some(routing),
        redis_transport: Some(transport),
        #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
        dynamic_grpc_routing: None,
        #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
        dynamic_grpc_credential: None,
        #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
        dynamic_grpc_timeout: None,
    })
}

/// 业务作用：为受管 Redis stream 生成统一有界消费、重领与持久 DLT 配置。
///
/// 参数说明：路由、group、consumer、认证与 hash tag 都来自 Ready 冻结合同。
///
/// 返回：可交给 result 或 command poller 构造器复验的完整配置。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
#[allow(clippy::too_many_arguments)]
fn managed_redis_consumer_config(
    source: &str,
    dlt: &str,
    group: &str,
    consumer: &str,
    auth: nasaga_runtime::SagaStreamAuth,
    producer: nasaga_runtime::ServiceIdentity,
    key_tag: Option<String>,
) -> nasaga_runtime::SagaStreamConsumerConfig {
    nasaga_runtime::SagaStreamConsumerConfig {
        stream: source.to_owned(),
        dlt_stream: dlt.to_owned(),
        marker_prefix: format!("{dlt}:marker"),
        group: group.to_owned(),
        consumer: consumer.to_owned(),
        batch: 100,
        block_ms: 100,
        handler_timeout_ms: 5_000,
        min_idle_ms: 30_000,
        replay_from_beginning: true,
        marker_ttl_seconds: 604_800,
        auth,
        producer,
        key_tag,
    }
}

/// 业务作用：从 mTLS secret、definition 快照和受信 route 构造 generated gRPC Outbox 发布端。
///
/// 参数说明：Application 提供 secret，其余参数固定角色、静态或动态路由及同代 capability。
///
/// 返回：所有必需 channel 可构造时返回发布端；凭据、URI、owner route 或 TLS 合同缺失时拒绝 Ready。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
async fn build_managed_grpc_publisher(
    application: &Application,
    settings: &SagaSettings,
    role: SagaRole,
    registry: &DefinitionRegistry,
    capabilities: &[nasaga_runtime::RegisteredCapability],
) -> ApplicationResult<ManagedPublisherBuild> {
    let grpc = settings
        .transport
        .command_result
        .as_ref()
        .and_then(|transport| transport.grpc.as_ref())
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "managed Saga gRPC settings are missing",
            )
        })?;
    let credential_ref = grpc.credential_ref.as_deref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "managed Saga gRPC credential_ref is missing",
        )
    })?;
    let credential = security::grpc_credential(application, credential_ref)?;
    if credential.ca_certificate_pem.is_empty()
        || credential.identity_certificate_pem.is_empty()
        || credential.identity_private_key_pem.is_empty()
    {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed Saga gRPC credential material is incomplete",
        ));
    }
    let credential = Arc::new(credential);
    let timeout = Duration::from_millis(grpc.request_timeout_ms.unwrap_or(5_000));
    let dynamic = grpc.routing.mode.as_deref() == Some("capability-registry");
    let owns_command_routing = matches!(role, SagaRole::Orchestrator | SagaRole::Combined);
    // mTLS result target 使用参与方显式的 Orchestrator endpoint；只有 command 路由消费目录地址。
    let address_policy = (owns_command_routing && dynamic)
        .then(|| {
            resolve_managed_address_policy(
                settings,
                &grpc.routing,
                SagaTransportKind::Grpc,
                ApplicationPhase::Ready,
            )
        })
        .transpose()?;
    let mut command_targets = if owns_command_routing && dynamic {
        build_dynamic_grpc_command_targets(
            registry,
            capabilities,
            &credential,
            timeout,
            address_policy.expect("dynamic gRPC address policy was validated"),
            true,
        )?
    } else {
        BTreeMap::new()
    };
    if owns_command_routing && !dynamic {
        for (tenant, definition) in registry.definitions_with_tenants() {
            for step in definition.steps() {
                let endpoint = grpc
                    .routing
                    .static_routes
                    .get(step.owner().as_str())
                    .ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "static Saga gRPC routing is missing a definition owner",
                        )
                    })?;
                command_targets.insert(
                    (
                        tenant.unwrap_or("").to_owned(),
                        definition.name().as_str().to_owned(),
                        definition.version().get(),
                        step.name().as_str().to_owned(),
                    ),
                    build_managed_grpc_target(endpoint, timeout, &credential)?,
                );
            }
        }
    }
    for target in command_targets.values() {
        probe_managed_grpc_target(target, ApplicationPhase::Ready).await?;
    }
    let result_target = if matches!(role, SagaRole::Participant | SagaRole::Combined) {
        let endpoint = grpc.orchestrator_discovery_ref.as_deref().ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "participant gRPC transport requires an Orchestrator endpoint",
            )
        })?;
        let target =
            managed_grpc_discovery_target(application, settings, endpoint, timeout, &credential)?;
        probe_managed_grpc_target(&target, ApplicationPhase::Ready).await?;
        Some(target)
    } else {
        None
    };
    let routing = Arc::new(std::sync::RwLock::new(ManagedGrpcRoutingSnapshot {
        command_targets,
    }));
    Ok(ManagedPublisherBuild {
        publisher: ManagedProtocolPublisher::Grpc(ManagedGrpcPublisher {
            routing: Arc::clone(&routing),
            result_target,
        }),
        #[cfg(feature = "web")]
        dynamic_http_routing: None,
        #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
        dynamic_kafka_routing: None,
        #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
        dynamic_redis_routing: None,
        #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
        redis_transport: None,
        dynamic_grpc_routing: (owns_command_routing && dynamic).then_some(routing),
        dynamic_grpc_credential: (owns_command_routing && dynamic).then_some(credential),
        dynamic_grpc_timeout: (owns_command_routing && dynamic).then_some(timeout),
    })
}

/// 业务作用：从同代 definition/capability 构造完整 gRPC command channel 快照。
///
/// 参数说明：快照、mTLS 凭据、deadline、地址政策与 `allow_missing` 共同决定空租约启动边界。
///
/// 返回：每个步骤保留全部合法逐实例 endpoint；同副本冲突、严格模式缺失或 TLS 错误拒绝发布。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn build_dynamic_grpc_command_targets(
    registry: &DefinitionRegistry,
    capabilities: &[nasaga_runtime::RegisteredCapability],
    credential: &ManagedGrpcCredentialMaterial,
    timeout: Duration,
    address_policy: &SagaAddressPolicySettings,
    allow_missing: bool,
) -> ApplicationResult<BTreeMap<(String, String, u32, String), ManagedGrpcTarget>> {
    let mut routes = BTreeMap::new();
    for (tenant, definition) in registry.definitions_with_tenants() {
        let tenant = tenant.unwrap_or("");
        for step in definition.steps() {
            let descriptors = nasaga_runtime::select_capability_routes(
                capabilities,
                "grpc",
                tenant,
                definition,
                step,
            )
            .map_err(|_| {
                saga_error(
                    ApplicationPhase::Running,
                    "dynamic Saga gRPC route contract is invalid",
                )
            })?;
            if descriptors.is_empty() {
                // 启动装载可等待参与方续租；运行期没有任何有效副本时必须停止发布业务路由。
                if allow_missing {
                    continue;
                }
                return Err(saga_error(
                    ApplicationPhase::Running,
                    "dynamic Saga gRPC route is absent",
                ));
            }
            let mut targets = Vec::new();
            for descriptor in descriptors {
                validate_capability_address(address_policy, descriptor, ApplicationPhase::Running)?;
                targets.push(descriptor.endpoint.clone());
            }
            routes.insert(
                (
                    tenant.to_owned(),
                    definition.name().as_str().to_owned(),
                    definition.version().get(),
                    step.name().as_str().to_owned(),
                ),
                build_managed_grpc_targets(&targets, timeout, credential)?,
            );
        }
    }
    Ok(routes)
}

/// 业务作用：在 candidate 获得数据库生命周期权威前，复验其每条 gRPC command route 的实际 mTLS health。
///
/// 参数说明：`activation` 提供冻结证书、deadline 与地址政策，`snapshot`/`artifact` 提供同代候选路由，
/// `phase` 标记人工或自动激活路径。
///
/// 返回：非 gRPC 数据面直接成功；全部候选端点明确 Serving 时成功，连接或认证不确定时拒绝激活。
async fn probe_managed_candidate_grpc_routes(
    activation: &ManagedDefinitionActivationContract,
    snapshot: &nasaga_runtime::DynamicCatalogSnapshot,
    artifact: &nasaga_runtime::DefinitionArtifact,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    if activation.transport != SagaTransportKind::Grpc {
        return Ok(());
    }
    #[cfg(not(any(feature = "saga-grpc", feature = "saga-grpc-pgsql")))]
    {
        let _ = (snapshot, artifact);
        return Err(saga_error(
            phase,
            "managed Saga gRPC candidate probe is unavailable",
        ));
    }
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    {
        let credential = activation.grpc_credential.as_deref().ok_or_else(|| {
            saga_error(phase, "managed Saga gRPC activation credential is missing")
        })?;
        let timeout = activation
            .grpc_timeout
            .ok_or_else(|| saga_error(phase, "managed Saga gRPC activation deadline is missing"))?;
        let policy = activation.address_policy.as_ref().ok_or_else(|| {
            saga_error(
                phase,
                "managed Saga gRPC activation address policy is missing",
            )
        })?;
        let definition = artifact.to_definition().map_err(|error| {
            saga_source_error(phase, "managed Saga candidate definition is invalid", error)
        })?;
        for step in definition.steps() {
            let descriptors = nasaga_runtime::select_capability_routes(
                &snapshot.capabilities,
                "grpc",
                &artifact.tenant,
                &definition,
                step,
            )
            .map_err(|_| {
                saga_error(
                    phase,
                    "managed Saga candidate gRPC route contract is invalid",
                )
            })?;
            let mut targets = Vec::new();
            for descriptor in descriptors {
                validate_capability_address(policy, descriptor, phase)?;
                targets.push(descriptor.endpoint.clone());
            }
            // 地址合同全部通过后，至少一个副本在当前 mTLS 身份下明确 Serving 才允许激活。
            probe_managed_grpc_target(
                &build_managed_grpc_targets(&targets, timeout, credential)?,
                phase,
            )
            .await?;
        }
        Ok(())
    }
}

/// 业务作用：为当前 definition 步骤构造 Kafka command topic，动态模式只接受 capability 自报的精确 route。
///
/// 参数说明：definition、有效能力、静态前缀、动态地址政策与两个模式开关共同决定
/// 缺失 route 是否允许等待。
///
/// 返回：每个步骤至多一个 topic 时返回冻结映射；歧义、非法 topic 或严格模式缺失时拒绝。
#[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
fn build_managed_kafka_command_topics(
    registry: &DefinitionRegistry,
    capabilities: &[nasaga_runtime::RegisteredCapability],
    command_prefix: &str,
    dynamic: bool,
    address_policy: Option<&SagaAddressPolicySettings>,
    allow_missing: bool,
) -> ApplicationResult<BTreeMap<(String, String, u32, String), String>> {
    let mut routes = BTreeMap::new();
    for (tenant, definition) in registry.definitions_with_tenants() {
        let tenant = tenant.unwrap_or("");
        for step in definition.steps() {
            let topic = if dynamic {
                let descriptor = nasaga_runtime::select_capability_route(
                    capabilities,
                    "kafka",
                    tenant,
                    definition,
                    step,
                )
                .map_err(|_| {
                    saga_error(
                        ApplicationPhase::Ready,
                        "dynamic Saga Kafka route is ambiguous",
                    )
                })?;
                match descriptor {
                    Some(descriptor) => {
                        // 地址政策必须复验刚由完整步骤身份选中的 Kafka descriptor；同名 Redis stream
                        // 或其它步骤的旧租约不能替代它决定当前 route 的准入边界。
                        validate_capability_address(
                            address_policy.expect("dynamic Kafka address policy was validated"),
                            descriptor,
                            ApplicationPhase::Running,
                        )?;
                        descriptor.endpoint.clone()
                    }
                    None if allow_missing => continue,
                    None => {
                        return Err(saga_error(
                            ApplicationPhase::Running,
                            "dynamic Saga Kafka route is absent",
                        ))
                    }
                }
            } else {
                managed_kafka_step_topic(
                    command_prefix,
                    step.owner().as_str(),
                    definition.name().as_str(),
                    definition.version().get(),
                    step.name().as_str(),
                )?
            };
            validate_managed_kafka_name(&topic, "command topic")?;
            routes.insert(
                (
                    tenant.to_owned(),
                    definition.name().as_str().to_owned(),
                    definition.version().get(),
                    step.name().as_str().to_owned(),
                ),
                topic,
            );
        }
    }
    Ok(routes)
}

/// 业务作用：按公开稳定字段派生静态 command topic，使发送方与参与方不依赖业务装配代码。
///
/// 参数说明：前缀、owner、workflow、版本和步骤共同形成唯一 topic。
///
/// 返回：组合结果符合 Kafka 可移植名称边界时返回；超长或非法字段拒绝。
#[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
fn managed_kafka_step_topic(
    prefix: &str,
    owner: &str,
    workflow: &str,
    version: u32,
    step: &str,
) -> ApplicationResult<String> {
    let topic = format!("{prefix}.{owner}.{workflow}.{version}.{step}");
    validate_managed_kafka_name(&topic, "command topic")?;
    Ok(topic)
}

/// 业务作用：按参与方逻辑 owner 派生独占 result topic，使 Orchestrator 可从 topic 绑定可信 producer。
///
/// 参数说明：`prefix` 是配置前缀，`owner` 是 definition 与凭据共同绑定的服务身份。
///
/// 返回：组合 topic 符合 Kafka 名称边界时返回，否则拒绝 Ready。
#[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
fn managed_kafka_owner_topic(prefix: &str, owner: &str) -> ApplicationResult<String> {
    use base64::Engine as _;
    let owner = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(owner.as_bytes());
    let topic = format!("{prefix}.{owner}");
    validate_managed_kafka_name(&topic, "result topic")?;
    Ok(topic)
}

/// 业务作用：按步骤完整身份派生 Redis command stream，使每个消费路由可独立授权与排空。
///
/// 参数说明：前缀必须已携带配置的 cluster hash tag，其余字段是冻结步骤身份。
///
/// 返回：名称可持久且不含控制字符时返回 stream；超界或字段非法时拒绝 Ready。
fn managed_redis_step_stream(
    prefix: &str,
    owner: &str,
    workflow: &str,
    version: u32,
    step: &str,
) -> ApplicationResult<String> {
    for value in [owner, workflow, step] {
        validate_runtime_name(value, "Redis stream route segment", ApplicationPhase::Ready)?;
    }
    let stream = format!("{prefix}:{owner}:{workflow}:{version}:{step}");
    if stream.len() > 190 || stream.chars().any(char::is_control) {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed Saga Redis command stream is invalid",
        ));
    }
    Ok(stream)
}

/// 业务作用：校验 Kafka topic/group 使用可移植字符和协议长度，避免启动后才由 broker 拒绝。
///
/// 参数说明：`value` 是名称，`kind` 仅用于稳定配置诊断。
///
/// 返回：名称非空、有界且字符合法时成功；其它输入返回 Ready 错误。
fn validate_managed_kafka_name(value: &str, kind: &str) -> ApplicationResult<()> {
    if value.is_empty()
        || value.len() > 249
        || value.trim() != value
        || matches!(value, "." | "..")
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(saga_error(
            ApplicationPhase::Ready,
            format!("managed Saga Kafka {kind} is invalid"),
        ));
    }
    Ok(())
}

/// 业务作用：把 Kafka topic 前缀转义为 broker 正则中的字面量，避免配置字符扩大订阅范围。
///
/// 参数说明：`value` 已通过 Kafka portable name 校验。
///
/// 返回：返回可安全嵌入 POSIX extended regex 的字面量片段。
#[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
fn managed_kafka_regex_literal(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() * 2);
    for character in value.chars() {
        if matches!(
            character,
            '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\'
        ) {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

/// 业务作用：从同代 definition 与 capability 快照构造逐命令多实例 HTTP route，不从服务名猜测路径。
///
/// 参数说明：`registry` 提供步骤 owner，`capabilities` 提供实例地址，`authenticator` 是协调者出站身份凭据，
/// `address_policy` 限制协议、主机与端口，`allow_missing_routes` 仅供编排端重启时开放 capability 续租入口。
///
/// 返回：严格模式下每个已装载 definition 步骤都有至少一个匹配 HTTP 实例时返回 route 表；
/// 启动代允许暂缺 route，但不会为缺口构造可投递目标。
#[cfg(feature = "web")]
fn build_dynamic_http_routing(
    registry: &DefinitionRegistry,
    capabilities: &[nasaga_runtime::RegisteredCapability],
    authenticator: &nasaga_runtime::SagaHttpMessageAuthenticator,
    address_policy: &SagaAddressPolicySettings,
    allow_missing_routes: bool,
) -> ApplicationResult<ManagedHttpCommandRoutes> {
    let mut routes = BTreeMap::new();
    for (tenant, definition) in registry.definitions_with_tenants() {
        let tenant = tenant.unwrap_or("");
        for step in definition.steps() {
            let key = (
                tenant.to_owned(),
                definition.name().as_str().to_owned(),
                definition.version().get(),
                step.name().as_str().to_owned(),
            );
            let mut targets = Vec::new();
            for capability in capabilities.iter().filter(|capability| {
                capability.descriptor.transport == "http"
                    && capability.descriptor.matches_step(tenant, definition, step)
            }) {
                validate_capability_address(
                    address_policy,
                    &capability.descriptor,
                    ApplicationPhase::Running,
                )?;
                targets.push(managed_http_capability_target(
                    &capability.descriptor,
                    "commands",
                    authenticator.clone(),
                )?);
            }
            if targets.is_empty() {
                if allow_missing_routes {
                    continue;
                }
                return Err(saga_error(
                    ApplicationPhase::Ready,
                    "active Saga definition step has no live HTTP capability route",
                ));
            }
            routes.insert(key, targets);
        }
    }
    Ok(routes)
}

/// 业务作用：从当前配置 generation 的 secret 快照构造 Saga HTTP HMAC 认证器。
///
/// 参数说明：`application` 提供已脱离普通配置树的 secret material，`credential_ref` 是受信 ID。
///
/// 返回：密钥是六十四位小写十六进制文本时返回认证器；缺失或格式错误时拒绝 Ready。
fn load_managed_http_authenticator(
    application: &Application,
    credential_ref: &str,
) -> ApplicationResult<nasaga_runtime::SagaHttpMessageAuthenticator> {
    security::http_authenticator(application, credential_ref)
}

/// 业务作用：把 capability 中的单实例 origin 与有效 Saga base path 扩展为唯一操作 URL。
///
/// 参数说明：`base` 必须已含该实例 context path 与 Saga base path，`operation` 是框架固定相对操作名。
///
/// 返回：HTTP(S) 地址无凭据、query、fragment 且基础路径非根时返回签名目标；其它形态拒绝。
fn managed_http_target(
    base: &str,
    operation: &str,
    authenticator: nasaga_runtime::SagaHttpMessageAuthenticator,
) -> ApplicationResult<ManagedHttpTarget> {
    let mut url = reqwest::Url::parse(base).map_err(|error| {
        saga_source_error(
            ApplicationPhase::Ready,
            "managed Saga HTTP route URL is invalid",
            error,
        )
    })?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || matches!(url.path(), "" | "/")
    {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed Saga HTTP route must contain an origin and effective Saga base path",
        ));
    }
    let signed_path = format!("{}/{}", url.path().trim_end_matches('/'), operation);
    url.set_path(&signed_path);
    Ok(ManagedHttpTarget {
        url,
        signed_path,
        authenticator,
        discovery: None,
        timeout: Duration::from_secs(5),
    })
}

/// 业务作用：把 capability 同一实例的 origin 与 effective Saga path 拼接一次，形成签名和拨号共同目标。
///
/// 参数说明：`descriptor` 携带逐实例 route，`operation` 是固定操作名，`authenticator` 是出站凭据。
///
/// 返回：origin、路径和 generation 合同规范时返回目标；含凭据、query、fragment 或重复路径时拒绝。
#[cfg(feature = "web")]
fn managed_http_capability_target(
    descriptor: &nasaga_runtime::CapabilityDescriptor,
    operation: &str,
    authenticator: nasaga_runtime::SagaHttpMessageAuthenticator,
) -> ApplicationResult<ManagedHttpTarget> {
    nasaga_runtime::validate_capability_route_contract(descriptor).map_err(|error| {
        saga_source_error(
            ApplicationPhase::Ready,
            "managed Saga HTTP capability route contract is invalid",
            error,
        )
    })?;
    let mut url = reqwest::Url::parse(&descriptor.endpoint).map_err(|error| {
        saga_source_error(
            ApplicationPhase::Ready,
            "managed Saga HTTP capability origin is invalid",
            error,
        )
    })?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed Saga HTTP capability endpoint must be an origin without path or credentials",
        ));
    }
    let base_path = descriptor
        .effective_saga_base_path
        .as_deref()
        .expect("shared HTTP capability route validation requires a base path");
    let signed_path = format!("{}/{}", base_path.trim_end_matches('/'), operation);
    url.set_path(&signed_path);
    Ok(ManagedHttpTarget {
        url,
        signed_path,
        authenticator,
        discovery: None,
        timeout: Duration::from_secs(5),
    })
}

/// 业务作用：从受信配置冻结 managed HTTP 入站路径、逐主体凭据、权限与共享 replay 数据源。
///
/// 参数说明：`application` 提供 secret 快照，`settings` 提供角色、transport 与 API 合同。
///
/// 返回：当前部署有 HTTP 入站时返回安全快照；未启用时返回空；任何身份或凭据缺口拒绝 Ready。
#[cfg(feature = "web")]
async fn build_managed_http_server(
    application: &Application,
    settings: &SagaSettings,
) -> ApplicationResult<Option<Arc<ManagedHttpServer>>> {
    let role = settings
        .role
        .ok_or_else(|| saga_error(ApplicationPhase::Ready, "saga.role is required"))?;
    let http_transport = settings
        .transport
        .command_result
        .as_ref()
        .filter(|transport| transport.kind == Some(SagaTransportKind::Http))
        .and_then(|transport| transport.http.as_ref());
    if http_transport.is_none() && !settings.api.http.enabled {
        return Ok(None);
    }
    let datasource = settings.primary_datasource_ref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "managed Saga HTTP ingress requires a role datasource_ref",
        )
    })?;
    let driver = managed_datasource_driver(application, datasource).await?;
    let mut command_authenticators = BTreeMap::new();
    let mut result_authenticators = BTreeMap::new();
    if let Some(http) = http_transport {
        if matches!(role, SagaRole::Participant | SagaRole::Combined) {
            let producer = nasaga_runtime::ServiceIdentity::new(
                settings
                    .participant
                    .orchestrator_identity
                    .as_deref()
                    .ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "participant orchestrator identity is missing",
                        )
                    })?,
            )
            .map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Ready,
                    "participant orchestrator identity is invalid",
                    error,
                )
            })?;
            let credential = http.command_credential_ref.as_deref().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "participant command credential is missing",
                )
            })?;
            command_authenticators.insert(
                producer,
                load_managed_http_authenticator(application, credential)?,
            );
        }
        if matches!(role, SagaRole::Orchestrator | SagaRole::Combined) {
            if settings.definition_catalog.mode == DefinitionCatalogMode::Dynamic {
                for (owner, credential) in &http.producer_credentials {
                    let owner = nasaga_runtime::ServiceIdentity::new(owner).map_err(|error| {
                        saga_source_error(
                            ApplicationPhase::Ready,
                            "HTTP result producer identity is invalid",
                            error,
                        )
                    })?;
                    result_authenticators.insert(
                        owner,
                        load_managed_http_authenticator(application, credential)?,
                    );
                }
            } else {
                let registry = nasaga_runtime::collect_workflow_definitions().map_err(|error| {
                    saga_source_error(
                        ApplicationPhase::Ready,
                        "HTTP result trust could not collect static definitions",
                        error,
                    )
                })?;
                for definition in registry.definitions() {
                    for step in definition.steps() {
                        let owner = step.owner().clone();
                        if result_authenticators.contains_key(&owner) {
                            continue;
                        }
                        let credential =
                            http.producer_credentials
                                .get(owner.as_str())
                                .ok_or_else(|| {
                                    saga_error(
                                        ApplicationPhase::Ready,
                                        "HTTP result producer credential is missing",
                                    )
                                })?;
                        result_authenticators.insert(
                            owner,
                            load_managed_http_authenticator(application, credential)?,
                        );
                    }
                }
            }
        }
    }
    let mut api_callers = BTreeMap::new();
    if settings.api.http.enabled {
        for (identity, caller) in &settings.api.http.callers {
            let identity = nasaga_runtime::ServiceIdentity::new(identity).map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Ready,
                    "Saga HTTP API caller identity is invalid",
                    error,
                )
            })?;
            let credential = caller.credential_ref.as_deref().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "Saga HTTP API caller credential is missing",
                )
            })?;
            api_callers.insert(
                identity.clone(),
                ManagedHttpApiCaller {
                    authenticator: load_managed_http_authenticator(application, credential)?,
                    actor: SagaApiActor {
                        identity,
                        tenants: caller.tenants.iter().cloned().collect(),
                        workflows: caller.workflows.iter().cloned().collect(),
                        permissions: caller.permissions.iter().cloned().collect(),
                    },
                },
            );
        }
    }
    let definition_signing_keys = if settings.api.http.expose_definition_registry {
        load_managed_definition_signing_keys(
            application,
            &settings.definition_catalog.signing_keys,
            ApplicationPhase::Ready,
        )?
    } else {
        BTreeMap::new()
    };
    let activation =
        managed_definition_activation_contract(application, settings, ApplicationPhase::Ready)?;
    let orchestrator_service_identity =
        if matches!(role, SagaRole::Orchestrator | SagaRole::Combined) {
            Some(
                nasaga_runtime::ServiceIdentity::new(
                    settings.service_identity.as_deref().ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "managed HTTP Orchestrator service identity is missing",
                        )
                    })?,
                )
                .map_err(|error| {
                    saga_source_error(
                        ApplicationPhase::Ready,
                        "managed HTTP Orchestrator service identity is invalid",
                        error,
                    )
                })?,
            )
        } else {
            None
        };
    let page_token_key = if settings.api.http.enabled {
        load_managed_page_token_key(
            application,
            settings.api.page_token_key_ref.as_deref().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "managed Saga API page token key reference is missing",
                )
            })?,
            ApplicationPhase::Ready,
        )?
    } else {
        [0; 32]
    };
    Ok(Some(Arc::new(ManagedHttpServer {
        datasource: datasource.to_owned(),
        driver,
        base_path: settings
            .http
            .base_path
            .clone()
            .unwrap_or_else(|| "/_nasa/saga".to_owned()),
        body_limit_bytes: http_transport
            .and_then(|http| http.body_limit_bytes)
            .unwrap_or(1024 * 1024),
        concurrency_limit: http_transport
            .and_then(|http| http.concurrency_limit)
            .unwrap_or(256),
        request_timeout: Duration::from_millis(
            http_transport
                .and_then(|http| http.request_timeout_ms)
                .unwrap_or(5_000),
        ),
        command_authenticators,
        result_authenticators,
        api_callers,
        api_enabled: settings.api.http.enabled,
        expose_admin: settings.api.http.expose_admin,
        expose_definition_registry: settings.api.http.expose_definition_registry,
        definition_signing_keys,
        orchestrator_service_identity,
        activation,
        page_token_key,
    })))
}

/// 业务作用：让 HTTP 创建请求使用唯一业务意图合同。
#[cfg(feature = "web")]
type ManagedHttpStartRequest = api::SagaApiStart;

/// 业务作用：承载管理写入口必须进入同事务审计的稳定操作身份与人工原因。
#[cfg(feature = "web")]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedHttpAdminRequest {
    operation_id: String,
    reason: String,
    expected_state_version: Option<u64>,
    expected_control_version: Option<u64>,
}

/// 业务作用：承载 HTTP definition 生命周期动作的事务内 seal 前置条件、幂等身份和审计原因。
#[cfg(feature = "web")]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedHttpDefinitionOperationRequest {
    expected_sha256: String,
    operation_id: String,
    reason: String,
}

/// 业务作用：让 HTTP DTO 使用唯一业务查询合同。
#[cfg(feature = "web")]
type ManagedHttpQueryRequest = api::SagaApiQuery;

/// 业务作用：承载签名 HTTP body 中的审计页大小与不透明继续 token。
#[cfg(feature = "web")]
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ManagedHttpAuditRequest {
    page_size: Option<u32>,
    page_token: Option<String>,
}

/// 业务作用：把统一审计事实编码成 HTTP JSON 记录，同时保留数据库时间和 epoch 毫秒。
///
/// 参数说明：`record` 是运行时已完成租户门禁和 keyset 排序的一条事实。
///
/// 返回：返回带稳定类别、身份和发生时间的 JSON 值。
#[cfg(feature = "web")]
fn managed_http_audit_record(record: nasaga_runtime::SagaAuditRecord) -> serde_json::Value {
    match record {
        nasaga_runtime::SagaAuditRecord::Attempt(row) => serde_json::json!({
            "audit_id": format!(
                "attempt:{}:{}:{}",
                row.attempt.step.as_str(),
                row.attempt.phase.as_str(),
                row.attempt.attempt.get()
            ),
            "kind": "attempt",
            "occurred_at": row.occurred_at,
            "occurred_at_ms": row.occurred_at_ms,
            "details": {
                "step": row.attempt.step.as_str(),
                "phase": row.attempt.phase.as_str(),
                "attempt": row.attempt.attempt.get(),
                "effect_id": row.attempt.effect_id,
                "command_id": row.attempt.command_id,
                "status": row.attempt.status.as_str(),
                "outcome_event_id": row.attempt.outcome_event_id,
            },
        }),
        nasaga_runtime::SagaAuditRecord::Transition(row) => serde_json::json!({
            "audit_id": format!("transition:{}", row.transition_seq),
            "kind": "transition",
            "state_version": row.transition_seq,
            "occurred_at": row.occurred_at,
            "occurred_at_ms": row.occurred_at_ms,
            "details": {
                "from_state": row.from_state,
                "to_state": row.to_state,
                "trigger_kind": row.trigger_kind,
                "trigger_id": row.trigger_id,
                "definition_version": row.definition_version,
            },
        }),
        nasaga_runtime::SagaAuditRecord::Control(row) => serde_json::json!({
            "audit_id": format!("control:{}", row.control_seq),
            "kind": "control",
            "state_version": row.control_seq,
            "actor": row.actor,
            "reason": row.reason,
            "occurred_at": row.occurred_at,
            "occurred_at_ms": row.occurred_at_ms,
            "details": {
                "from_state": row.from_state,
                "to_state": row.to_state,
                "operation_id": row.operation_id,
            },
        }),
        nasaga_runtime::SagaAuditRecord::Management(row) => serde_json::json!({
            "audit_id": format!("management:{}", row.operation_id),
            "kind": "management",
            "actor": row.actor,
            "reason": row.reason,
            "occurred_at": row.occurred_at,
            "occurred_at_ms": row.occurred_at_ms,
            "details": {
                "operation_id": row.operation_id,
                "action": row.action,
            },
        }),
        nasaga_runtime::SagaAuditRecord::Conflict(row) => serde_json::json!({
            "audit_id": format!("conflict:{}", row.incoming_event_id),
            "kind": "conflict",
            "reason": row.conflict_kind,
            "occurred_at": row.occurred_at,
            "occurred_at_ms": row.occurred_at_ms,
            "details": {
                "incoming_event_id": row.incoming_event_id,
                "step": row.step.as_str(),
                "phase": row.phase.as_str(),
                "attempt": row.attempt.get(),
                "existing_status": row.existing_status.as_str(),
                "incoming_status": row.incoming_status.as_str(),
            },
        }),
    }
}

/// 业务作用：区分 HTTP command、result 与业务 API 三个互不继承权限的签名平面。
#[cfg(feature = "web")]
enum ManagedHttpAuthPlane {
    Command,
    Result,
    Api,
}

/// 业务作用：校验 Saga HTTP 请求的原始 path/body 签名并在共享数据库原子占用 nonce。
///
/// 参数说明：
/// - `server`: Ready 时冻结的角色、凭据与共享 replay 数据源。
/// - `plane`: 当前固定路由所属权限平面。
/// - `path`: listener 实际观察到并参与签名的规范路径。
/// - `headers`: 携带 producer、时间、nonce 与签名的请求头。
/// - `body`: 未经 JSON 重编码的原始请求体。
///
/// 返回：身份、时间窗、HMAC 与共享 nonce 首次占用均成功时返回 producer；其它情况脱敏拒绝。
#[cfg(feature = "web")]
async fn authenticate_managed_http_request(
    server: &ManagedHttpServer,
    plane: ManagedHttpAuthPlane,
    path: &str,
    headers: &axum::http::HeaderMap,
    body: &[u8],
) -> Result<nasaga_runtime::ServiceIdentity, axum::http::StatusCode> {
    let header = |name: &'static str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
    };
    let producer = nasaga_runtime::ServiceIdentity::new(
        header("x-saga-producer").ok_or(axum::http::StatusCode::UNAUTHORIZED)?,
    )
    .map_err(|_| axum::http::StatusCode::UNAUTHORIZED)?;
    let timestamp_ms = header("x-saga-timestamp")
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or(axum::http::StatusCode::UNAUTHORIZED)?;
    let nonce = header("x-saga-nonce").ok_or(axum::http::StatusCode::UNAUTHORIZED)?;
    let signature = header("x-saga-signature").ok_or(axum::http::StatusCode::UNAUTHORIZED)?;
    let authenticator = match plane {
        ManagedHttpAuthPlane::Command => server.command_authenticators.get(&producer),
        ManagedHttpAuthPlane::Result => server.result_authenticators.get(&producer),
        ManagedHttpAuthPlane::Api => server
            .api_callers
            .get(&producer)
            .map(|caller| &caller.authenticator),
    }
    .ok_or(axum::http::StatusCode::UNAUTHORIZED)?;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| axum::http::StatusCode::SERVICE_UNAVAILABLE)?
        .as_millis();
    let now_ms = u64::try_from(now_ms).map_err(|_| axum::http::StatusCode::SERVICE_UNAVAILABLE)?;
    let expires_at_ms = authenticator
        .verify_replay_horizon_ms(
            &nasaga_runtime::SagaHttpSignedMessage::new(
                &producer,
                path,
                timestamp_ms,
                nonce,
                body,
                signature,
            ),
            now_ms,
        )
        .map_err(|_| axum::http::StatusCode::UNAUTHORIZED)?;
    let now_i64 = i64::try_from(now_ms).map_err(|_| axum::http::StatusCode::SERVICE_UNAVAILABLE)?;
    let expires_at_ms =
        i64::try_from(expires_at_ms).map_err(|_| axum::http::StatusCode::SERVICE_UNAVAILABLE)?;
    let claimed = match server.driver {
        natx_core::DatabaseDriver::MySql => {
            #[cfg(feature = "saga")]
            {
                nasaga_runtime::claim_http_replay_for(
                    &server.datasource,
                    producer.as_str(),
                    nonce,
                    now_i64,
                    expires_at_ms,
                )
                .await
            }
            #[cfg(not(feature = "saga"))]
            Err(anyhow::anyhow!("MySQL Saga runtime is unavailable"))
        }
        natx_core::DatabaseDriver::PostgreSql => {
            #[cfg(feature = "saga-pgsql")]
            {
                nasaga_runtime_pgsql::claim_http_replay_for(
                    &server.datasource,
                    producer.as_str(),
                    nonce,
                    now_i64,
                    expires_at_ms,
                )
                .await
            }
            #[cfg(not(feature = "saga-pgsql"))]
            Err(anyhow::anyhow!("PostgreSQL Saga runtime is unavailable"))
        }
    }
    .map_err(|_| axum::http::StatusCode::SERVICE_UNAVAILABLE)?;
    if !claimed {
        return Err(axum::http::StatusCode::CONFLICT);
    }
    Ok(producer)
}

/// 业务作用：确认已认证 API 主体在指定租户拥有当前标准操作权限。
///
/// 参数说明：`server` 是冻结政策，`producer` 是验签主体，`tenant` 是请求目标，`permission` 是封闭操作名。
///
/// 返回：主体同时命中租户与权限时成功；其它情况统一返回禁止访问。
#[cfg(feature = "web")]
fn authorize_managed_http_api(
    server: &ManagedHttpServer,
    producer: &nasaga_runtime::ServiceIdentity,
    tenant: &str,
    permission: &str,
) -> Result<(), axum::http::StatusCode> {
    let caller = server
        .api_callers
        .get(producer)
        .ok_or(axum::http::StatusCode::FORBIDDEN)?;
    SagaOrchestratorApi::authorize(&caller.actor, tenant, permission)
        .map_err(|_| axum::http::StatusCode::FORBIDDEN)
}

/// 业务作用：把 HTTP registry 权限同时绑定到定义租户和 workflow，避免首次发布依赖请求自报 owner。
///
/// 参数说明：`server` 与 `producer` 定位冻结主体，`tenant`/`workflow` 是完整授权目标。
///
/// 返回：租户、registry 权限和 workflow grant 同时命中时成功；其它情况拒绝访问。
#[cfg(feature = "web")]
fn authorize_managed_http_registry(
    server: &ManagedHttpServer,
    producer: &nasaga_runtime::ServiceIdentity,
    tenant: &str,
    workflow: &str,
) -> Result<(), axum::http::StatusCode> {
    let caller = server
        .api_callers
        .get(producer)
        .ok_or(axum::http::StatusCode::FORBIDDEN)?;
    SagaOrchestratorApi::authorize_registry(&caller.actor, tenant, workflow)
        .map_err(|_| axum::http::StatusCode::FORBIDDEN)
}

/// 业务作用：从可选 `traceparent` 请求头解析显式链路上下文，非法值按无上下文处理而不改变业务裁决。
///
/// 参数说明：`headers` 是当前 Saga 专用请求头集合。
///
/// 返回：规范 W3C 上下文存在时返回解析值，否则为空。
#[cfg(feature = "web")]
fn managed_http_trace(headers: &axum::http::HeaderMap) -> Option<nasaga_runtime::TraceContext> {
    headers
        .get("traceparent")
        .and_then(|value| value.to_str().ok())
        .and_then(nasaga_runtime::TraceContext::parse_traceparent)
}

/// 业务作用：生成 transport 发送端唯一认可的标准持久收据 JSON。
///
/// 参数说明：`status` 是封闭的提交裁决名。
///
/// 返回：固定 content-type 与不含内部错误的 JSON 响应。
#[cfg(feature = "web")]
fn managed_http_receipt(status: &'static str) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    axum::Json(serde_json::json!({ "status": status })).into_response()
}

/// 业务作用：处理参与方 command 入站，只有本地 Inbox、gate、业务事实和结果 Outbox 提交后返回成功收据。
///
/// 参数说明：`application` 提供 Ready runtime，`uri/headers/body` 是未经业务中间件改写的请求事实。
///
/// 返回：提交或重复返回标准收据；认证、合同或事务失败返回封闭 HTTP 状态且不虚假确认。
#[cfg(feature = "web")]
async fn managed_http_command(
    axum::extract::State(application): axum::extract::State<Application>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let state = application.saga_runtime();
    if state.ensure_ready().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Some(server) = state.http_server() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let producer = match authenticate_managed_http_request(
        &server,
        ManagedHttpAuthPlane::Command,
        uri.path(),
        &headers,
        &body,
    )
    .await
    {
        Ok(producer) => producer,
        Err(status) => return status.into_response(),
    };
    // 共享 replay claim 可能等待数据库；认证完成后复验当前资格，旧入口检查不能跨越等待生效。
    if state.ensure_ready().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let envelope: nasaga_runtime::SagaCommandEnvelope = match serde_json::from_slice(&body) {
        Ok(envelope) => envelope,
        Err(_) => return axum::http::StatusCode::BAD_REQUEST.into_response(),
    };
    if headers
        .get("x-saga-event-id")
        .and_then(|value| value.to_str().ok())
        != Some(envelope.command_id.as_str())
    {
        return axum::http::StatusCode::UNPROCESSABLE_ENTITY.into_response();
    }
    let Some(handler) = state.command_handler(&envelope) else {
        return axum::http::StatusCode::UNPROCESSABLE_ENTITY.into_response();
    };
    match handler
        .handle(&envelope, &producer, managed_http_trace(&headers).as_ref())
        .await
    {
        Ok(nasaga_runtime::ParticipantHandled::Duplicate) => managed_http_receipt("Duplicate"),
        Ok(_) => managed_http_receipt("Committed"),
        Err(error) => match nasaga_runtime::classify_command_delivery_error(&error) {
            nasaga_runtime::CommandDeliveryDisposition::DeadLetter => {
                let mut response = managed_http_receipt("DeterministicReject");
                *response.status_mut() = axum::http::StatusCode::UNPROCESSABLE_ENTITY;
                response
            }
            _ => axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response(),
        },
    }
}

/// 业务作用：处理 Orchestrator result 入站，producer owner、Inbox 与状态推进全部成功后才确认。
///
/// 参数说明：`application` 提供唯一 Orchestrator，`uri/headers/body` 是签名覆盖的原始请求事实。
///
/// 返回：应用或重复返回标准收据；身份、合同或本地提交不明时不返回成功。
#[cfg(feature = "web")]
async fn managed_http_result(
    axum::extract::State(application): axum::extract::State<Application>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let state = application.saga_runtime();
    let permit = match state.result_permit() {
        Ok(permit) => permit,
        Err(_) => return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    let Some(server) = state.http_server() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let producer = match authenticate_managed_http_request(
        &server,
        ManagedHttpAuthPlane::Result,
        uri.path(),
        &headers,
        &body,
    )
    .await
    {
        Ok(producer) => producer,
        Err(status) => return status.into_response(),
    };
    // 共享 replay claim 可能等待数据库；认证后复验同一次资格，不能借等待期间的新确认重新准入。
    if state.ensure_running().is_err() || !permit.is_valid() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let envelope: nasaga_runtime::SagaResultEnvelope = match serde_json::from_slice(&body) {
        Ok(envelope) => envelope,
        Err(_) => return axum::http::StatusCode::BAD_REQUEST.into_response(),
    };
    if headers
        .get("x-saga-event-id")
        .and_then(|value| value.to_str().ok())
        != Some(envelope.event_id.as_str())
    {
        return axum::http::StatusCode::UNPROCESSABLE_ENTITY.into_response();
    }
    let Some(orchestrator) = state.managed_orchestrator() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let now_ms = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(value) => i64::try_from(value.as_millis()).unwrap_or(i64::MAX),
        Err(_) => return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    match orchestrator
        .handle_result(
            &envelope,
            &producer,
            managed_http_trace(&headers).as_ref(),
            now_ms,
            &state,
            &permit,
        )
        .await
    {
        Ok(nasaga_runtime::HandleOutcome::Duplicate) => managed_http_receipt("Duplicate"),
        Ok(_) => managed_http_receipt("Committed"),
        Err(error) => match nasaga_runtime::classify_result_delivery_error(&error) {
            nasaga_runtime::ResultDeliveryDisposition::DeadLetter => {
                let mut response = managed_http_receipt("DeterministicReject");
                *response.status_mut() = axum::http::StatusCode::UNPROCESSABLE_ENTITY;
                response
            }
            _ => axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response(),
        },
    }
}

/// 业务作用：把已提交 Saga 实例投影成不携带业务 payload 的稳定 HTTP 快照。
///
/// 参数说明：`row` 是数据库返回的已提交实例事实。
///
/// 返回：包含身份、冻结 definition、状态与 CAS 版本的 JSON 值。
#[cfg(feature = "web")]
fn managed_http_instance_json(row: &nasaga_runtime::SagaInstanceRow) -> serde_json::Value {
    serde_json::json!({
        "tenant_id": row.tenant.as_str(),
        "saga_id": row.saga_id.as_str(),
        "workflow": row.workflow.as_str(),
        "definition_version": row.definition_version.get(),
        "definition_digest": row.definition_digest,
        "business_key": row.business_key.as_str(),
        "status": row.status.as_str(),
        "control_state": row.control_state.as_str(),
        "direction": row.direction.as_str(),
        "current_step": row.current_step.as_ref().map(|step| step.as_str()),
        "state_version": row.version,
        "control_version": row.control_version,
        "deadline_at_ms": row.deadline_at_ms,
        "failure_code": row.failure_code,
        "traceparent": row.traceparent,
    })
}

/// 业务作用：通过标准 HTTP API 发起 Saga，并复用运行核心的创建幂等与原子首步事务。
///
/// 参数说明：`application` 提供唯一 Orchestrator，`uri/headers/body` 是独立 Saga 安全链的请求事实。
///
/// 返回：创建或幂等命中返回标准收据与实例快照；认证、授权、合同或提交失败返回脱敏状态。
#[cfg(feature = "web")]
async fn managed_http_start(
    axum::extract::State(application): axum::extract::State<Application>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let state = application.saga_runtime();
    if state.ensure_ready().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Some(server) = state.http_server() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let producer = match authenticate_managed_http_request(
        &server,
        ManagedHttpAuthPlane::Api,
        uri.path(),
        &headers,
        &body,
    )
    .await
    {
        Ok(producer) => producer,
        Err(status) => return status.into_response(),
    };
    // 共享 replay claim 可能等待数据库；认证完成后复验当前资格，旧入口检查不能跨越等待生效。
    if state.ensure_ready().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let request: ManagedHttpStartRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return axum::http::StatusCode::BAD_REQUEST.into_response(),
    };
    if authorize_managed_http_api(&server, &producer, &request.tenant_id, "start").is_err() {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }
    let Some(orchestrator) = state.managed_orchestrator() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let Some(caller) = server.api_callers.get(&producer) else {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    };
    let outcome = orchestrator
        .start_instance(
            &caller.actor,
            request,
            managed_http_trace(&headers).as_ref(),
            &state,
        )
        .await;
    match outcome {
        Ok(nasaga_runtime::StartOutcome::Started(row)) => axum::Json(serde_json::json!({
            "status": "Committed",
            "request_digest": row.start_request_digest.clone().unwrap_or_default(),
            "saga": managed_http_instance_json(&row),
        }))
        .into_response(),
        Ok(nasaga_runtime::StartOutcome::AlreadyExists(row)) => axum::Json(serde_json::json!({
            "status": "Duplicate",
            "request_digest": row.start_request_digest.clone().unwrap_or_default(),
            "saga": managed_http_instance_json(&row),
        }))
        .into_response(),
        Err(error) => error.http_status().into_response(),
    }
}

/// 业务作用：通过标准 HTTP API 读取同租户 Saga 快照，不泄漏其它租户实例存在性。
///
/// 参数说明：路径提供租户与实例身份，签名主体必须同时拥有该租户的 `read` 权限。
///
/// 返回：命中返回快照，不存在与跨租户统一为 404；认证、授权或数据库失败返回脱敏状态。
#[cfg(feature = "web")]
async fn managed_http_get_instance(
    axum::extract::State(application): axum::extract::State<Application>,
    axum::extract::Path((tenant_text, saga_text)): axum::extract::Path<(String, String)>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let state = application.saga_runtime();
    if state.ensure_ready().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Some(server) = state.http_server() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let producer = match authenticate_managed_http_request(
        &server,
        ManagedHttpAuthPlane::Api,
        uri.path(),
        &headers,
        &body,
    )
    .await
    {
        Ok(producer) => producer,
        Err(status) => return status.into_response(),
    };
    // 共享 replay claim 可能等待数据库；认证完成后复验当前资格，旧入口检查不能跨越等待生效。
    if state.ensure_ready().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if authorize_managed_http_api(&server, &producer, &tenant_text, "read").is_err() {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }
    let Some(orchestrator) = state.managed_orchestrator() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let Some(caller) = server.api_callers.get(&producer) else {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    };
    match orchestrator
        .get_instance(&caller.actor, &tenant_text, &saga_text)
        .await
    {
        Ok(row) => axum::Json(managed_http_instance_json(&row)).into_response(),
        Err(error) => error.http_status().into_response(),
    }
}

/// 业务作用：执行一个标准 Saga 管理动作，并在成功后回读同租户实例快照。
///
/// 参数说明：`action` 是固定路由映射，路径定位租户和实例，body 提供审计 operation 与 reason。
///
/// 返回：动作提交返回最新快照；认证、授权、状态、CAS 或数据库失败返回脱敏状态。
#[cfg(feature = "web")]
async fn execute_managed_http_admin(
    application: Application,
    action: ManagedAdminAction,
    tenant_text: String,
    saga_text: String,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let state = application.saga_runtime();
    if state.ensure_ready().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Some(server) = state.http_server() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    if !server.expose_admin {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    }
    let producer = match authenticate_managed_http_request(
        &server,
        ManagedHttpAuthPlane::Api,
        uri.path(),
        &headers,
        &body,
    )
    .await
    {
        Ok(producer) => producer,
        Err(status) => return status.into_response(),
    };
    // 共享 replay claim 可能等待数据库；认证完成后复验当前资格，旧入口检查不能跨越等待生效。
    if state.ensure_ready().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if authorize_managed_http_api(&server, &producer, &tenant_text, "admin").is_err() {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }
    let request: ManagedHttpAdminRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return axum::http::StatusCode::BAD_REQUEST.into_response(),
    };
    let Some(orchestrator) = state.managed_orchestrator() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let Some(caller) = server.api_callers.get(&producer) else {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    };
    match orchestrator
        .administer_instance(
            &caller.actor,
            action,
            api::SagaApiAdmin {
                tenant_id: tenant_text,
                saga_id: saga_text,
                operation_id: request.operation_id,
                reason: request.reason,
                expected_state_version: request.expected_state_version,
                expected_control_version: request.expected_control_version,
            },
        )
        .await
    {
        Ok(row) => axum::Json(managed_http_instance_json(&row)).into_response(),
        Err(error) => error.http_status().into_response(),
    }
}

#[cfg(feature = "web")]
macro_rules! managed_admin_handler {
    ($name:ident, $action:expr, $purpose:literal) => {
        #[doc = $purpose]
        ///
        /// 参数说明：路径定位租户和实例，headers/body 携带受签名身份与审计字段。
        ///
        /// 返回：动作与审计同事务提交后返回最新实例；拒绝或失败返回脱敏 HTTP 状态。
        async fn $name(
            axum::extract::State(application): axum::extract::State<Application>,
            axum::extract::Path((tenant, saga_id)): axum::extract::Path<(String, String)>,
            axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
            headers: axum::http::HeaderMap,
            body: axum::body::Bytes,
        ) -> axum::response::Response {
            execute_managed_http_admin(application, $action, tenant, saga_id, uri, headers, body)
                .await
        }
    };
}

#[cfg(feature = "web")]
managed_admin_handler!(
    managed_http_pause,
    ManagedAdminAction::Pause,
    "业务作用：暂停实例的自动推进，并把认证主体、原因和 operation id 与控制态切换同事务提交。"
);
#[cfg(feature = "web")]
managed_admin_handler!(
    managed_http_resume,
    ManagedAdminAction::Resume,
    "业务作用：恢复实例的自动推进，不重置已经过去的业务 deadline。"
);
#[cfg(feature = "web")]
managed_admin_handler!(
    managed_http_retry_compensation,
    ManagedAdminAction::RetryCompensation,
    "业务作用：在人工介入后沿已冻结逆序计划重试补偿，不重新计算业务顺序。"
);
#[cfg(feature = "web")]
managed_admin_handler!(
    managed_http_retry_resolution,
    ManagedAdminAction::RetryResolution,
    "业务作用：在人工介入后为 Unknown 效果开启一次受审计的新 resolve 周期。"
);
#[cfg(feature = "web")]
managed_admin_handler!(
    managed_http_manual_close,
    ManagedAdminAction::ManualClose,
    "业务作用：记录系统外处置后关闭自动化，不伪造成功或已补偿业务事实。"
);

/// 业务作用：通过标准 HTTP API 执行租户受限、状态可筛选且使用 keyset 的有界实例检索。
///
/// 参数说明：签名 body 提供过滤条件，调用主体必须拥有目标租户的 `read` 权限。
///
/// 返回：成功返回低敏摘要数组；认证、授权、参数或数据库失败返回脱敏状态。
#[cfg(feature = "web")]
async fn managed_http_query_instances(
    axum::extract::State(application): axum::extract::State<Application>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let state = application.saga_runtime();
    if state.ensure_ready().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Some(server) = state.http_server() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let producer = match authenticate_managed_http_request(
        &server,
        ManagedHttpAuthPlane::Api,
        uri.path(),
        &headers,
        &body,
    )
    .await
    {
        Ok(producer) => producer,
        Err(status) => return status.into_response(),
    };
    // 共享 replay claim 可能等待数据库；认证完成后复验当前资格，旧入口检查不能跨越等待生效。
    if state.ensure_ready().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let request: ManagedHttpQueryRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return axum::http::StatusCode::BAD_REQUEST.into_response(),
    };
    if authorize_managed_http_api(&server, &producer, &request.tenant_id, "read").is_err() {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }
    let Some(orchestrator) = state.managed_orchestrator() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let Some(caller) = server.api_callers.get(&producer) else {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    };
    let page = match orchestrator
        .query_instances(&caller.actor, request, &server.page_token_key)
        .await
    {
        Ok(page) => page,
        Err(ManagedApiFailure::Invalid) => {
            return axum::http::StatusCode::BAD_REQUEST.into_response()
        }
        Err(ManagedApiFailure::PermissionDenied) => {
            return axum::http::StatusCode::FORBIDDEN.into_response()
        }
        Err(_) => return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    let rows = page.rows;
    let next_page_token = page.next_page_token;
    {
        let rows = rows
            .into_iter()
            .map(|row| {
                serde_json::json!({
                    "tenant_id": row.tenant.as_str(),
                    "saga_id": row.saga_id.as_str(),
                    "workflow": row.workflow.as_str(),
                    "definition_version": row.definition_version.get(),
                    "definition_digest": row.definition_digest,
                    "control_version": row.control_version,
                    "deadline_at_ms": row.deadline_at_ms,
                    "traceparent": row.traceparent,
                    "business_key": row.business_key.as_str(),
                    "status": row.status.as_str(),
                    "control_state": row.control_state.as_str(),
                    "direction": row.direction.as_str(),
                    "current_step": row.current_step.as_ref().map(|step| step.as_str()),
                    "state_version": row.version,
                    "failure_code": row.failure_code,
                    "created_at_ms": row.created_at_ms,
                    "updated_at_ms": row.updated_at_ms,
                })
            })
            .collect::<Vec<_>>();
        axum::Json(serde_json::json!({
            "sagas": rows,
            "next_page_token": next_page_token,
        }))
        .into_response()
    }
}

/// 业务作用：通过标准 HTTP API 返回实例的有界审计链，且先鉴权再检查实例存在性。
///
/// 参数说明：路径提供租户与实例身份，签名主体必须拥有目标租户的 `audit` 权限。
///
/// 返回：成功返回 attempt、迁移、控制、管理与冲突事实；越权、不存在或数据库失败均脱敏。
#[cfg(feature = "web")]
async fn managed_http_audit(
    axum::extract::State(application): axum::extract::State<Application>,
    axum::extract::Path((tenant_text, saga_text)): axum::extract::Path<(String, String)>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let state = application.saga_runtime();
    if state.ensure_ready().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Some(server) = state.http_server() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let producer = match authenticate_managed_http_request(
        &server,
        ManagedHttpAuthPlane::Api,
        uri.path(),
        &headers,
        &body,
    )
    .await
    {
        Ok(producer) => producer,
        Err(status) => return status.into_response(),
    };
    // 重放声明可能等待数据库，审计读取同样不能越过停机或 Catalog 失权边界。
    if state.ensure_ready().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if authorize_managed_http_api(&server, &producer, &tenant_text, "audit").is_err() {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }
    let request = if body.is_empty() {
        ManagedHttpAuditRequest::default()
    } else {
        match serde_json::from_slice::<ManagedHttpAuditRequest>(&body) {
            Ok(value) => value,
            Err(_) => return axum::http::StatusCode::BAD_REQUEST.into_response(),
        }
    };
    let Some(orchestrator) = state.managed_orchestrator() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let Some(caller) = server.api_callers.get(&producer) else {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    };
    match orchestrator.query_audit(&caller.actor, api::SagaApiAudit {tenant_id: tenant_text, saga_id: saga_text, page_size: request.page_size, page_token: request.page_token}, &server.page_token_key).await {
        Ok(page) => axum::Json(serde_json::json!({"records":page.records.into_iter().map(managed_http_audit_record).collect::<Vec<_>>(),"next_page_token":page.next_page_token})).into_response(),
        Err(error) => error.http_status().into_response(),
    }
}

/// 业务作用：输出不含 tenant、payload 和 saga_id 标签的 Saga 指标，供控制面容量与积压观测。
///
/// 参数说明：`application` 提供唯一 Orchestrator；指标端点不读取业务请求体。
///
/// 返回：数据库聚合成功返回 Prometheus 文本；角色不符或查询失败返回对应状态。
#[cfg(feature = "web")]
async fn managed_http_metrics(
    axum::extract::State(application): axum::extract::State<Application>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let state = application.saga_runtime();
    let Some(orchestrator) = state.managed_orchestrator() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let now_ms = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(value) => i64::try_from(value.as_millis()).unwrap_or(i64::MAX),
        Err(_) => return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    match orchestrator.operational_metrics(now_ms).await {
        Ok(metrics) => (
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            metrics.render_prometheus(),
        )
            .into_response(),
        Err(_) => axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

/// 业务作用：通过同一 Registry 授权和存储查询读取不可变 definition 及当前生命周期。
/// 参数说明：路径定位完整定义键，`headers/body/uri` 是 HTTP 认证边界观察到的原始请求。
/// 返回：命中返回封签内容与生命周期；越权、非法键、不存在或存储失败返回统一 HTTP 状态。
#[cfg(feature = "web")]
async fn managed_http_get_definition(
    axum::extract::State(application): axum::extract::State<Application>,
    axum::extract::Path((tenant, workflow, version)): axum::extract::Path<(String, String, u32)>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let state = application.saga_runtime();
    // Registry 必须在能力租约缺失时仍可恢复或退休；运行状态、认证和动作自己的事务门禁继续生效。
    if state.ensure_running().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Some(server) = state.http_server() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    if !server.expose_definition_registry {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    }
    let producer = match authenticate_managed_http_request(
        &server,
        ManagedHttpAuthPlane::Api,
        uri.path(),
        &headers,
        &body,
    )
    .await
    {
        Ok(producer) => producer,
        Err(status) => return status.into_response(),
    };
    // nonce 声明等待结束后再次确认读取资格，不能跨越停机或安全材料切换。
    if state.ensure_running().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Some(runtime) = state.managed_orchestrator() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let Some(caller) = server.api_callers.get(&producer) else {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    };
    match runtime
        .get_definition_record(&caller.actor, &tenant, &workflow, version)
        .await
    {
        Ok(record) => axum::Json(record).into_response(),
        Err(error) => error.http_status().into_response(),
    }
}

/// 业务作用：登记或续租一个已认证 owner 的逐实例 capability，不能由请求字段冒充其它服务。
///
/// 参数说明：Application 提供共享 Catalog，`uri/headers/body` 是 Saga 安全链读取的原始请求。
///
/// 返回：运行期间的持久租约收据，Catalog 摘流不阻止合法续租；认证、授权、合同、停机或数据库失败返回脱敏 HTTP 状态。
#[cfg(feature = "web")]
async fn managed_http_register_capability(
    axum::extract::State(application): axum::extract::State<Application>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let state = application.saga_runtime();
    // capability 自然到期时业务会摘流，但合法续租必须仍能进入完整认证链恢复路由。
    if state.ensure_running().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Some(server) = state.http_server() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    if !server.expose_definition_registry {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    }
    let producer = match authenticate_managed_http_request(
        &server,
        ManagedHttpAuthPlane::Api,
        uri.path(),
        &headers,
        &body,
    )
    .await
    {
        Ok(value) => value,
        Err(status) => return status.into_response(),
    };
    // 共享 replay claim 可能等待数据库；认证完成后复验当前资格，旧入口检查不能跨越等待生效。
    if state.ensure_running().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let descriptor: nasaga_runtime::CapabilityDescriptor = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return axum::http::StatusCode::BAD_REQUEST.into_response(),
    };
    let Some(caller) = server.api_callers.get(&producer) else {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    };
    let Some(runtime) = state.managed_orchestrator() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    match runtime
        .register_capability_record(&caller.actor, &server.activation, &descriptor)
        .await
    {
        Ok(receipt) => axum::Json(receipt).into_response(),
        Err(error) => managed_http_catalog_status(&error).into_response(),
    }
}

/// 业务作用：持久化 workflow owner 提交的完整 definition seal，候选激活由 Catalog watcher 统一裁决。
///
/// 参数说明：Application 提供共享 Catalog，`uri/headers/body` 是 Saga 安全链读取的原始请求。
///
/// 返回：首次或幂等发布记录；越权、同键异摘要或非法流程返回脱敏状态。
#[cfg(feature = "web")]
async fn managed_http_publish_definition(
    axum::extract::State(application): axum::extract::State<Application>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let state = application.saga_runtime();
    // Registry 必须在能力租约缺失时仍可恢复或退休；运行状态、认证和动作自己的事务门禁继续生效。
    if state.ensure_running().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Some(server) = state.http_server() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    if !server.expose_definition_registry {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    }
    let producer = match authenticate_managed_http_request(
        &server,
        ManagedHttpAuthPlane::Api,
        uri.path(),
        &headers,
        &body,
    )
    .await
    {
        Ok(value) => value,
        Err(status) => return status.into_response(),
    };
    // 共享 replay claim 可能等待数据库；认证完成后复验当前资格，旧入口检查不能跨越等待生效。
    if state.ensure_running().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let signed: ManagedSignedDefinitionArtifact = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return axum::http::StatusCode::BAD_REQUEST.into_response(),
    };
    let Some(caller) = server.api_callers.get(&producer) else {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    };
    let Some(runtime) = state.managed_orchestrator() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let artifact = match SagaOrchestratorApi::validate_signed_definition(
        &caller.actor,
        &server.definition_signing_keys,
        &signed,
    ) {
        Ok(artifact) => artifact,
        Err(error) => return managed_http_catalog_status(&error).into_response(),
    };
    let (_, record) = match runtime
        .publish_definition_record(&caller.actor, &artifact)
        .await
    {
        Ok(value) => value,
        Err(error) => return managed_http_catalog_status(&error).into_response(),
    };
    axum::Json(record).into_response()
}

/// 业务作用：由获权 workflow owner 显式激活一个已满足 capability 门禁的 candidate。
///
/// 参数说明：路径定位 definition，签名请求体携带 expected seal、操作身份与审计原因。
///
/// 返回：active 记录；认证、授权、状态或能力缺口返回脱敏状态。
#[cfg(feature = "web")]
async fn managed_http_activate_definition(
    axum::extract::State(application): axum::extract::State<Application>,
    axum::extract::Path((tenant, workflow, version)): axum::extract::Path<(String, String, u32)>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    managed_http_change_definition(
        application,
        tenant,
        workflow,
        version,
        uri,
        headers,
        body,
        nasaga_runtime::DefinitionLifecycle::Active,
    )
    .await
}

/// 业务作用：由获权 workflow owner 将 active definition 标记为 deprecated，不打断在途实例。
///
/// 参数说明：路径定位 definition，签名请求体携带 expected seal、操作身份与审计原因。
///
/// 返回：deprecated 记录；认证、授权或状态非法返回脱敏状态。
#[cfg(feature = "web")]
async fn managed_http_deprecate_definition(
    axum::extract::State(application): axum::extract::State<Application>,
    axum::extract::Path((tenant, workflow, version)): axum::extract::Path<(String, String, u32)>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    managed_http_change_definition(
        application,
        tenant,
        workflow,
        version,
        uri,
        headers,
        body,
        nasaga_runtime::DefinitionLifecycle::Deprecated,
    )
    .await
}

/// 业务作用：由获权 workflow owner 退休已无持久引用的 deprecated definition。
///
/// 参数说明：路径定位 definition，签名请求体携带 expected seal、操作身份与审计原因。
///
/// 返回：retired 记录；认证、授权或状态非法返回脱敏状态。
#[cfg(feature = "web")]
async fn managed_http_retire_definition(
    axum::extract::State(application): axum::extract::State<Application>,
    axum::extract::Path((tenant, workflow, version)): axum::extract::Path<(String, String, u32)>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    managed_http_change_definition(
        application,
        tenant,
        workflow,
        version,
        uri,
        headers,
        body,
        nasaga_runtime::DefinitionLifecycle::Retired,
    )
    .await
}

/// 业务作用：复用同一认证、授权和后端适配执行 definition 生命周期迁移。
///
/// 参数说明：Application、定义键、原始请求与 `target` 封闭选择共同描述控制面操作。
///
/// 返回：生命周期记录；合同非法、越权、状态或数据库失败返回脱敏状态。
#[cfg(feature = "web")]
#[allow(clippy::too_many_arguments)]
async fn managed_http_change_definition(
    application: Application,
    tenant: String,
    workflow: String,
    version: u32,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
    target: nasaga_runtime::DefinitionLifecycle,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let state = application.saga_runtime();
    // Registry 必须在能力租约缺失时仍可恢复或退休；运行状态、认证和动作自己的事务门禁继续生效。
    if state.ensure_running().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Some(server) = state.http_server() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    if !server.expose_definition_registry {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    }
    let producer = match authenticate_managed_http_request(
        &server,
        ManagedHttpAuthPlane::Api,
        uri.path(),
        &headers,
        &body,
    )
    .await
    {
        Ok(value) => value,
        Err(status) => return status.into_response(),
    };
    // 共享 replay claim 可能等待数据库；认证完成后复验当前资格，旧入口检查不能跨越等待生效。
    if state.ensure_running().is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if authorize_managed_http_registry(&server, &producer, &tenant, &workflow).is_err() {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }
    let request: ManagedHttpDefinitionOperationRequest = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return axum::http::StatusCode::BAD_REQUEST.into_response(),
    };
    let operation = match nasaga_runtime::DefinitionLifecycleOperation::new(
        request.expected_sha256,
        request.operation_id,
        request.reason,
    ) {
        Ok(value) => value,
        Err(_) => return axum::http::StatusCode::BAD_REQUEST.into_response(),
    };
    let Some(caller) = server.api_callers.get(&producer) else {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    };
    let Some(runtime) = state.managed_orchestrator() else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let Some(identity) = server.orchestrator_service_identity.as_ref() else {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match runtime
        .change_definition_record(
            &caller.actor,
            (&tenant, &workflow, version),
            target,
            &operation,
            identity,
            &server.activation,
        )
        .await
    {
        Ok(record) => axum::Json(record).into_response(),
        Err(error) => managed_http_catalog_status(&error).into_response(),
    }
}

/// 业务作用：让 HTTP 与 gRPC 共用 definition 的事务、CAS、幂等审计和退休门禁。
///
/// 参数说明：driver 与 datasource 定位权威，actor 是认证主体，key 与 target 定位生命周期动作，
/// operation 固定摘要和审计身份，activation_gate 只用于激活。
///
/// 返回：后端原子提交的记录；协议适配器统一映射领域拒绝，不改变错误类别。
#[cfg(any(feature = "web", feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
async fn apply_managed_definition_lifecycle(
    driver: natx_core::DatabaseDriver,
    datasource: &str,
    actor: &nasaga_runtime::ServiceIdentity,
    key: (&str, &str, u32),
    target: nasaga_runtime::DefinitionLifecycle,
    operation: &nasaga_runtime::DefinitionLifecycleOperation,
    activation_gate: Option<&nasaga_runtime::DefinitionActivationGate>,
) -> anyhow::Result<nasaga_runtime::DefinitionRecord> {
    let (tenant, workflow, version) = key;
    match driver {
        natx_core::DatabaseDriver::MySql => {
            #[cfg(feature = "saga")]
            {
                match target {
                    nasaga_runtime::DefinitionLifecycle::Active => {
                        nasaga_runtime::activate_definition_with_operation_for(
                            datasource,
                            actor,
                            activation_gate.ok_or(
                                nasaga_runtime::DefinitionCatalogError::FailedPrecondition,
                            )?,
                            tenant,
                            workflow,
                            version,
                            operation,
                        )
                        .await
                    }
                    nasaga_runtime::DefinitionLifecycle::Deprecated => {
                        nasaga_runtime::deprecate_definition_with_operation_for(
                            datasource, actor, tenant, workflow, version, operation,
                        )
                        .await
                    }
                    nasaga_runtime::DefinitionLifecycle::Retired => {
                        nasaga_runtime::retire_definition_with_operation_for(
                            datasource, actor, tenant, workflow, version, operation,
                        )
                        .await
                    }
                    nasaga_runtime::DefinitionLifecycle::Candidate => {
                        Err(nasaga_runtime::DefinitionCatalogError::FailedPrecondition.into())
                    }
                }
            }
            #[cfg(not(feature = "saga"))]
            anyhow::bail!("MySql Saga runtime is unavailable")
        }
        natx_core::DatabaseDriver::PostgreSql => {
            #[cfg(feature = "saga-pgsql")]
            {
                match target {
                    nasaga_runtime::DefinitionLifecycle::Active => {
                        nasaga_runtime_pgsql::activate_definition_with_operation_for(
                            datasource,
                            actor,
                            activation_gate.ok_or(
                                nasaga_runtime::DefinitionCatalogError::FailedPrecondition,
                            )?,
                            tenant,
                            workflow,
                            version,
                            operation,
                        )
                        .await
                    }
                    nasaga_runtime::DefinitionLifecycle::Deprecated => {
                        nasaga_runtime_pgsql::deprecate_definition_with_operation_for(
                            datasource, actor, tenant, workflow, version, operation,
                        )
                        .await
                    }
                    nasaga_runtime::DefinitionLifecycle::Retired => {
                        nasaga_runtime_pgsql::retire_definition_with_operation_for(
                            datasource, actor, tenant, workflow, version, operation,
                        )
                        .await
                    }
                    nasaga_runtime::DefinitionLifecycle::Candidate => {
                        Err(nasaga_runtime::DefinitionCatalogError::FailedPrecondition.into())
                    }
                }
            }
            #[cfg(not(feature = "saga-pgsql"))]
            anyhow::bail!("PostgreSql Saga runtime is unavailable")
        }
    }
}

/// 业务作用：让 Saga 保留前缀下的未知路径在专用分支内终止，禁止落入业务 fallback。
///
/// 参数说明: 无。
///
/// 返回：固定 404。
#[cfg(feature = "web")]
async fn managed_http_not_found() -> axum::http::StatusCode {
    axum::http::StatusCode::NOT_FOUND
}

/// 业务作用：构造已补齐 Application 状态的独立 Saga Router，供 Web 在进入业务 middleware 前按保留前缀分派。
///
/// 参数说明：`application` 提供 Ready Saga 状态，`context_path` 是 listener 已冻结的应用前缀。
///
/// 返回：未启用 HTTP 入站时为空；启用时返回唯一有效前缀和自带 body 与并发上限的 Router。
#[cfg(feature = "web")]
pub(crate) fn managed_http_router(
    application: &Application,
    context_path: &str,
) -> ApplicationResult<Option<(String, axum::Router)>> {
    use axum::routing::{get, post};
    let Some(server) = application.saga_runtime().http_server() else {
        return Ok(None);
    };
    if !context_path.is_empty()
        && (server.base_path == context_path
            || server.base_path.starts_with(&format!("{context_path}/")))
    {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "saga.http.base_path must not repeat server.context_path",
        ));
    }
    let mut subtree = axum::Router::<Application>::new();
    if !server.command_authenticators.is_empty() {
        subtree = subtree.route("/commands", post(managed_http_command));
    }
    if !server.result_authenticators.is_empty() {
        subtree = subtree.route("/results", post(managed_http_result));
    }
    if server.api_enabled {
        subtree = subtree
            .route("/instances", post(managed_http_start))
            .route("/instances/query", post(managed_http_query_instances))
            .route(
                "/instances/{tenant}/{saga_id}",
                get(managed_http_get_instance),
            )
            .route(
                "/instances/{tenant}/{saga_id}/audit",
                get(managed_http_audit),
            )
            .route("/metrics", get(managed_http_metrics));
        if server.expose_admin {
            subtree = subtree
                .route(
                    "/instances/{tenant}/{saga_id}/pause",
                    post(managed_http_pause),
                )
                .route(
                    "/instances/{tenant}/{saga_id}/resume",
                    post(managed_http_resume),
                )
                .route(
                    "/instances/{tenant}/{saga_id}/retry-compensation",
                    post(managed_http_retry_compensation),
                )
                .route(
                    "/instances/{tenant}/{saga_id}/retry-resolution",
                    post(managed_http_retry_resolution),
                )
                .route(
                    "/instances/{tenant}/{saga_id}/manual-close",
                    post(managed_http_manual_close),
                );
        }
        if server.expose_definition_registry {
            subtree = subtree
                .route(
                    "/registry/capabilities",
                    post(managed_http_register_capability),
                )
                .route(
                    "/registry/definitions",
                    post(managed_http_publish_definition),
                )
                .route(
                    "/registry/definitions/{tenant}/{workflow}/{version}",
                    get(managed_http_get_definition),
                )
                .route(
                    "/registry/definitions/{tenant}/{workflow}/{version}/activate",
                    post(managed_http_activate_definition),
                )
                .route(
                    "/registry/definitions/{tenant}/{workflow}/{version}/deprecate",
                    post(managed_http_deprecate_definition),
                )
                .route(
                    "/registry/definitions/{tenant}/{workflow}/{version}/retire",
                    post(managed_http_retire_definition),
                );
        }
    }
    // 在进入验签、共享 replay 占用和数据库事务前统一限流，避免未受控请求耗尽连接池。
    // deadline 覆盖排队、body 读取与完整事务，超时时不返回任何伪提交收据。
    let subtree = subtree
        .fallback(managed_http_not_found)
        .layer(axum::extract::DefaultBodyLimit::max(
            server.body_limit_bytes,
        ))
        .layer(tower::limit::ConcurrencyLimitLayer::new(
            server.concurrency_limit,
        ))
        .layer(tower_http::timeout::TimeoutLayer::with_status_code(
            axum::http::StatusCode::REQUEST_TIMEOUT,
            server.request_timeout,
        ));
    let effective_path = if context_path.is_empty() {
        server.base_path.clone()
    } else {
        format!("{context_path}{}", server.base_path)
    };
    Ok(Some((
        effective_path.clone(),
        axum::Router::new()
            .nest(&effective_path, subtree)
            .with_state(application.clone()),
    )))
}

/// 业务作用：解析 datasource catalog 中的唯一 driver 权威，禁止根据已编入 feature 猜测数据库类型。
///
/// 参数说明：`application` 提供冻结 catalog，`datasource` 是待解析的角色作用域引用。
///
/// 返回：canonical 引用存在时返回 driver；名称缺失或非法时拒绝 Ready。
async fn managed_datasource_driver(
    application: &Application,
    datasource: &str,
) -> ApplicationResult<natx_core::DatabaseDriver> {
    let reference = natx_core::DatasourceRef::new(datasource).map_err(|error| {
        saga_source_error(
            ApplicationPhase::Ready,
            "Saga role datasource_ref is invalid",
            error,
        )
    })?;
    if let Ok(catalog) = application
        .resource::<Arc<natx_core::DataSourceCatalog>>()
        .await
    {
        return catalog
            .entries()
            .into_iter()
            .find_map(|(candidate, driver)| (candidate == reference).then_some(driver))
            .ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "Saga role datasource_ref is absent from the managed catalog",
                )
            });
    }

    // 单库引导发布的是 driver 专属 Registry，不额外发布多数据源 Catalog；通过受生命周期保护的
    // datasource 入口复验实际 driver，避免单库参与方被误判为没有受管数据源。
    #[cfg(feature = "saga")]
    if application.datasource(datasource).await.is_ok() {
        return Ok(natx_core::DatabaseDriver::MySql);
    }
    #[cfg(feature = "saga-pgsql")]
    if application.pg_datasource(datasource).await.is_ok() {
        return Ok(natx_core::DatabaseDriver::PostgreSql);
    }
    Err(saga_error(
        ApplicationPhase::Ready,
        "Saga role datasource_ref is absent from the managed datasource registry",
    ))
}

/// 业务作用：按冻结 driver 从精确 datasource 装载一份共享动态 Catalog 快照。
///
/// 参数说明：`driver` 来自 Application datasource catalog，`datasource` 是角色作用域内的明确引用。
///
/// 返回：generation、definition 与 capability 同代复验成功时返回快照；后端不可用或内容漂移拒绝 Ready。
async fn load_managed_dynamic_catalog(
    driver: natx_core::DatabaseDriver,
    datasource: &str,
    phase: ApplicationPhase,
) -> ApplicationResult<nasaga_runtime::DynamicCatalogSnapshot> {
    match driver {
        natx_core::DatabaseDriver::MySql => {
            #[cfg(feature = "saga")]
            {
                nasaga_runtime::load_dynamic_catalog_for(datasource)
                    .await
                    .map_err(|error| {
                        saga_source_error(
                            phase,
                            "managed MySQL dynamic Saga catalog is unavailable",
                            error,
                        )
                    })
            }
            #[cfg(not(feature = "saga"))]
            Err(saga_error(
                phase,
                "dynamic Saga catalog requires the MySQL Saga capability",
            ))
        }
        natx_core::DatabaseDriver::PostgreSql => {
            #[cfg(feature = "saga-pgsql")]
            {
                nasaga_runtime_pgsql::load_dynamic_catalog_for(datasource)
                    .await
                    .map_err(|error| {
                        saga_source_error(
                            phase,
                            "managed PostgreSQL dynamic Saga catalog is unavailable",
                            error,
                        )
                    })
            }
            #[cfg(not(feature = "saga-pgsql"))]
            Err(saga_error(
                phase,
                "dynamic Saga catalog requires the PostgreSQL Saga capability",
            ))
        }
    }
}

/// 业务作用：提取 definition 激活时 Redis publisher 实际采用的 Cluster 同槽上下文。
///
/// 参数说明：`settings` 是已通过 Ready 校验的配置，`transport` 是当前 command 数据面。
///
/// 返回：Redis Streams 返回必填的 `key_tag`，其它 transport 返回空。
fn managed_activation_redis_key_tag(
    settings: &SagaSettings,
    transport: SagaTransportKind,
) -> Option<String> {
    (transport == SagaTransportKind::RedisStream)
        .then(|| {
            settings
                .transport
                .command_result
                .as_ref()
                .and_then(|transport| transport.redis_stream.as_ref())
                .and_then(|stream| stream.key_tag.clone())
        })
        .flatten()
}

/// 业务作用：把当前 transport 实际使用的地址 allowlist 规范化为副本确认合同摘要。
///
/// 参数说明：`policy` 是已解析的受信地址政策，`transport` 选择其中生效的协议字段。
///
/// 返回：集合字段排序去重后形成的 canonical SHA-256，不受配置项书写顺序影响。
fn managed_address_policy_digest(
    policy: &SagaAddressPolicySettings,
    transport: SagaTransportKind,
) -> String {
    let mut fields = vec![
        b"napp-saga-address-policy".to_vec(),
        transport.as_str().as_bytes().to_vec(),
    ];
    let mut append_strings = |label: &'static [u8], values: &[String]| {
        let mut values = values.to_vec();
        values.sort();
        values.dedup();
        fields.push(label.to_vec());
        fields.push((values.len() as u64).to_be_bytes().to_vec());
        fields.extend(values.into_iter().map(String::into_bytes));
    };
    match transport {
        SagaTransportKind::Http => {
            append_strings(b"schemes", &policy.http_schemes);
            append_strings(b"hosts", &policy.http_hosts);
            let mut ports = policy.http_ports.clone();
            ports.sort_unstable();
            ports.dedup();
            fields.push(b"ports".to_vec());
            fields.push((ports.len() as u64).to_be_bytes().to_vec());
            fields.extend(ports.into_iter().map(|port| port.to_be_bytes().to_vec()));
        }
        SagaTransportKind::Grpc => {
            append_strings(b"hosts", &policy.grpc_hosts);
            let mut ports = policy.grpc_ports.clone();
            ports.sort_unstable();
            ports.dedup();
            fields.push(b"ports".to_vec());
            fields.push((ports.len() as u64).to_be_bytes().to_vec());
            fields.extend(ports.into_iter().map(|port| port.to_be_bytes().to_vec()));
        }
        SagaTransportKind::Kafka => {
            append_strings(b"topic-prefixes", &policy.kafka_topic_prefixes);
        }
        SagaTransportKind::RedisStream => {
            append_strings(b"stream-prefixes", &policy.redis_stream_prefixes);
        }
    }
    let mut digest = Sha256::new();
    for field in fields {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// 业务作用：把 JSON 配置按类型、长度和有序 object key 写入摘要，消除配置解析器的键顺序差异。
///
/// 参数说明：`digest` 接收 canonical 字节，`value` 是已完成插值与脱敏分离的配置节点。
///
/// 返回：无；相同语义的 object 产生相同字节序列，数组保持业务声明顺序。
fn update_managed_canonical_json(digest: &mut Sha256, value: &serde_json::Value) {
    match value {
        serde_json::Value::Null => digest.update([0]),
        serde_json::Value::Bool(value) => digest.update([1, u8::from(*value)]),
        serde_json::Value::Number(value) => {
            digest.update([2]);
            let value = value.to_string();
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
        serde_json::Value::String(value) => {
            digest.update([3]);
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
        serde_json::Value::Array(values) => {
            digest.update([4]);
            digest.update((values.len() as u64).to_be_bytes());
            for value in values {
                update_managed_canonical_json(digest, value);
            }
        }
        serde_json::Value::Object(values) => {
            digest.update([5]);
            digest.update((values.len() as u64).to_be_bytes());
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for key in keys {
                digest.update((key.len() as u64).to_be_bytes());
                digest.update(key.as_bytes());
                update_managed_canonical_json(
                    digest,
                    values
                        .get(key)
                        .expect("canonical JSON key came from the same object"),
                );
            }
        }
    }
}

/// 业务作用：提取 transport 实际引用的受管 Kafka 或 Redis client 配置节点。
///
/// 参数说明：`application` 提供冻结配置树，`transport` 与 `client_ref` 定位唯一 client。
///
/// 返回：返回将参与 publisher 摘要的配置副本；引用无法映射到当前配置树时拒绝 Ready。
fn managed_backend_client_config(
    application: &Application,
    transport: SagaTransportKind,
    client_ref: &str,
    phase: ApplicationPhase,
) -> ApplicationResult<serde_json::Value> {
    let snapshot = application.config();
    let root = snapshot.value();
    let value = match transport {
        SagaTransportKind::Kafka => match (root.get("kafka"), root.get("kafkas")) {
            (Some(single), None) => Some(single),
            (None, Some(multiple)) => multiple.get(client_ref),
            _ => None,
        },
        SagaTransportKind::RedisStream => {
            let redis = root.get("redis");
            match redis.and_then(|value| value.get("properties")) {
                Some(properties) => properties.get(client_ref).or_else(|| {
                    (client_ref == "default")
                        .then(|| properties.get("primary"))
                        .flatten()
                }),
                None if matches!(client_ref, "default" | "primary") => redis,
                None => None,
            }
        }
        SagaTransportKind::Http | SagaTransportKind::Grpc => None,
    };
    let value = value.cloned().ok_or_else(|| {
        saga_error(
            phase,
            "managed Saga publisher client reference is not present in the frozen configuration",
        )
    })?;
    if transport == SagaTransportKind::Kafka {
        return Ok(serde_json::json!({
            "bootstrap_servers": value.get("bootstrap_servers"),
            "security": value.get("security"),
            "properties": value.get("properties"),
            "producer_properties": value.pointer("/producer/properties"),
            "consumer_properties": value.pointer("/consumer/properties"),
        }));
    }
    Ok(value)
}

/// 业务作用：为 Kafka command/result 双向闭环形成参与方与 Orchestrator 可独立计算的后端身份。
///
/// 参数说明：`application` 提供所选 Kafka client 的冻结配置，`settings` 提供 client 引用和 result topic
/// 前缀，`phase` 标记配置缺口所属的生命周期阶段。
///
/// 返回：规范化 broker 地址集合与 result topic 前缀形成的 SHA-256；字段缺失或含空地址时拒绝推进。
fn managed_kafka_result_backend_digest(
    application: &Application,
    settings: &SagaSettings,
    phase: ApplicationPhase,
) -> ApplicationResult<String> {
    let kafka = settings
        .transport
        .command_result
        .as_ref()
        .and_then(|transport| transport.kafka.as_ref())
        .ok_or_else(|| saga_error(phase, "managed Saga Kafka result settings are missing"))?;
    let backend = managed_backend_client_config(
        application,
        SagaTransportKind::Kafka,
        kafka
            .client_ref
            .as_deref()
            .ok_or_else(|| saga_error(phase, "managed Saga Kafka client_ref is missing"))?,
        phase,
    )?;
    let bootstrap_servers = backend
        .get("bootstrap_servers")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| saga_error(phase, "managed Saga Kafka bootstrap_servers is missing"))?;
    let mut brokers = bootstrap_servers
        .split(',')
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    if brokers.is_empty() || brokers.iter().any(String::is_empty) {
        return Err(saga_error(
            phase,
            "managed Saga Kafka bootstrap_servers contains an empty address",
        ));
    }
    brokers.sort();
    brokers.dedup();
    let result_prefix = kafka
        .result_topic
        .as_deref()
        .ok_or_else(|| saga_error(phase, "managed Saga Kafka result_topic is missing"))?;
    validate_managed_kafka_name(result_prefix, "result topic")?;

    let mut digest = Sha256::new();
    for field in std::iter::once("napp-saga-kafka-result-backend")
        .chain(brokers.iter().map(String::as_str))
        .chain(std::iter::once(result_prefix))
    {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field.as_bytes());
    }
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// 业务作用：按带长度边界的字段序列计算 result 发送与接收双方可独立形成的凭据合同摘要。
///
/// 参数说明：`domain` 隔离协议用途，`fields` 按协议定义的稳定顺序提供身份与密钥材料。
///
/// 返回：返回不暴露原始凭据的 canonical SHA-256。
fn managed_result_contract_digest<'a>(
    domain: &[u8],
    fields: impl IntoIterator<Item = &'a [u8]>,
) -> String {
    let mut digest = Sha256::new();
    digest.update((domain.len() as u64).to_be_bytes());
    digest.update(domain);
    for field in fields {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// 业务作用：从 HTTP result 实际使用的 HMAC secret 形成不暴露密钥的双向合同摘要。
///
/// 参数说明：`application` 提供 secret 快照，`credential_ref` 定位发送或接收凭据，`phase` 标记缺口阶段。
///
/// 返回：凭据可构造认证器时返回 SHA-256；缺失或格式非法时拒绝推进。
fn managed_http_result_contract_digest(
    application: &Application,
    credential_ref: &str,
    phase: ApplicationPhase,
) -> ApplicationResult<String> {
    load_managed_http_authenticator(application, credential_ref)?;
    let secrets = application.secrets();
    let material = secrets
        .get(credential_ref)
        .filter(|material| !material.is_empty())
        .ok_or_else(|| saga_error(phase, "managed Saga HTTP result credential is unavailable"))?;
    Ok(managed_result_contract_digest(
        b"napp-saga-http-result",
        [material.expose()],
    ))
}

/// 业务作用：从 gRPC client leaf certificate 形成与 Orchestrator peer allowlist 可比较的合同摘要。
///
/// 参数说明：`credential` 是实际用于 result channel 的 mTLS 身份，`phase` 标记证书解析失败阶段。
///
/// 返回：leaf principal 可派生时返回 SHA-256；PEM 不合法时拒绝推进。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn managed_grpc_result_contract_digest(
    credential: &ManagedGrpcCredentialMaterial,
    phase: ApplicationPhase,
) -> ApplicationResult<String> {
    let principal =
        nagrpc::client_certificate_principal(credential.identity_certificate_pem.as_bytes())
            .map_err(|error| {
                saga_source_error(
                    phase,
                    "managed Saga gRPC result certificate is invalid",
                    error,
                )
            })?;
    Ok(managed_result_contract_digest(
        b"napp-saga-grpc-result",
        [principal.as_bytes()],
    ))
}

/// 业务作用：从 Redis result 的 key id 与 HMAC key 形成发送端和验签端一致的合同摘要。
///
/// 参数说明：`key_id` 是 wire 身份，`key_hex` 是发送或接收配置中的密钥材料，`phase` 标记解析阶段。
///
/// 返回：key id 与至少 256 bit 的密钥合法时返回 SHA-256；其它输入拒绝推进。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
fn managed_redis_result_contract_digest(
    key_id: &str,
    key_hex: &str,
    phase: ApplicationPhase,
) -> ApplicationResult<String> {
    validate_runtime_name(key_id, "Redis result key id", phase)?;
    let key = hex::decode(key_hex).map_err(|error| {
        saga_source_error(
            phase,
            "managed Saga Redis result key is not hexadecimal",
            error,
        )
    })?;
    if key.len() < 32 {
        return Err(saga_error(
            phase,
            "managed Saga Redis result key is shorter than 256 bits",
        ));
    }
    Ok(managed_result_contract_digest(
        b"napp-saga-redis-result",
        [key_id.as_bytes(), key.as_slice()],
    ))
}

/// 业务作用：计算最终 publisher 的配置、secret 与后端 client 身份摘要，供所有 Ready 副本比较。
///
/// 参数说明：`application` 提供同代配置和 secret，`settings` 提供所选数据面，`transport` 固定分支，
/// `phase` 标记拒绝发生的生命周期阶段。
///
/// 返回：全部引用可解析时返回 canonical SHA-256；缺失 publisher 输入时拒绝生命周期推进。
fn managed_publisher_contract_seed(
    application: &Application,
    settings: &SagaSettings,
    transport: SagaTransportKind,
    phase: ApplicationPhase,
) -> ApplicationResult<(Sha256, BTreeSet<String>)> {
    let selected = settings
        .transport
        .command_result
        .as_ref()
        .ok_or_else(|| saga_error(phase, "managed Saga publisher transport is missing"))?;
    let transport_value = match transport {
        SagaTransportKind::Http => {
            let http = selected.http.as_ref().ok_or_else(|| {
                saga_error(phase, "managed Saga HTTP publisher settings are missing")
            })?;
            serde_json::json!({
                "shared_replay_claim": http.shared_replay_claim,
                "routing": http.routing,
                "command_credential_ref": http.command_credential_ref,
                "producer_credentials": http.producer_credentials,
                "request_timeout_ms": http.request_timeout_ms,
                "body_limit_bytes": http.body_limit_bytes,
                "concurrency_limit": http.concurrency_limit,
            })
        }
        SagaTransportKind::Grpc => {
            let grpc = selected.grpc.as_ref().ok_or_else(|| {
                saga_error(phase, "managed Saga gRPC publisher settings are missing")
            })?;
            serde_json::json!({
                "routing": grpc.routing,
                "credential_ref": grpc.credential_ref,
                "peer_principals": grpc.peer_principals,
                "request_timeout_ms": grpc.request_timeout_ms,
            })
        }
        SagaTransportKind::Kafka => {
            serde_json::to_value(selected.kafka.as_ref().ok_or_else(|| {
                saga_error(phase, "managed Saga Kafka publisher settings are missing")
            })?)
            .map_err(|error| {
                saga_source_error(
                    phase,
                    "managed Saga Kafka publisher settings cannot be canonicalized",
                    error,
                )
            })?
        }
        SagaTransportKind::RedisStream => {
            let stream = selected.redis_stream.as_ref().ok_or_else(|| {
                saga_error(phase, "managed Saga Redis publisher settings are missing")
            })?;
            serde_json::json!({
                "client_ref": stream.client_ref,
                "key_tag": stream.key_tag,
                "command_stream": stream.command_stream,
                "result_stream": stream.result_stream,
                "result_group": stream.result_group,
                "result_dlt_stream": stream.result_dlt_stream,
                "credential_ref": stream.credential_ref,
                "routing": stream.routing,
            })
        }
    };
    let mut digest = Sha256::new();
    digest.update(b"napp-saga-publisher-contract");
    update_managed_canonical_json(&mut digest, &transport_value);

    let mut secret_refs = BTreeSet::new();
    let backend =
        match transport {
            SagaTransportKind::Http => {
                let http = selected
                    .http
                    .as_ref()
                    .expect("selected HTTP settings were checked");
                secret_refs.extend(http.command_credential_ref.iter().cloned());
                secret_refs.extend(http.producer_credentials.values().cloned());
                None
            }
            SagaTransportKind::Grpc => {
                let grpc = selected
                    .grpc
                    .as_ref()
                    .expect("selected gRPC settings were checked");
                secret_refs.extend(grpc.credential_ref.iter().cloned());
                None
            }
            SagaTransportKind::Kafka => {
                let kafka = selected
                    .kafka
                    .as_ref()
                    .expect("selected Kafka settings were checked");
                Some(managed_backend_client_config(
                    application,
                    transport,
                    kafka.client_ref.as_deref().ok_or_else(|| {
                        saga_error(phase, "managed Saga Kafka client_ref is missing")
                    })?,
                    phase,
                )?)
            }
            SagaTransportKind::RedisStream => {
                let stream = selected
                    .redis_stream
                    .as_ref()
                    .expect("selected Redis settings were checked");
                secret_refs.extend(stream.credential_ref.iter().cloned());
                Some(managed_backend_client_config(
                    application,
                    transport,
                    stream.client_ref.as_deref().ok_or_else(|| {
                        saga_error(phase, "managed Saga Redis client_ref is missing")
                    })?,
                    phase,
                )?)
            }
        };
    if let Some(backend) = backend {
        update_managed_canonical_json(&mut digest, &backend);
    }
    Ok((digest, secret_refs))
}

/// 业务作用：构造激活、人工操作与 watcher 共用的 publisher 安全合同。
///
/// 参数说明：`application` 提供冻结运行资源，`settings` 是已校验 Saga 配置，`phase` 标记构造阶段。
///
/// 返回：路由、接收端 owner 身份、后端 client 与 gRPC 探针材料完整时返回合同；缺口拒绝推进。
fn managed_definition_activation_contract(
    application: &Application,
    settings: &SagaSettings,
    phase: ApplicationPhase,
) -> ApplicationResult<ManagedDefinitionActivationContract> {
    let transport = settings
        .transport
        .command_result
        .as_ref()
        .and_then(|transport| transport.kind)
        .ok_or_else(|| saga_error(phase, "managed Saga activation transport is missing"))?;
    let routing = settings
        .transport
        .command_result
        .as_ref()
        .and_then(|selected| match transport {
            SagaTransportKind::Http => selected.http.as_ref().map(|value| &value.routing),
            SagaTransportKind::Grpc => selected.grpc.as_ref().map(|value| &value.routing),
            SagaTransportKind::Kafka => selected.kafka.as_ref().map(|value| &value.routing),
            SagaTransportKind::RedisStream => {
                selected.redis_stream.as_ref().map(|value| &value.routing)
            }
        })
        .ok_or_else(|| saga_error(phase, "managed Saga activation routing is missing"))?;
    let owns_activation = matches!(
        settings.role,
        Some(SagaRole::Orchestrator | SagaRole::Combined)
    );
    let address_policy = (owns_activation
        && routing.mode.as_deref() == Some("capability-registry"))
    .then(|| resolve_managed_address_policy(settings, routing, transport, phase).cloned())
    .transpose()?;
    let trusted_result_contracts = match transport {
        SagaTransportKind::Http => {
            let http = settings
                .transport
                .command_result
                .as_ref()
                .and_then(|value| value.http.as_ref())
                .expect("selected HTTP settings were checked");
            let mut contracts = BTreeMap::<String, BTreeSet<String>>::new();
            for (owner, credential) in &http.producer_credentials {
                nasaga_runtime::ServiceIdentity::new(owner).map_err(|error| {
                    saga_source_error(phase, "managed Saga HTTP result owner is invalid", error)
                })?;
                contracts.entry(owner.clone()).or_default().insert(
                    managed_http_result_contract_digest(application, credential, phase)?,
                );
            }
            contracts
        }
        SagaTransportKind::Grpc => {
            let grpc = settings
                .transport
                .command_result
                .as_ref()
                .and_then(|value| value.grpc.as_ref())
                .expect("selected gRPC settings were checked");
            let mut contracts = BTreeMap::<String, BTreeSet<String>>::new();
            for (owner, principal) in &grpc.peer_principals {
                nasaga_runtime::ServiceIdentity::new(owner).map_err(|error| {
                    saga_source_error(phase, "managed Saga gRPC result owner is invalid", error)
                })?;
                validate_managed_grpc_principal(principal, phase)?;
                contracts
                    .entry(owner.clone())
                    .or_default()
                    .insert(managed_result_contract_digest(
                        b"napp-saga-grpc-result",
                        [principal.as_bytes()],
                    ));
            }
            contracts
        }
        SagaTransportKind::Kafka => BTreeMap::new(),
        SagaTransportKind::RedisStream => {
            #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
            {
                let stream = settings
                    .transport
                    .command_result
                    .as_ref()
                    .and_then(|value| value.redis_stream.as_ref())
                    .expect("selected Redis settings were checked");
                let credentials = load_managed_redis_credentials(
                    application,
                    stream.credential_ref.as_deref().ok_or_else(|| {
                        saga_error(phase, "managed Saga Redis credential_ref is missing")
                    })?,
                )?;
                managed_redis_verification_auth(&credentials)?;
                let mut contracts = BTreeMap::<String, BTreeSet<String>>::new();
                for (key_id, key) in &credentials.verification_keys {
                    nasaga_runtime::ServiceIdentity::new(&key.service_identity).map_err(
                        |error| {
                            saga_source_error(
                                phase,
                                "managed Saga Redis result owner is invalid",
                                error,
                            )
                        },
                    )?;
                    contracts
                        .entry(key.service_identity.clone())
                        .or_default()
                        .insert(managed_redis_result_contract_digest(
                            key_id,
                            &key.key_hex,
                            phase,
                        )?);
                }
                contracts
            }
            #[cfg(not(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql")))]
            return Err(saga_error(
                phase,
                "managed Saga Redis activation contract is unavailable",
            ));
        }
    };
    let result_backend_digest = (transport == SagaTransportKind::Kafka)
        .then(|| managed_kafka_result_backend_digest(application, settings, phase))
        .transpose()?;
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    let (grpc_credential, grpc_timeout) = if transport == SagaTransportKind::Grpc {
        let grpc = settings
            .transport
            .command_result
            .as_ref()
            .and_then(|value| value.grpc.as_ref())
            .expect("selected gRPC settings were checked");
        let credential = security::grpc_credential(
            application,
            grpc.credential_ref
                .as_deref()
                .ok_or_else(|| saga_error(phase, "managed Saga gRPC credential_ref is missing"))?,
        )?;
        (
            Some(Arc::new(credential)),
            Some(Duration::from_millis(
                grpc.request_timeout_ms.unwrap_or(5_000),
            )),
        )
    } else {
        (None, None)
    };
    let (publisher_seed, publisher_refs) =
        managed_publisher_contract_seed(application, settings, transport, phase)?;
    let state = security::SagaSecurityState::for_application(application)?;
    let http_result_refs = settings
        .transport
        .command_result
        .as_ref()
        .and_then(|value| value.http.as_ref())
        .map(|value| value.producer_credentials.clone())
        .unwrap_or_default();
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    let grpc_peer_bindings = settings
        .transport
        .command_result
        .as_ref()
        .and_then(|value| value.grpc.as_ref())
        .map(|value| value.peer_principals.clone())
        .unwrap_or_default();
    let security = Arc::new(ManagedDefinitionActivationSecurity {
        state,
        publisher_seed,
        publisher_refs,
        http_result_refs,
        #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
        grpc_peer_bindings,
    });
    let mut contract = ManagedDefinitionActivationContract {
        transport,
        redis_key_tag: managed_activation_redis_key_tag(settings, transport),
        address_policy,
        publisher_contract_digest: String::new(),
        security_generation: 0,
        security: Some(security),
        result_backend_digest,
        trusted_result_contracts,
        #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
        grpc_credential,
        #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
        grpc_timeout,
    };
    let current = contract.current()?;
    contract.publisher_contract_digest = current.publisher_contract_digest;
    contract.trusted_result_contracts = current.trusted_result_contracts;
    contract.security_generation = current.security_generation;
    Ok(contract)
}

/// 业务作用：证明每个可启动 definition owner 都能被当前 result consumer 认证，避免形成单向流程。
///
/// 参数说明：`activation` 提供受信 owner 与凭据合同，`registry` 是拟发布的 active/deprecated 快照，
/// `phase` 标记首次 Ready 或运行期切代。
///
/// 返回：HTTP、gRPC、Redis 的全部 owner 均有接收凭据时成功；缺口拒绝发布 registry。
fn validate_managed_registry_result_contract(
    activation: &ManagedDefinitionActivationContract,
    registry: &DefinitionRegistry,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let activation = activation.current()?;
    if activation.transport == SagaTransportKind::Kafka {
        // Kafka 的 producer 写 ACL 由 capability 发布前的 broker 探针证明，consumer 读 ACL
        // 由同一受管 group 的 readiness 门禁证明，owner 则由无歧义 topic 编码绑定。
        return Ok(());
    }
    let complete = registry
        .definitions_with_tenants()
        .flat_map(|(_, definition)| definition.steps())
        .all(|step| {
            activation
                .trusted_result_contracts
                .contains_key(step.owner().as_str())
        });
    if complete {
        Ok(())
    } else {
        Err(saga_error(
            phase,
            "managed Saga registry contains an owner without a trusted result receiver identity",
        ))
    }
}

/// 业务作用：构造绑定完整 transport 发布上下文的 definition 激活门禁。
///
/// 参数说明：服务身份、generation 与摘要冻结集群快照，`activation` 冻结实际 publisher 的
/// route、安全和后端 client 合同，`phase` 标记拒绝发生的生命周期阶段。
///
/// 返回：运行时路由字段均合法时返回带 canonical 合同摘要的门禁；配置不完整时拒绝激活。
fn managed_definition_activation_gate(
    service_identity: &nasaga_runtime::ServiceIdentity,
    catalog_generation: u64,
    snapshot_digest: impl Into<String>,
    activation: &ManagedDefinitionActivationContract,
    phase: ApplicationPhase,
) -> ApplicationResult<nasaga_runtime::DefinitionActivationGate> {
    let activation = activation.current()?;
    let mut gate = nasaga_runtime::DefinitionActivationGate::new(
        service_identity.clone(),
        catalog_generation,
        snapshot_digest,
        activation.transport.as_str(),
    )
    .map_err(|error| {
        saga_source_error(
            phase,
            "managed Saga definition activation gate is invalid",
            error,
        )
    })?;
    if activation.transport == SagaTransportKind::RedisStream {
        let key_tag = activation
            .redis_key_tag
            .as_deref()
            .ok_or_else(|| saga_error(phase, "managed Saga Redis activation key tag is missing"))?;
        gate = gate.with_redis_key_tag(key_tag).map_err(|error| {
            saga_source_error(
                phase,
                "managed Saga Redis activation key tag is invalid",
                error,
            )
        })?;
    }
    if let Some(policy) = activation.address_policy.as_ref() {
        validate_managed_address_policy(policy, activation.transport, phase)?;
        gate = gate
            .with_address_policy_digest(managed_address_policy_digest(policy, activation.transport))
            .map_err(|error| {
                saga_source_error(
                    phase,
                    "managed Saga activation address policy digest is invalid",
                    error,
                )
            })?;
    }
    if let Some(result_backend_digest) = activation.result_backend_digest.as_deref() {
        gate = gate
            .with_result_backend_digest(result_backend_digest)
            .map_err(|error| {
                saga_source_error(
                    phase,
                    "managed Saga activation result backend digest is invalid",
                    error,
                )
            })?;
    }
    for (owner, contracts) in &activation.trusted_result_contracts {
        for contract in contracts {
            gate = gate
                .with_result_contract_digest(owner, contract)
                .map_err(|error| {
                    saga_source_error(
                        phase,
                        "managed Saga activation result contract digest is invalid",
                        error,
                    )
                })?;
        }
    }
    gate.with_publisher_contract_digest(&activation.publisher_contract_digest)
        .map_err(|error| {
            saga_source_error(
                phase,
                "managed Saga activation publisher contract digest is invalid",
                error,
            )
        })
}

/// 业务作用：从数据库当前完整快照构造 definition 激活的同代副本与数据面门禁。
///
/// 参数说明：`driver`/`datasource` 定位共享 Catalog，`service_identity` 定位逻辑 Orchestrator，
/// `activation` 固定当前部署的实际投递与接收边界，`definition_key` 的租户、workflow 与版本定位待激活记录，
/// `phase` 标记调用阶段。
///
/// 返回：candidate 与 capability 均已纳入摘要时返回门禁；装载漂移或字段非法时拒绝激活。
async fn load_managed_definition_activation_gate(
    driver: natx_core::DatabaseDriver,
    datasource: &str,
    service_identity: &nasaga_runtime::ServiceIdentity,
    activation: &ManagedDefinitionActivationContract,
    definition_key: (&str, &str, u32),
    phase: ApplicationPhase,
) -> ApplicationResult<nasaga_runtime::DefinitionActivationGate> {
    let activation = activation.current()?;
    let snapshot = load_managed_dynamic_catalog(driver, datasource, phase).await?;
    let (tenant, workflow, definition_version) = definition_key;
    let record = snapshot
        .definition_records
        .iter()
        .find(|record| {
            record.artifact.tenant == tenant
                && record.artifact.workflow == workflow
                && record.artifact.definition_version == definition_version
        })
        .ok_or_else(|| saga_error(phase, "managed Saga definition does not exist"))?;
    if record.lifecycle == nasaga_runtime::DefinitionLifecycle::Candidate
        && !candidate_capabilities_are_routable(&activation, &snapshot, &record.artifact, phase)?
    {
        return Err(saga_error(
            phase,
            "managed Saga candidate route and security contract is incomplete",
        ));
    }
    if record.lifecycle == nasaga_runtime::DefinitionLifecycle::Candidate {
        probe_managed_candidate_grpc_routes(&activation, &snapshot, &record.artifact, phase)
            .await?;
    }
    managed_definition_activation_gate(
        service_identity,
        snapshot.generation,
        snapshot.snapshot_digest,
        &activation,
        phase,
    )
}

/// 业务作用：把当前二进制的 MySQL managed 工厂实例化为按命令身份唯一索引的 handler 快照。
///
/// 参数说明：`runtime` 是所有本地步骤共享的同源 Participant 事务运行时。
///
/// 返回：每个步骤恰有一个显式 managed 工厂时返回快照；缺失或重复时拒绝 Ready。
#[cfg(feature = "saga")]
fn build_mysql_managed_command_handlers(
    runtime: Arc<nasaga_runtime::ParticipantRuntime>,
    expected: &std::collections::BTreeSet<(String, u32, String)>,
) -> ApplicationResult<BTreeMap<(String, u32, String), ManagedParticipantCommandHandler>> {
    let mut handlers = BTreeMap::new();
    for descriptor in nasaga_runtime::COLLECTED_MANAGED_SAGA_STEPS {
        let key = (
            descriptor.workflow.to_owned(),
            descriptor.definition_version,
            descriptor.step.to_owned(),
        );
        if !expected.contains(&key) {
            continue;
        }
        let handler =
            ManagedParticipantCommandHandler::MySql((descriptor.factory)(Arc::clone(&runtime)));
        if handlers.insert(key, handler).is_some() {
            return Err(saga_error(
                ApplicationPhase::Ready,
                "managed Saga step factory is duplicated",
            ));
        }
    }
    if handlers
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        != *expected
    {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "every participant #[saga] step must opt into managed construction",
        ));
    }
    Ok(handlers)
}

/// 业务作用：把当前二进制的 PostgreSQL managed 工厂实例化为按命令身份唯一索引的 handler 快照。
///
/// 参数说明：`runtime` 是所有本地步骤共享的同源 Participant 事务运行时。
///
/// 返回：每个步骤恰有一个显式 managed 工厂时返回快照；缺失或重复时拒绝 Ready。
#[cfg(feature = "saga-pgsql")]
fn build_pgsql_managed_command_handlers(
    runtime: Arc<nasaga_runtime_pgsql::PgParticipantRuntime>,
    expected: &std::collections::BTreeSet<(String, u32, String)>,
) -> ApplicationResult<BTreeMap<(String, u32, String), ManagedParticipantCommandHandler>> {
    let mut handlers = BTreeMap::new();
    for descriptor in nasaga_runtime_pgsql::COLLECTED_MANAGED_SAGA_STEPS {
        let key = (
            descriptor.workflow.to_owned(),
            descriptor.definition_version,
            descriptor.step.to_owned(),
        );
        if !expected.contains(&key) {
            continue;
        }
        let handler = ManagedParticipantCommandHandler::PostgreSql((descriptor.factory)(
            Arc::clone(&runtime),
        ));
        if handlers.insert(key, handler).is_some() {
            return Err(saga_error(
                ApplicationPhase::Ready,
                "managed PostgreSQL Saga step factory is duplicated",
            ));
        }
    }
    if handlers
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        != *expected
    {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "every PostgreSQL participant #[saga] step must opt into managed construction",
        ));
    }
    Ok(handlers)
}

/// 业务作用：把当前二进制的完整流程定义投影为每个获批租户的不可变发布产物。
///
/// 参数说明：`settings` 提供受信租户范围，`publisher` 是 workflow owner，`phase` 是错误归属阶段。
///
/// 返回：本地没有定义时返回空集合；所有定义、租户与 seal 都合法时返回稳定排序的产物集合。
fn collect_managed_definition_artifacts(
    settings: &SagaSettings,
    publisher: &nasaga_runtime::ServiceIdentity,
    phase: ApplicationPhase,
) -> ApplicationResult<Vec<nasaga_runtime::DefinitionArtifact>> {
    let registry = nasaga_runtime::collect_workflow_definitions().map_err(|error| {
        saga_source_error(
            phase,
            "dynamic Saga definitions could not be collected",
            error,
        )
    })?;
    if registry.is_empty() {
        return Ok(Vec::new());
    }
    if settings.definition_catalog.publish_tenants.is_empty() {
        return Err(saga_error(
            phase,
            "local dynamic Saga definitions require definition_catalog.publish_tenants",
        ));
    }
    let mut artifacts = Vec::new();
    for tenant in &settings.definition_catalog.publish_tenants {
        nasaga_runtime::__private::core::TenantId::new(tenant).map_err(|error| {
            saga_source_error(
                phase,
                "definition publish tenant is invalid",
                anyhow::anyhow!(error.code()),
            )
        })?;
        for definition in registry.definitions() {
            artifacts.push(nasaga_runtime::DefinitionArtifact::from_definition(
                tenant,
                publisher.as_str(),
                definition,
            ));
        }
    }
    artifacts.sort_by(|left, right| {
        (&left.tenant, &left.workflow, left.definition_version).cmp(&(
            &right.tenant,
            &right.workflow,
            right.definition_version,
        ))
    });
    Ok(artifacts)
}

/// 业务作用：把 workflow owner 链接的完整定义签名后绑定到唯一远程 Registry 控制面。
///
/// 参数说明：`application` 提供私钥与客户端凭据，`settings` 冻结租户和 Registry 路由，`publisher` 是定义 owner。
///
/// 返回：没有本地定义或不是动态模式时返回空；其余情况只有全部 seal、身份与协议配置完整才返回发布计划。
fn build_managed_definition_publish_plan(
    application: &Application,
    settings: &SagaSettings,
    publisher: &nasaga_runtime::ServiceIdentity,
) -> ApplicationResult<Option<ManagedDefinitionPublishPlan>> {
    if settings.definition_catalog.mode != DefinitionCatalogMode::Dynamic {
        return Ok(None);
    }
    let artifacts =
        collect_managed_definition_artifacts(settings, publisher, ApplicationPhase::Ready)?;
    if artifacts.is_empty() {
        return Ok(None);
    }
    if !matches!(
        settings.definition_catalog.activation_policy.as_deref(),
        Some("validated" | "approved")
    ) {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "workflow definition publication requires a validated or approved activation policy",
        ));
    }
    let registry = settings
        .definition_catalog
        .registry_client
        .as_ref()
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "local workflow definitions require definition_catalog.registry_client",
            )
        })?;
    let signing_key_id = registry
        .definition_signing_key_id
        .as_deref()
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "local workflow definitions require definition_signing_key_id",
            )
        })?;
    validate_runtime_name(
        signing_key_id,
        "definition signing key id",
        ApplicationPhase::Ready,
    )?;
    let signing_key_ref = registry
        .definition_signing_key_ref
        .as_deref()
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "local workflow definitions require definition_signing_key_ref",
            )
        })?;
    let secrets = application.secrets();
    let signing_key = secrets.get(signing_key_ref).ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "workflow definition signing key is unavailable",
        )
    })?;
    let signing_key = std::str::from_utf8(signing_key.expose()).map_err(|error| {
        saga_source_error(
            ApplicationPhase::Ready,
            "workflow definition signing key is not UTF-8",
            error,
        )
    })?;
    if signing_key.is_empty() || signing_key.trim() != signing_key {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "workflow definition signing key is invalid",
        ));
    }
    let mut signed_artifacts = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        let canonical_document = serde_json::to_string(&artifact).map_err(|error| {
            saga_source_error(
                ApplicationPhase::Ready,
                "workflow definition serialization failed",
                error,
            )
        })?;
        let signature =
            ncrypto::sign_ed25519(&canonical_document, signing_key).map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Ready,
                    "workflow definition signing failed",
                    error,
                )
            })?;
        signed_artifacts.push(ManagedSignedDefinitionArtifact {
            format: "nasaga-definition-json".to_owned(),
            sha256: managed_document_sha256(canonical_document.as_bytes()),
            canonical_document,
            signing_key_id: signing_key_id.to_owned(),
            signature,
        });
    }
    let discovery_ref = registry.discovery_ref.as_deref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "definition registry discovery_ref is missing",
        )
    })?;
    let credential_ref = registry.credential_ref.as_deref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "definition registry credential_ref is missing",
        )
    })?;
    let transport = match registry.protocol.ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "definition registry protocol is missing",
        )
    })? {
        SagaClientProtocol::Http => {
            let target = managed_http_discovery_target(
                application,
                settings,
                discovery_ref,
                "registry/definitions",
                load_managed_http_authenticator(application, credential_ref)?,
                Duration::from_secs(5),
            )?;
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|error| {
                    saga_source_error(
                        ApplicationPhase::Ready,
                        "definition registry HTTP client construction failed",
                        error,
                    )
                })?;
            ManagedDefinitionPublishTransport::Http {
                client,
                target: Box::new(target),
            }
        }
        SagaClientProtocol::Grpc => {
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            {
                let credential = security::grpc_credential(application, credential_ref)?;
                ManagedDefinitionPublishTransport::Grpc {
                    target: managed_grpc_discovery_target(
                        application,
                        settings,
                        discovery_ref,
                        Duration::from_secs(5),
                        &credential,
                    )?,
                }
            }
            #[cfg(not(any(feature = "saga-grpc", feature = "saga-grpc-pgsql")))]
            return Err(saga_error(
                ApplicationPhase::Ready,
                "definition registry gRPC client capability is not compiled",
            ));
        }
    };
    Ok(Some(ManagedDefinitionPublishPlan {
        transport,
        producer: publisher.clone(),
        artifacts: signed_artifacts,
        require_active: settings.definition_catalog.activation_policy.as_deref()
            == Some("validated"),
    }))
}

/// 业务作用：只为 orchestrator 角色建立全局状态结构、definition 快照、推进引擎与 timer fencing 身份。
///
/// 参数说明：`application` 提供数据库资源，`settings` 只读取 orchestrator 作用域。
///
/// 返回：建表复验、definition 门禁和 runtime 构造成功时返回只含 Orchestrator 的计划。
async fn build_managed_orchestrator_plan(
    application: &Application,
    settings: &SagaSettings,
) -> ApplicationResult<SagaApplicationPlan> {
    let datasource = settings.orchestrator_datasource_ref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "saga.orchestrator.datasource_ref is required",
        )
    })?;
    let driver = managed_datasource_driver(application, datasource).await?;
    if settings.definition_catalog.mode == DefinitionCatalogMode::Dynamic
        && settings.definition_catalog.datasource_ref.as_deref() != Some(datasource)
    {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed dynamic Catalog must share the orchestrator datasource",
        ));
    }
    let timer_owner = settings.replica_identity.clone().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "managed orchestrator requires saga.replica_identity",
        )
    })?;
    match driver {
        natx_core::DatabaseDriver::MySql => {
            #[cfg(feature = "saga")]
            {
                let _pool = application.datasource(datasource).await?;
                // 全局状态、result Inbox 与 command Outbox 必须在同一 datasource 上完成
                // schema 门禁，任一 DDL 或最终复验失败都不能构造推进权威。
                nasaga_runtime::ensure_orchestrator_schema_for(datasource)
                    .await
                    .map_err(|error| {
                        saga_source_error(
                            ApplicationPhase::Ready,
                            "managed MySQL orchestrator schema is unavailable",
                            error,
                        )
                    })?;
            }
            #[cfg(not(feature = "saga"))]
            return Err(saga_error(
                ApplicationPhase::Ready,
                "managed orchestrator datasource requires the MySQL Saga capability",
            ));
        }
        natx_core::DatabaseDriver::PostgreSql => {
            #[cfg(feature = "saga-pgsql")]
            {
                let _pool = application.pg_datasource(datasource).await?;
                // PostgreSQL 与 MySQL 遵守相同角色裁剪，但只访问当前作用域的命名连接池。
                nasaga_runtime_pgsql::ensure_orchestrator_schema_for(datasource)
                    .await
                    .map_err(|error| {
                        saga_source_error(
                            ApplicationPhase::Ready,
                            "managed PostgreSQL orchestrator schema is unavailable",
                            error,
                        )
                    })?;
            }
            #[cfg(not(feature = "saga-pgsql"))]
            return Err(saga_error(
                ApplicationPhase::Ready,
                "managed orchestrator datasource requires the PostgreSQL Saga capability",
            ));
        }
    }
    let catalog_snapshot = match settings.definition_catalog.mode {
        DefinitionCatalogMode::Static => {
            let registry = nasaga_runtime::collect_workflow_definitions().map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Ready,
                    "static Saga definitions could not be collected",
                    error,
                )
            })?;
            if registry.is_empty() {
                return Err(saga_error(
                    ApplicationPhase::Ready,
                    "static definition catalog requires at least one saga_workflow descriptor",
                ));
            }
            nasaga_runtime::DynamicCatalogSnapshot::new(0, registry, Vec::new())
        }
        // 动态模式只装载共享 Catalog 已激活或仍服务在途实例的 definition，
        // 不把当前二进制意外链接的 workflow 提升为本地权威。
        DefinitionCatalogMode::Dynamic => {
            load_managed_dynamic_catalog(driver, datasource, ApplicationPhase::Ready).await?
        }
    };
    let registry = catalog_snapshot.registry.clone();
    let runtime = match driver {
        natx_core::DatabaseDriver::MySql => {
            #[cfg(feature = "saga")]
            {
                SagaOrchestratorApi::MySql(Arc::new(
                    nasaga_runtime::Orchestrator::with_datasource(
                        registry,
                        settings.orchestrator_config(),
                        datasource,
                    )
                    .map_err(|error| {
                        saga_source_error(
                            ApplicationPhase::Ready,
                            "managed MySQL orchestrator construction failed",
                            error,
                        )
                    })?,
                ))
            }
            #[cfg(not(feature = "saga"))]
            return Err(saga_error(
                ApplicationPhase::Ready,
                "managed orchestrator datasource requires the MySQL Saga capability",
            ));
        }
        natx_core::DatabaseDriver::PostgreSql => {
            #[cfg(feature = "saga-pgsql")]
            {
                SagaOrchestratorApi::PostgreSql(Arc::new(
                    nasaga_runtime_pgsql::PgOrchestrator::with_datasource(
                        registry,
                        settings.orchestrator_config(),
                        datasource,
                    )
                    .map_err(|error| {
                        saga_source_error(
                            ApplicationPhase::Ready,
                            "managed PostgreSQL orchestrator construction failed",
                            error,
                        )
                    })?,
                ))
            }
            #[cfg(not(feature = "saga-pgsql"))]
            return Err(saga_error(
                ApplicationPhase::Ready,
                "managed orchestrator datasource requires the PostgreSQL Saga capability",
            ));
        }
    };
    let mut plan = SagaApplicationPlan::new();
    plan.orchestrator = Some(OrchestratorPlan {
        runtime: runtime.clone(),
        timer_owner,
    });
    #[cfg_attr(
        not(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql")),
        allow(unused_mut)
    )]
    let mut publisher = build_managed_protocol_publisher(
        application,
        settings,
        &catalog_snapshot.registry,
        &catalog_snapshot.capabilities,
    )
    .await?;
    if settings.definition_catalog.mode == DefinitionCatalogMode::Dynamic {
        let definition_publisher = nasaga_runtime::ServiceIdentity::new(
            settings.service_identity.as_deref().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "managed orchestrator service identity is missing",
                )
            })?,
        )
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Ready,
                "managed definition publisher identity is invalid",
                error,
            )
        })?;
        let definition_artifacts = collect_managed_definition_artifacts(
            settings,
            &definition_publisher,
            ApplicationPhase::Ready,
        )?;
        let activation =
            managed_definition_activation_contract(application, settings, ApplicationPhase::Ready)?;
        #[cfg(feature = "web")]
        let http_authenticator = settings
            .transport
            .command_result
            .as_ref()
            .filter(|transport| transport.kind == Some(SagaTransportKind::Http))
            .and_then(|transport| transport.http.as_ref())
            .and_then(|http| http.command_credential_ref.as_deref())
            .map(|credential| load_managed_http_authenticator(application, credential))
            .transpose()?;
        #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
        let kafka_command_prefix = publisher.dynamic_kafka_routing.as_ref().map(|_| {
            settings
                .transport
                .command_result
                .as_ref()
                .and_then(|transport| transport.kafka.as_ref())
                .and_then(|kafka| kafka.command_topic.clone())
                .expect("validated managed Kafka command topic")
        });
        #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
        let (redis_client, redis_command_auth, redis_key_tag) =
            if publisher.dynamic_redis_routing.is_some() {
                let stream = settings
                    .transport
                    .command_result
                    .as_ref()
                    .and_then(|transport| transport.redis_stream.as_ref())
                    .expect("validated managed Redis Streams settings");
                let client = application
                    .redis(
                        stream
                            .client_ref
                            .as_deref()
                            .expect("validated Redis client_ref"),
                    )
                    .await?;
                let credentials = load_managed_redis_credentials(
                    application,
                    stream
                        .credential_ref
                        .as_deref()
                        .expect("validated Redis credential_ref"),
                )?;
                let auth = managed_redis_signing_auth(&credentials, &definition_publisher)?;
                (Some(client), Some(auth), stream.key_tag.clone())
            } else {
                (None, None, None)
            };
        plan.catalog_watch = Some(ManagedCatalogWatchPlan {
            datasource: datasource.to_owned(),
            driver,
            interval_ms: settings.definition_catalog.watch_interval_ms.unwrap_or(500),
            generation: catalog_snapshot.generation,
            snapshot_digest: catalog_snapshot.snapshot_digest,
            result_registry: catalog_snapshot.registry.clone(),
            service_identity: definition_publisher.clone(),
            replica_identity: settings.replica_identity.clone().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "managed orchestrator replica identity is missing",
                )
            })?,
            runtime,
            #[cfg(feature = "web")]
            http_routing: publisher.dynamic_http_routing,
            #[cfg(feature = "web")]
            http_authenticator,
            #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
            kafka_routing: publisher.dynamic_kafka_routing.clone(),
            #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
            kafka_command_prefix,
            #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
            redis_routing: publisher.dynamic_redis_routing.clone(),
            #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
            redis_client,
            #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
            redis_command_auth,
            #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
            redis_key_tag,
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            grpc_routing: publisher.dynamic_grpc_routing.clone(),
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            grpc_credential: publisher.dynamic_grpc_credential.clone(),
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            grpc_timeout: publisher.dynamic_grpc_timeout,
            definition_publisher: (!definition_artifacts.is_empty())
                .then_some(definition_publisher),
            definition_artifacts,
            activation_policy: settings
                .definition_catalog
                .activation_policy
                .clone()
                .unwrap_or_else(|| "approved".to_owned()),
            activation,
        });
    }
    #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
    {
        plan.redis_transport = publisher.redis_transport.take();
    }
    let outbox = crate::outbox::OutboxApplicationPlan::new(Arc::new(publisher.publisher))
        .dead_letter_after(1)?;
    plan = plan.with_event_publisher_plan(outbox)?;
    #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
    register_managed_kafka_result_consumer(application, settings, plan.orchestrator.as_ref())?;
    Ok(plan)
}

/// 业务作用：只为 participant 角色建立本地 gate、command Inbox、result Outbox 与冻结命令信任投影。
///
/// 参数说明：`application` 提供本地事务域，`settings` 只读取 participant 作用域。
///
/// 返回：本地 descriptor、handler 工厂与 schema 可用时返回只含 participant 的计划。
async fn build_managed_participant_plan(
    application: &Application,
    settings: &SagaSettings,
) -> ApplicationResult<SagaApplicationPlan> {
    let service_identity = nasaga_runtime::ServiceIdentity::new(
        settings
            .participant
            .service_identity
            .as_deref()
            .ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "managed participant requires service_identity",
                )
            })?,
    )
    .map_err(|error| {
        saga_source_error(
            ApplicationPhase::Ready,
            "participant service identity is invalid",
            error,
        )
    })?;
    let producer = nasaga_runtime::ServiceIdentity::new(
        settings
            .participant
            .orchestrator_identity
            .as_deref()
            .ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "managed participant requires orchestrator_identity",
                )
            })?,
    )
    .map_err(|error| {
        saga_source_error(
            ApplicationPhase::Ready,
            "participant orchestrator identity is invalid",
            error,
        )
    })?;
    if nasaga_runtime::COLLECTED_SAGA_STEPS.is_empty() {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed participant requires at least one local saga descriptor",
        ));
    }
    let consumer = settings
        .participant
        .consumer_identity
        .clone()
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "managed participant requires consumer_identity",
            )
        })?;
    let mut grouped = BTreeMap::<String, (String, Vec<&nasaga_runtime::SagaStepDescriptor>)>::new();
    if settings.participant.bindings.is_empty() {
        let datasource = settings.participant_datasource_ref().ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "saga.participant.datasource_ref is required",
            )
        })?;
        if nasaga_runtime::COLLECTED_SAGA_STEPS
            .iter()
            .any(|descriptor| descriptor.binding.is_some())
        {
            return Err(saga_error(
                ApplicationPhase::Ready,
                "participant descriptor binding requires saga.participant.bindings",
            ));
        }
        grouped.insert(
            service_identity.as_str().to_owned(),
            (
                datasource.to_owned(),
                nasaga_runtime::COLLECTED_SAGA_STEPS.iter().collect(),
            ),
        );
    } else {
        for (binding, settings) in &settings.participant.bindings {
            let datasource = settings.datasource_ref.clone().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "every participant binding requires datasource_ref",
                )
            })?;
            grouped.insert(binding.clone(), (datasource, Vec::new()));
        }
        for descriptor in nasaga_runtime::COLLECTED_SAGA_STEPS {
            let binding = descriptor.binding.ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "every descriptor in a multi-datasource participant requires binding",
                )
            })?;
            let entry = grouped.get_mut(binding).ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "participant descriptor references an unknown binding",
                )
            })?;
            entry.1.push(descriptor);
        }
        if grouped
            .values()
            .any(|(_, descriptors)| descriptors.is_empty())
        {
            return Err(saga_error(
                ApplicationPhase::Ready,
                "every participant binding must own at least one descriptor",
            ));
        }
    }

    let mut plan = SagaApplicationPlan::new();
    let mut datasources = std::collections::BTreeSet::new();
    for (binding, (datasource, descriptors)) in grouped {
        let mut trusts = BTreeMap::new();
        let mut expected = std::collections::BTreeSet::new();
        for descriptor in descriptors {
            let workflow = nasaga_runtime::__private::core::WorkflowName::new(descriptor.workflow)
                .map_err(|error| {
                    saga_source_error(
                        ApplicationPhase::Ready,
                        "participant descriptor workflow is invalid",
                        anyhow::anyhow!(error.code()),
                    )
                })?;
            let version = nasaga_runtime::__private::core::DefinitionVersion::new(
                descriptor.definition_version,
            )
            .map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Ready,
                    "participant descriptor definition version is invalid",
                    anyhow::anyhow!(error.code()),
                )
            })?;
            nasaga_runtime::__private::core::StepName::new(descriptor.step).map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Ready,
                    "participant descriptor step is invalid",
                    anyhow::anyhow!(error.code()),
                )
            })?;
            expected.insert((
                descriptor.workflow.to_owned(),
                descriptor.definition_version,
                descriptor.step.to_owned(),
            ));
            trusts
                .entry((workflow.as_str().to_owned(), version.get()))
                .or_insert_with(|| {
                    nasaga_runtime::ParticipantCommandTrust::for_definition_version(
                        producer.clone(),
                        workflow,
                        version,
                    )
                });
        }
        let trusts = trusts.into_values().collect::<Vec<_>>();
        let driver = managed_datasource_driver(application, &datasource).await?;
        match driver {
            natx_core::DatabaseDriver::MySql => {
                #[cfg(feature = "saga")]
                {
                    let _pool = application.datasource(&datasource).await?;
                    // binding 的 Inbox、gate、业务事务与结果 Outbox 必须落在同一个明确数据源。
                    nasaga_runtime::ensure_participant_schema_for(&datasource)
                        .await
                        .map_err(|error| {
                            saga_source_error(
                                ApplicationPhase::Ready,
                                "managed MySQL participant schema is unavailable",
                                error,
                            )
                        })?;
                    let runtime = Arc::new(
                        nasaga_runtime::ParticipantRuntime::with_datasource(
                            consumer.clone(),
                            trusts,
                            &datasource,
                        )
                        .map_err(|error| {
                            saga_source_error(
                                ApplicationPhase::Ready,
                                "managed MySQL participant construction failed",
                                error,
                            )
                        })?,
                    );
                    let handlers =
                        build_mysql_managed_command_handlers(Arc::clone(&runtime), &expected)?;
                    plan.participants
                        .insert(binding.clone(), ManagedParticipant::MySql(runtime));
                    for (key, handler) in handlers {
                        if plan.command_handlers.insert(key, handler).is_some() {
                            return Err(saga_error(
                                ApplicationPhase::Ready,
                                "participant descriptor is assigned to more than one binding",
                            ));
                        }
                    }
                }
                #[cfg(not(feature = "saga"))]
                return Err(saga_error(
                    ApplicationPhase::Ready,
                    "managed participant datasource requires the MySQL Saga capability",
                ));
            }
            natx_core::DatabaseDriver::PostgreSql => {
                #[cfg(feature = "saga-pgsql")]
                {
                    let _pool = application.pg_datasource(&datasource).await?;
                    nasaga_runtime_pgsql::ensure_participant_schema_for(&datasource)
                        .await
                        .map_err(|error| {
                            saga_source_error(
                                ApplicationPhase::Ready,
                                "managed PostgreSQL participant schema is unavailable",
                                error,
                            )
                        })?;
                    let runtime = Arc::new(
                        nasaga_runtime_pgsql::PgParticipantRuntime::with_datasource(
                            consumer.clone(),
                            trusts,
                            &datasource,
                        )
                        .map_err(|error| {
                            saga_source_error(
                                ApplicationPhase::Ready,
                                "managed PostgreSQL participant construction failed",
                                error,
                            )
                        })?,
                    );
                    let handlers =
                        build_pgsql_managed_command_handlers(Arc::clone(&runtime), &expected)?;
                    plan.participants
                        .insert(binding.clone(), ManagedParticipant::PostgreSql(runtime));
                    for (key, handler) in handlers {
                        if plan.command_handlers.insert(key, handler).is_some() {
                            return Err(saga_error(
                                ApplicationPhase::Ready,
                                "participant descriptor is assigned to more than one binding",
                            ));
                        }
                    }
                }
                #[cfg(not(feature = "saga-pgsql"))]
                return Err(saga_error(
                    ApplicationPhase::Ready,
                    "managed participant datasource requires the PostgreSQL Saga capability",
                ));
            }
        }
        datasources.insert(datasource);
    }
    let registry = DefinitionRegistry::new();
    #[cfg_attr(
        not(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql")),
        allow(unused_mut)
    )]
    let mut publisher =
        build_managed_protocol_publisher(application, settings, &registry, &[]).await?;
    if managed_uses_capability_registry(settings) {
        match managed_registry_protocol(settings) {
            Some(SagaClientProtocol::Http) => {
                plan.capability_publish = Some(build_managed_capability_publish_plan(
                    application,
                    settings,
                    &service_identity,
                )?);
            }
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            Some(SagaClientProtocol::Grpc) => {
                plan.grpc_capability_publish = Some(build_managed_grpc_capability_publish_plan(
                    application,
                    settings,
                    &service_identity,
                )?);
            }
            _ => {
                return Err(saga_error(
                    ApplicationPhase::Ready,
                    "capability-registry routing requires a compiled registry client protocol",
                ));
            }
        }
    }
    plan.definition_publish =
        build_managed_definition_publish_plan(application, settings, &service_identity)?;
    #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
    {
        plan.redis_transport = publisher.redis_transport.take();
    }
    let publisher = Arc::new(publisher.publisher);
    for datasource in datasources {
        let outbox = crate::outbox::OutboxApplicationPlan::new(Arc::clone(&publisher))
            .with_datasource_ref(datasource)?
            .dead_letter_after(1)?;
        plan.outboxes.push(outbox);
    }
    #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
    register_managed_kafka_command_consumer(application, settings)?;
    Ok(plan)
}

/// 业务作用：在 Kafka Ready 封口前登记参与方 command consumer，使业务无需接触 registry builder。
///
/// 参数说明：`application` 提供受管 Kafka runtime，`settings` 提供 topic 前缀、身份与稳定 group。
///
/// 返回：非 Kafka transport 幂等跳过；route 完整且登记成功时完成，否则拒绝 Ready。
#[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
fn register_managed_kafka_command_consumer(
    application: &Application,
    settings: &SagaSettings,
) -> ApplicationResult<()> {
    let Some(kafka) = settings
        .transport
        .command_result
        .as_ref()
        .filter(|transport| transport.kind == Some(SagaTransportKind::Kafka))
        .and_then(|transport| transport.kafka.as_ref())
    else {
        return Ok(());
    };
    let client_ref = kafka.client_ref.as_deref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "managed Saga Kafka client_ref is missing",
        )
    })?;
    let kafka_handle = application.kafka(client_ref)?;
    if !kafka_handle.dead_letter_required() || kafka_handle.dead_letter_topic_suffix().is_empty() {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed Saga Kafka command consumer requires durable dead-letter persistence",
        ));
    }
    let command_prefix = kafka.command_topic.as_deref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "managed Saga Kafka command_topic is missing",
        )
    })?;
    let owner = settings
        .participant
        .service_identity
        .as_deref()
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "managed Saga Kafka participant identity is missing",
            )
        })?;
    let producer = nasaga_runtime::ServiceIdentity::new(
        settings
            .participant
            .orchestrator_identity
            .as_deref()
            .ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "managed Saga Kafka orchestrator identity is missing",
                )
            })?,
    )
    .map_err(|error| {
        saga_source_error(
            ApplicationPhase::Ready,
            "managed Saga Kafka orchestrator identity is invalid",
            error,
        )
    })?;
    let mut routes = BTreeMap::new();
    for descriptor in nasaga_runtime::COLLECTED_SAGA_STEPS {
        let topic = managed_kafka_step_topic(
            command_prefix,
            owner,
            descriptor.workflow,
            descriptor.definition_version,
            descriptor.step,
        )?;
        routes.insert(
            topic,
            (
                owner.to_owned(),
                descriptor.workflow.to_owned(),
                descriptor.definition_version,
                descriptor.step.to_owned(),
            ),
        );
    }
    let group = format!(
        "{}.saga-command",
        settings
            .participant
            .consumer_identity
            .as_deref()
            .ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "managed Saga Kafka participant consumer identity is missing",
                )
            })?
    );
    validate_managed_kafka_name(&group, "command group")?;
    let consumer = ManagedKafkaCommandConsumer {
        routes,
        group,
        producer,
        state: application.saga_runtime(),
        delivery_policy: nasaga_runtime::CommandDeliveryPolicy::default(),
    };
    application.kafka_runtime().push_customization(
        client_ref,
        Box::new(move |builder| builder.register(consumer)),
    )
}

/// 业务作用：在 Kafka Ready 封口前登记 Orchestrator result consumer，并用 owner topic 作为身份权威。
///
/// 参数说明：`application` 提供 Kafka runtime，`settings` 提供 result 前缀/group，`orchestrator` 是推进权威。
///
/// 返回：非 Kafka transport 幂等跳过；配置和 runtime 完整时登记成功，否则拒绝 Ready。
#[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
fn register_managed_kafka_result_consumer(
    application: &Application,
    settings: &SagaSettings,
    orchestrator: Option<&OrchestratorPlan>,
) -> ApplicationResult<()> {
    let Some(kafka) = settings
        .transport
        .command_result
        .as_ref()
        .filter(|transport| transport.kind == Some(SagaTransportKind::Kafka))
        .and_then(|transport| transport.kafka.as_ref())
    else {
        return Ok(());
    };
    let client_ref = kafka.client_ref.as_deref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "managed Saga Kafka client_ref is missing",
        )
    })?;
    let result_prefix = kafka.result_topic.as_deref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "managed Saga Kafka result_topic is missing",
        )
    })?;
    validate_managed_kafka_name(result_prefix, "result topic")?;
    let group = kafka.result_group.clone().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "managed Saga Kafka result_group is missing",
        )
    })?;
    validate_managed_kafka_name(&group, "result group")?;
    let dlt = kafka.result_dlt_topic.as_deref().ok_or_else(|| {
        saga_error(
            ApplicationPhase::Ready,
            "managed Saga Kafka result_dlt_topic is missing",
        )
    })?;
    let kafka_handle = application.kafka(client_ref)?;
    if !kafka_handle.dead_letter_required() || kafka_handle.dead_letter_topic_suffix().is_empty() {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed Saga Kafka requires durable dead-letter persistence",
        ));
    }
    let expected_dlt = format!(
        "{result_prefix}.{{owner}}{}",
        kafka_handle.dead_letter_topic_suffix()
    );
    if dlt != expected_dlt {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed Saga Kafka result_dlt_topic must match the owner topic and client DLT suffix",
        ));
    }
    let orchestrator = orchestrator
        .map(|plan| plan.runtime.clone())
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "managed Saga Kafka result consumer has no Orchestrator runtime",
            )
        })?;
    let topic_prefix = format!("{result_prefix}.");
    let topic_pattern = format!(
        "^{}\\.[A-Za-z0-9_-]+$",
        managed_kafka_regex_literal(result_prefix)
    );
    let consumer = ManagedKafkaResultConsumer {
        topic_pattern,
        topic_prefix,
        group,
        orchestrator,
        state: application.saga_runtime(),
        delivery_policy: nasaga_runtime::ResultDeliveryPolicy::default(),
    };
    application.kafka_runtime().push_customization(
        client_ref,
        Box::new(move |builder| builder.register(consumer)),
    )
}

/// 业务作用：把配置中的逻辑服务到 mTLS leaf 指纹映射冻结为反向认证表。
///
/// 参数说明：`configured` 的 key 是 ServiceIdentity，value 是 nagrpc 发布的 SHA-256 principal。
///
/// 返回：身份与指纹均合法且一一对应时返回映射；空表、重复指纹或非法值拒绝 Start。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn build_managed_grpc_peer_map(
    application: &Application,
    configured: &BTreeMap<String, String>,
) -> ApplicationResult<security::GrpcPeerBindings<nasaga_runtime::ServiceIdentity>> {
    if configured.is_empty() {
        return Err(saga_error(
            ApplicationPhase::Start,
            "managed Saga gRPC requires at least one peer principal binding",
        ));
    }
    let mut peers = BTreeMap::new();
    for (identity, principal) in configured {
        let identity = nasaga_runtime::ServiceIdentity::new(identity).map_err(|error| {
            saga_source_error(
                ApplicationPhase::Start,
                "managed Saga gRPC peer service identity is invalid",
                error,
            )
        })?;
        validate_managed_grpc_principal(principal, ApplicationPhase::Start)?;
        if peers.insert(principal.clone(), identity).is_some() {
            return Err(saga_error(
                ApplicationPhase::Start,
                "managed Saga gRPC peer principal is assigned more than once",
            ));
        }
    }
    security::GrpcPeerBindings::new(application, peers)
}

/// 业务作用：验证 principal 与 nagrpc TLS 扩展使用同一稳定证书指纹格式。
///
/// 参数说明：`principal` 必须是 `sha256:` 加 64 个小写十六进制字符。
///
/// 返回：格式合法时成功；其它输入拒绝生命周期推进。
fn validate_managed_grpc_principal(
    principal: &str,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    if let Some(reference) = principal.strip_prefix("secret://") {
        if !reference.is_empty()
            && reference.len() <= 128
            && reference.split('/').all(|segment| {
                !segment.is_empty()
                    && segment != "."
                    && segment != ".."
                    && segment.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
                    })
            })
        {
            return Ok(());
        }
        return Err(saga_error(
            phase,
            "managed Saga gRPC certificate reference is invalid",
        ));
    }
    let digest = principal.strip_prefix("sha256:").unwrap_or_default();
    if digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(saga_error(phase, "managed Saga gRPC principal is invalid"))
    }
}

/// 业务作用：在 gRPC Prepare 封口前按 managed 角色自动登记 command/result generated service。
///
/// 参数说明：`application` 提供唯一 service registry 与 Saga 状态，`settings` 冻结角色和 peer 映射。
///
/// 返回：非 gRPC 数据面幂等跳过；角色允许的 service 全部登记成功时完成，冲突或安全缺口拒绝 Start。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn register_managed_grpc_transport_services(
    application: &Application,
    settings: &SagaSettings,
) -> ApplicationResult<()> {
    let Some(grpc) = settings
        .transport
        .command_result
        .as_ref()
        .filter(|transport| transport.kind == Some(SagaTransportKind::Grpc))
        .and_then(|transport| transport.grpc.as_ref())
    else {
        return Ok(());
    };
    let peers = build_managed_grpc_peer_map(application, &grpc.peer_principals)?;
    let role = settings
        .role
        .ok_or_else(|| saga_error(ApplicationPhase::Start, "saga.role is required"))?;
    if matches!(role, SagaRole::Participant | SagaRole::Combined) {
        let orchestrator = settings
            .participant
            .orchestrator_identity
            .as_deref()
            .ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Start,
                    "managed Saga gRPC participant requires orchestrator identity",
                )
            })?;
        if peers
            .values()
            .all(|identity| identity.as_str() != orchestrator)
        {
            return Err(saga_error(
                ApplicationPhase::Start,
                "managed Saga gRPC command peer does not bind the configured Orchestrator",
            ));
        }
        application.grpc_runtime().register_boxed(Box::new(
            nasaga_runtime::grpc_proto::saga_command_transport_server::SagaCommandTransportServer::new(
                ManagedGrpcCommandService {
                    state: application.saga_runtime(),
                    peers: peers.clone(),
                },
            ),
        ))?;
    }
    if matches!(role, SagaRole::Orchestrator | SagaRole::Combined) {
        application.grpc_runtime().register_boxed(Box::new(
            nasaga_runtime::grpc_proto::saga_result_transport_server::SagaResultTransportServer::new(
                ManagedGrpcResultService {
                    state: application.saga_runtime(),
                    peers,
                },
            ),
        ))?;
    }
    Ok(())
}

/// 业务作用：把 gRPC API caller 配置冻结为 principal 到最小权限快照的反向认证表。
///
/// 参数说明：`api` 声明证书 principal、逻辑主体、租户、权限与高权限暴露面。
///
/// 返回：全部映射合法且 principal 唯一时返回；空安全表、非法身份或空权限拒绝 Start。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn build_managed_grpc_api_callers(
    application: &Application,
    api: &SagaGrpcApiSettings,
) -> ApplicationResult<security::GrpcPeerBindings<SagaApiActor>> {
    validate_managed_grpc_api_settings(api, ApplicationPhase::Start)?;
    let mut callers = BTreeMap::new();
    for (principal, settings) in &api.callers {
        let identity = nasaga_runtime::ServiceIdentity::new(
            settings.service_identity.as_deref().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Start,
                    "managed Saga gRPC API caller identity is missing",
                )
            })?,
        )
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Start,
                "managed Saga gRPC API caller identity is invalid",
                error,
            )
        })?;
        let tenants = settings
            .tenants
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let permissions = settings
            .permissions
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let workflows = settings
            .workflows
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        callers.insert(
            principal.clone(),
            SagaApiActor {
                identity,
                tenants,
                workflows,
                permissions,
            },
        );
    }
    security::GrpcPeerBindings::new(application, callers)
}

/// 业务作用：从 SecretSnapshot 冻结 definition 发布验签公钥，避免注册入口信任请求自带密钥。
///
/// 参数说明：`application` 提供脱离普通配置树的 secret material，`configured` 把稳定 key id 映射到 secret reference。
///
/// 返回：每个引用都解析为无空白的 Ed25519 SPKI Base64 公钥时返回只读映射；空表、重复或缺失材料拒绝 Start。
fn load_managed_definition_signing_keys(
    application: &Application,
    configured: &BTreeMap<String, String>,
    phase: ApplicationPhase,
) -> ApplicationResult<BTreeMap<String, String>> {
    if configured.is_empty() {
        return Err(saga_error(
            phase,
            "managed Saga Definition Registry requires signing keys",
        ));
    }
    let secrets = application.secrets();
    let mut keys = BTreeMap::new();
    for (key_id, secret_ref) in configured {
        validate_runtime_name(key_id, "definition signing key id", phase)?;
        let material = secrets.get(secret_ref).ok_or_else(|| {
            saga_error(phase, "managed Saga definition signing key is unavailable")
        })?;
        let public_key = std::str::from_utf8(material.expose()).map_err(|error| {
            saga_source_error(
                phase,
                "managed Saga definition signing key is not UTF-8",
                error,
            )
        })?;
        if public_key.is_empty() || public_key.trim() != public_key {
            return Err(saga_error(
                phase,
                "managed Saga definition signing key is invalid",
            ));
        }
        keys.insert(key_id.clone(), public_key.to_owned());
    }
    Ok(keys)
}

/// 业务作用：从共享 secret 派生跨副本稳定的分页签名密钥，使 token 在滚动发布和故障转移后仍可验证。
///
/// 参数说明：`application` 提供 secret 快照，`credential_ref` 是独立密钥引用，`phase` 标记构造阶段。
///
/// 返回：非空且不少于三十二字节的材料派生为固定 SHA-256 密钥；缺失或过短时拒绝开放查询 API。
fn load_managed_page_token_key(
    application: &Application,
    credential_ref: &str,
    phase: ApplicationPhase,
) -> ApplicationResult<[u8; 32]> {
    let secrets = application.secrets();
    let material = secrets
        .get(credential_ref)
        .ok_or_else(|| saga_error(phase, "managed Saga page token signing key is unavailable"))?;
    if material.expose().len() < 32 {
        return Err(saga_error(
            phase,
            "managed Saga page token signing key is too short",
        ));
    }
    Ok(Sha256::digest(material.expose()).into())
}

/// 业务作用：在 gRPC Prepare 前按 exposure 策略自动登记公开 Orchestrator 与管理 service。
///
/// 参数说明：Application 提供 registry/runtime，settings 固定 exposure、RBAC 和 Orchestrator 角色。
///
/// 返回：未启用幂等跳过；服务名、权限和角色完整时登记成功，否则拒绝 Start。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn register_managed_grpc_api_services(
    application: &Application,
    settings: &SagaSettings,
) -> ApplicationResult<()> {
    if !settings.api.grpc.enabled {
        return Ok(());
    }
    if !matches!(
        settings.role,
        Some(SagaRole::Orchestrator | SagaRole::Combined)
    ) {
        return Err(saga_error(
            ApplicationPhase::Start,
            "managed Saga gRPC API requires an Orchestrator role",
        ));
    }
    let callers = build_managed_grpc_api_callers(application, &settings.api.grpc)?;
    let definition_signing_keys = if settings.api.grpc.expose_definition_registry {
        load_managed_definition_signing_keys(
            application,
            &settings.definition_catalog.signing_keys,
            ApplicationPhase::Start,
        )?
    } else {
        BTreeMap::new()
    };
    let activation =
        managed_definition_activation_contract(application, settings, ApplicationPhase::Start)?;
    let service = Arc::new(ManagedGrpcApiService {
        state: application.saga_runtime(),
        callers,
        definition_signing_keys,
        orchestrator_service_identity: nasaga_runtime::ServiceIdentity::new(
            settings.service_identity.as_deref().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Start,
                    "managed gRPC Orchestrator service identity is missing",
                )
            })?,
        )
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Start,
                "managed gRPC Orchestrator service identity is invalid",
                error,
            )
        })?,
        activation,
        page_token_key: load_managed_page_token_key(
            application,
            settings.api.page_token_key_ref.as_deref().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Start,
                    "managed Saga API page token key reference is missing",
                )
            })?,
            ApplicationPhase::Start,
        )?,
    });
    application.grpc_runtime().register_boxed(Box::new(
        nasaga_runtime::orchestrator_proto::saga_orchestrator_server::SagaOrchestratorServer::from_arc(
            Arc::clone(&service),
        ),
    ))?;
    if settings.api.grpc.expose_admin {
        application.grpc_runtime().register_boxed(Box::new(
            nasaga_runtime::orchestrator_proto::saga_orchestrator_admin_server::SagaOrchestratorAdminServer::from_arc(
                Arc::clone(&service),
            ),
        ))?;
    }
    if settings.api.grpc.expose_definition_registry {
        application.grpc_runtime().register_boxed(Box::new(
            nasaga_runtime::orchestrator_proto::saga_definition_registry_server::SagaDefinitionRegistryServer::from_arc(
                Arc::clone(&service),
            ),
        ))?;
    }
    Ok(())
}

/// 业务作用：按当前数据面为本地步骤生成可续租的完整 capability 路由合同。
///
/// 参数说明：`application` 提供 Web 实际 context，`settings` 冻结数据面，`owner` 是认证服务身份。
///
/// 返回：每个链接步骤都能得到唯一实例路由时返回 descriptor；端点或组件缺失时拒绝 Ready。
#[cfg(any(
    feature = "web",
    feature = "saga-grpc",
    feature = "saga-grpc-pgsql",
    feature = "saga-kafka",
    feature = "saga-kafka-pgsql",
    feature = "saga-redis-stream",
    feature = "saga-redis-stream-pgsql"
))]
fn build_managed_local_capabilities(
    application: &Application,
    settings: &SagaSettings,
    owner: &nasaga_runtime::ServiceIdentity,
) -> ApplicationResult<(Vec<nasaga_runtime::CapabilityDescriptor>, Option<String>)> {
    let _ = application;
    let transport = settings
        .transport
        .command_result
        .as_ref()
        .and_then(|transport| transport.kind)
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "capability publication requires command_result transport",
            )
        })?;
    let replica_identity = settings
        .participant
        .consumer_identity
        .clone()
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "capability publication requires participant consumer identity",
            )
        })?;
    let (
        transport_name,
        endpoint,
        effective_saga_base_path,
        result_contract_digest,
        advertised_endpoint,
    ) = match transport {
        SagaTransportKind::Http => {
            #[cfg(feature = "web")]
            {
                let http = settings
                    .transport
                    .command_result
                    .as_ref()
                    .and_then(|transport| transport.http.as_ref())
                    .ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "HTTP capability settings are missing",
                        )
                    })?;
                let web = application.web()?;
                let context_path = web.context_path().trim_end_matches('/');
                let base_path = settings.http.base_path.as_deref().unwrap_or("/_nasa/saga");
                let effective = if context_path.is_empty() {
                    base_path.to_owned()
                } else {
                    format!("{context_path}{base_path}")
                };
                validate_saga_base_path(&effective, ApplicationPhase::Ready)?;
                let result_contract_digest = managed_http_result_contract_digest(
                    application,
                    http.result_credential_ref.as_deref().ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "HTTP capability publication requires result_credential_ref",
                        )
                    })?,
                    ApplicationPhase::Ready,
                )?;
                (
                    "http",
                    http.advertised_endpoint
                        .as_deref()
                        .map(|endpoint| {
                            normalize_managed_capability_endpoint(endpoint, ApplicationPhase::Ready)
                        })
                        .transpose()?
                        .unwrap_or_default(),
                    Some(effective),
                    Some(result_contract_digest),
                    http.advertised_endpoint.clone(),
                )
            }
            #[cfg(not(feature = "web"))]
            return Err(saga_error(
                ApplicationPhase::Ready,
                "HTTP capability publication requires the Web capability",
            ));
        }
        SagaTransportKind::Grpc => {
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            {
                let grpc = settings
                    .transport
                    .command_result
                    .as_ref()
                    .and_then(|transport| transport.grpc.as_ref())
                    .ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "gRPC capability settings are missing",
                        )
                    })?;
                let endpoint = grpc.advertised_endpoint.clone().ok_or_else(|| {
                    saga_error(
                        ApplicationPhase::Ready,
                        "gRPC capability publication requires advertised_endpoint",
                    )
                })?;
                if !endpoint.starts_with("https://") {
                    return Err(saga_error(
                        ApplicationPhase::Ready,
                        "gRPC capability advertised_endpoint must use HTTPS with mTLS",
                    ));
                }
                nagrpc::Endpoint::from_shared(endpoint.clone()).map_err(|error| {
                    saga_source_error(
                        ApplicationPhase::Ready,
                        "gRPC capability advertised_endpoint is invalid",
                        error,
                    )
                })?;
                let credential = security::grpc_credential(
                    application,
                    grpc.credential_ref.as_deref().ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "gRPC capability publication requires credential_ref",
                        )
                    })?,
                )?;
                (
                    "grpc",
                    endpoint,
                    None,
                    Some(managed_grpc_result_contract_digest(
                        &credential,
                        ApplicationPhase::Ready,
                    )?),
                    None,
                )
            }
            #[cfg(not(any(feature = "saga-grpc", feature = "saga-grpc-pgsql")))]
            return Err(saga_error(
                ApplicationPhase::Ready,
                "gRPC capability publication is not compiled into this application",
            ));
        }
        SagaTransportKind::Kafka => {
            #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
            {
                let kafka = settings
                    .transport
                    .command_result
                    .as_ref()
                    .and_then(|transport| transport.kafka.as_ref())
                    .ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "Kafka capability settings are missing",
                        )
                    })?;
                let prefix = kafka.command_topic.as_deref().ok_or_else(|| {
                    saga_error(
                        ApplicationPhase::Ready,
                        "Kafka capability publication requires command_topic",
                    )
                })?;
                (
                    "kafka",
                    prefix.to_owned(),
                    None,
                    Some(managed_kafka_result_backend_digest(
                        application,
                        settings,
                        ApplicationPhase::Ready,
                    )?),
                    None,
                )
            }
            #[cfg(not(any(feature = "saga-kafka", feature = "saga-kafka-pgsql")))]
            return Err(saga_error(
                ApplicationPhase::Ready,
                "Kafka capability publication is not compiled into this application",
            ));
        }
        SagaTransportKind::RedisStream => {
            #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
            {
                let stream = settings
                    .transport
                    .command_result
                    .as_ref()
                    .and_then(|transport| transport.redis_stream.as_ref())
                    .and_then(|stream| stream.command_stream.clone())
                    .ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "Redis Streams capability publication requires command_stream",
                        )
                    })?;
                let redis = settings
                    .transport
                    .command_result
                    .as_ref()
                    .and_then(|transport| transport.redis_stream.as_ref())
                    .expect("selected Redis settings were checked");
                let credentials = load_managed_redis_credentials(
                    application,
                    redis.credential_ref.as_deref().ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "Redis capability publication requires credential_ref",
                        )
                    })?,
                )?;
                let signing = credentials
                    .signing_keys
                    .get(owner.as_str())
                    .ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "Redis capability owner has no result signing key",
                        )
                    })?;
                (
                    "redis-stream",
                    stream,
                    None,
                    Some(managed_redis_result_contract_digest(
                        &signing.key_id,
                        &signing.key_hex,
                        ApplicationPhase::Ready,
                    )?),
                    None,
                )
            }
            #[cfg(not(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql")))]
            return Err(saga_error(
                ApplicationPhase::Ready,
                "Redis capability publication is not compiled into this application",
            ));
        }
    };
    if settings.definition_catalog.publish_tenants.is_empty() {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "capability publication requires definition_catalog.publish_tenants",
        ));
    }
    let mut descriptors = Vec::new();
    for tenant in &settings.definition_catalog.publish_tenants {
        nasaga_runtime::__private::core::TenantId::new(tenant).map_err(|error| {
            saga_source_error(
                ApplicationPhase::Ready,
                "capability publication tenant is invalid",
                error,
            )
        })?;
        for descriptor in nasaga_runtime::COLLECTED_SAGA_STEPS {
            let endpoint = match transport {
                #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
                SagaTransportKind::Kafka => managed_kafka_step_topic(
                    &endpoint,
                    owner.as_str(),
                    descriptor.workflow,
                    descriptor.definition_version,
                    descriptor.step,
                )?,
                SagaTransportKind::RedisStream => managed_redis_step_stream(
                    &endpoint,
                    owner.as_str(),
                    descriptor.workflow,
                    descriptor.definition_version,
                    descriptor.step,
                )?,
                _ => endpoint.clone(),
            };
            let capability = nasaga_runtime::CapabilityDescriptor {
                payload_contract: {
                    let contract = nasaga_runtime::SagaPayloadContract {
                        content_type: descriptor.payload_content_type.to_owned(),
                        schema_id: descriptor.payload_schema_id.to_owned(),
                    };
                    (contract != nasaga_runtime::SagaPayloadContract::default()).then_some(contract)
                },
                tenant: tenant.clone(),
                owner: owner.as_str().to_owned(),
                replica_identity: replica_identity.clone(),
                workflow: descriptor.workflow.to_owned(),
                definition_version: descriptor.definition_version,
                step: descriptor.step.to_owned(),
                compensation: descriptor.compensation.into(),
                cancel_mode: descriptor.cancel_mode.into(),
                allow_unknown: descriptor.allow_unknown,
                resolution_mode: descriptor.resolution_mode.map(Into::into),
                transport: transport_name.to_owned(),
                endpoint,
                effective_saga_base_path: effective_saga_base_path.clone(),
                result_contract_digest: result_contract_digest.clone(),
                route_generation: 1,
                requested_lease_ms: 30_000,
            };
            // 动态 listener 尚未绑定时只延后其 endpoint 校验；显式地址和其它数据面立即封闭合同。
            // 所有 descriptor 在真正发布前还必须通过同一个完整校验器。
            if capability.transport != "http" || !capability.endpoint.is_empty() {
                nasaga_runtime::validate_capability_route_contract(&capability).map_err(
                    |error| {
                        saga_source_error(
                            ApplicationPhase::Ready,
                            "managed Saga capability route contract is invalid",
                            error,
                        )
                    },
                )?;
            }
            descriptors.push(capability);
        }
    }
    if descriptors.is_empty() {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "capability publication requires at least one local Saga step",
        ));
    }
    Ok((descriptors, advertised_endpoint))
}

/// 业务作用：允许纯数据库应用编译，仅在实际请求发布受管能力时拒绝缺失的数据面。
///
/// 参数说明：`_application`、`_settings`、`_owner` 保留受管能力构建合同。
///
/// 返回：未编译任何 command/result transport 时返回明确的 Ready 阶段错误。
#[cfg(not(any(
    feature = "web",
    feature = "saga-grpc",
    feature = "saga-grpc-pgsql",
    feature = "saga-kafka",
    feature = "saga-kafka-pgsql",
    feature = "saga-redis-stream",
    feature = "saga-redis-stream-pgsql"
)))]
fn build_managed_local_capabilities(
    _application: &Application,
    _settings: &SagaSettings,
    _owner: &nasaga_runtime::ServiceIdentity,
) -> ApplicationResult<(Vec<nasaga_runtime::CapabilityDescriptor>, Option<String>)> {
    // 没有数据面能力时不能声明可接收命令，纯数据库自定义计划不会进入此入口。
    Err(saga_error(
        ApplicationPhase::Ready,
        "capability publication requires a compiled command/result transport",
    ))
}

/// 业务作用：从独立控制面配置选择 capability registry 协议，并为存量 HTTP/gRPC 数据面保留明确兼容路径。
///
/// 参数说明：`settings` 是已封闭的 Saga 配置。
///
/// 返回：显式 registry client 优先；未配置时仅 HTTP/gRPC 数据面可以复用同协议控制端点。
fn managed_registry_protocol(settings: &SagaSettings) -> Option<SagaClientProtocol> {
    settings
        .definition_catalog
        .registry_client
        .as_ref()
        .and_then(|client| client.protocol)
        .or_else(|| {
            match settings
                .transport
                .command_result
                .as_ref()
                .and_then(|transport| transport.kind)
            {
                Some(SagaTransportKind::Http) => Some(SagaClientProtocol::Http),
                Some(SagaTransportKind::Grpc) => Some(SagaClientProtocol::Grpc),
                _ => None,
            }
        })
}

/// 业务作用：读取当前数据面的 owner route 是否以动态 capability 为唯一权威。
///
/// 参数说明：`settings` 是已封闭的 Saga 配置。
///
/// 返回：选中协议的 `routing.mode` 为 `capability-registry` 时返回真。
fn managed_uses_capability_registry(settings: &SagaSettings) -> bool {
    let Some(transport) = settings.transport.command_result.as_ref() else {
        return false;
    };
    let routing = match transport.kind {
        Some(SagaTransportKind::Http) => transport.http.as_ref().map(|value| &value.routing),
        Some(SagaTransportKind::Grpc) => transport.grpc.as_ref().map(|value| &value.routing),
        Some(SagaTransportKind::Kafka) => transport.kafka.as_ref().map(|value| &value.routing),
        Some(SagaTransportKind::RedisStream) => {
            transport.redis_stream.as_ref().map(|value| &value.routing)
        }
        None => None,
    };
    routing.is_some_and(|routing| routing.mode.as_deref() == Some("capability-registry"))
}

/// 业务作用：从本地步骤合同、实例身份与 Web 路径构造受管 capability 续租计划。
///
/// 参数说明：`application` 提供凭据与 context path，`settings` 提供受信协议地址，`owner` 是本地步骤逻辑主体。
///
/// 返回：全部 descriptor 与签名目标合法时返回计划；无本地步骤、凭据或路由不完整时拒绝 Ready。
fn build_managed_capability_publish_plan(
    application: &Application,
    settings: &SagaSettings,
    owner: &nasaga_runtime::ServiceIdentity,
) -> ApplicationResult<ManagedCapabilityPublishPlan> {
    let http_transport = settings
        .transport
        .command_result
        .as_ref()
        .and_then(|transport| transport.http.as_ref());
    let registry = settings.definition_catalog.registry_client.as_ref();
    let orchestrator = registry
        .and_then(|client| client.discovery_ref.as_deref())
        .or_else(|| http_transport.and_then(|http| http.orchestrator_discovery_ref.as_deref()))
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "capability publication requires an orchestrator discovery route",
            )
        })?;
    let credential = registry
        .and_then(|client| client.credential_ref.as_deref())
        .or_else(|| http_transport.and_then(|http| http.result_credential_ref.as_deref()))
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "capability publication requires a registry credential",
            )
        })?;
    let authenticator = load_managed_http_authenticator(application, credential)?;
    let target = managed_http_discovery_target(
        application,
        settings,
        orchestrator,
        "registry/capabilities",
        authenticator,
        Duration::from_millis(
            http_transport
                .and_then(|http| http.request_timeout_ms)
                .unwrap_or(5_000),
        ),
    )?;
    let (descriptors, advertised_endpoint) =
        build_managed_local_capabilities(application, settings, owner)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(
            http_transport
                .and_then(|http| http.request_timeout_ms)
                .unwrap_or(5_000),
        ))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Ready,
                "capability registry HTTP client construction failed",
                error,
            )
        })?;
    Ok(ManagedCapabilityPublishPlan {
        result_source: security::CapabilityResultSource::new(application, settings)?,
        client,
        target,
        producer: owner.clone(),
        descriptors,
        advertised_endpoint,
        lease_ms: 30_000,
    })
}

/// 业务作用：从本地步骤合同和 gRPC mTLS 控制面构造自动 capability 续租计划。
///
/// 参数说明：`application` 提供 client secret，`settings` 固定 Orchestrator 与本副本公开端点，`owner` 是认证逻辑服务。
///
/// 返回：发现地址、公开 HTTPS 端点、凭据和 descriptor 完整时返回计划；任一安全边界缺失时拒绝 Ready。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
fn build_managed_grpc_capability_publish_plan(
    application: &Application,
    settings: &SagaSettings,
    owner: &nasaga_runtime::ServiceIdentity,
) -> ApplicationResult<ManagedGrpcCapabilityPublishPlan> {
    let grpc_transport = settings
        .transport
        .command_result
        .as_ref()
        .and_then(|transport| transport.grpc.as_ref());
    let registry = settings.definition_catalog.registry_client.as_ref();
    let orchestrator = registry
        .and_then(|client| client.discovery_ref.as_deref())
        .or_else(|| grpc_transport.and_then(|grpc| grpc.orchestrator_discovery_ref.as_deref()))
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "gRPC capability publication requires an Orchestrator endpoint",
            )
        })?;
    let credential_ref = registry
        .and_then(|client| client.credential_ref.as_deref())
        .or_else(|| grpc_transport.and_then(|grpc| grpc.credential_ref.as_deref()))
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Ready,
                "gRPC capability publication requires credential_ref",
            )
        })?;
    let credential = security::grpc_credential(application, credential_ref)?;
    let target = managed_grpc_discovery_target(
        application,
        settings,
        orchestrator,
        Duration::from_millis(
            grpc_transport
                .and_then(|grpc| grpc.request_timeout_ms)
                .unwrap_or(5_000),
        ),
        &credential,
    )?;
    let (descriptors, advertised_endpoint) =
        build_managed_local_capabilities(application, settings, owner)?;
    Ok(ManagedGrpcCapabilityPublishPlan {
        result_source: security::CapabilityResultSource::new(application, settings)?,
        target,
        descriptors,
        advertised_endpoint,
        lease_seconds: 30,
    })
}

/// 业务作用：让 custom 计划与 managed 计划共用相同的角色 schema 门禁，禁止高级入口绕过建表和最终复验。
///
/// 参数说明：`datasource` 是计划的单一事务域，`driver` 来自冻结 catalog，`role` 决定允许建立的表集。
///
/// 返回：角色所需结构全部可用时成功；任一后端操作失败则拒绝 Ready。
async fn ensure_custom_role_schema(
    datasource: &str,
    driver: natx_core::DatabaseDriver,
    role: SagaRole,
) -> ApplicationResult<()> {
    match driver {
        natx_core::DatabaseDriver::MySql => {
            #[cfg(feature = "saga")]
            {
                if matches!(role, SagaRole::Orchestrator | SagaRole::Combined) {
                    nasaga_runtime::ensure_orchestrator_schema_for(datasource)
                        .await
                        .map_err(|error| {
                            saga_source_error(
                                ApplicationPhase::Ready,
                                "custom MySQL orchestrator schema is unavailable",
                                error,
                            )
                        })?;
                }
                if matches!(role, SagaRole::Participant | SagaRole::Combined) {
                    nasaga_runtime::ensure_participant_schema_for(datasource)
                        .await
                        .map_err(|error| {
                            saga_source_error(
                                ApplicationPhase::Ready,
                                "custom MySQL participant schema is unavailable",
                                error,
                            )
                        })?;
                }
            }
            #[cfg(not(feature = "saga"))]
            return Err(saga_error(
                ApplicationPhase::Ready,
                "custom plan requires the MySQL Saga capability",
            ));
        }
        natx_core::DatabaseDriver::PostgreSql => {
            #[cfg(feature = "saga-pgsql")]
            {
                if matches!(role, SagaRole::Orchestrator | SagaRole::Combined) {
                    nasaga_runtime_pgsql::ensure_orchestrator_schema_for(datasource)
                        .await
                        .map_err(|error| {
                            saga_source_error(
                                ApplicationPhase::Ready,
                                "custom PostgreSQL orchestrator schema is unavailable",
                                error,
                            )
                        })?;
                }
                if matches!(role, SagaRole::Participant | SagaRole::Combined) {
                    nasaga_runtime_pgsql::ensure_participant_schema_for(datasource)
                        .await
                        .map_err(|error| {
                            saga_source_error(
                                ApplicationPhase::Ready,
                                "custom PostgreSQL participant schema is unavailable",
                                error,
                            )
                        })?;
                }
            }
            #[cfg(not(feature = "saga-pgsql"))]
            return Err(saga_error(
                ApplicationPhase::Ready,
                "custom plan requires the PostgreSQL Saga capability",
            ));
        }
    }
    Ok(())
}

impl ApplicationComponent for SagaComponent {
    /// 业务作用：返回 Saga 生命周期的稳定组件身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`ComponentId::Saga`。
    fn id(&self) -> ComponentId {
        ComponentId::Saga
    }

    /// 业务作用：声明 Saga 持久化门禁必须建立在受管数据库组件之上。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只包含 `ComponentId::Db` 的静态依赖；Outbox 位于 Saga 之后，由组合规范另行强制。
    fn dependencies(&self) -> &'static [ComponentId] {
        &[ComponentId::Db]
    }

    /// 业务作用：校验 timer 预算并注册初始未就绪的关键贡献项。
    ///
    /// 参数说明：
    /// - `context`：提供最终初始配置和动态就绪注册入口的 Start 上下文。
    ///
    /// 返回：配置与贡献项注册成功时完成；非法预算或重名时阻止进入 UserHook。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let settings = read_saga_settings(context.application(), ApplicationPhase::Start)?;
            context
                .application()
                .metrics_hub()
                .register_legacy_source_reserved(
                    Arc::new(latency_metrics::SagaLatencyMetricsSource),
                    5,
                )
                .map_err(|error| {
                    saga_error(
                        ApplicationPhase::Start,
                        format!("Saga stage metrics registration failed: {error:?}"),
                    )
                })?;

            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            if settings.plan_mode == SagaPlanMode::Managed {
                // gRPC registry 在 Prepare 永久封口，因此 managed transport 必须在 Start 完成登记。
                register_managed_grpc_transport_services(context.application(), &settings)?;
                register_managed_grpc_api_services(context.application(), &settings)?;
            }
            let timer_failure_threshold = settings.timer_settings().3;
            let contributor = context.application().register_readiness(
                ComponentId::Saga,
                Arc::<str>::from("saga:runtime"),
                ReadinessPolicy {
                    affects_ready: true,
                    failure_threshold: timer_failure_threshold,
                    recovery_threshold: 1,
                    stale_after: None,
                },
            )?;
            self.catalog_contributor = Some(context.application().register_readiness(
                ComponentId::Saga,
                Arc::<str>::from("saga:definition-catalog"),
                ReadinessPolicy {
                    affects_ready: true,
                    failure_threshold: 1,
                    recovery_threshold: 1,
                    stale_after: None,
                },
            )?);
            self.capability_contributor = Some(context.application().register_readiness(
                ComponentId::Saga,
                Arc::<str>::from("saga:capability-publication"),
                ReadinessPolicy {
                    affects_ready: true,
                    failure_threshold: 1,
                    recovery_threshold: 1,
                    stale_after: None,
                },
            )?);
            self.definition_publish_contributor = Some(context.application().register_readiness(
                ComponentId::Saga,
                Arc::<str>::from("saga:definition-publication"),
                ReadinessPolicy {
                    affects_ready: true,
                    failure_threshold: 1,
                    recovery_threshold: 1,
                    stale_after: None,
                },
            )?);
            // 就绪注册表在 UserHook 完成时封口,而计划要到 UserHook 才提交:此处必须
            // 先注册 stream 贡献项占位;Ready 阶段若计划不含 Redis transport,占位被
            // 一次性置绿中和,不影响未启用者。stream 指标源不在此注册——它的序列数取决于
            // 计划冻结后的 poller 数,推迟到 Ready 以精确预算注册。
            #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
            {
                self.stream_contributor = Some(context.application().register_readiness(
                    ComponentId::Saga,
                    Arc::<str>::from("saga:redis-stream"),
                    ReadinessPolicy {
                        affects_ready: true,
                        failure_threshold: timer_failure_threshold,
                        recovery_threshold: 1,
                        stale_after: None,
                    },
                )?);
            }
            self.settings = Some(settings);
            self.contributor = Some(contributor);
            Ok(())
        })
    }

    /// 业务作用：封口计划、执行合同与历史实例门禁，最后发布能力并启动 durable timer。
    ///
    /// 参数说明：
    /// - `context`：提供 Application 共享状态与反向停机 action 登记入口的 Ready 上下文。
    ///
    /// 返回：全部门禁通过后发布 Ready；任何校验或持久化读取失败都保持零对外能力并拒绝启动。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let application = context.application().clone();
            let state = application.saga_runtime();
            let settings = self
                .settings
                .clone()
                .ok_or_else(|| saga_error(ApplicationPhase::Ready, "saga settings are missing"))?;
            let configured_plan = state.take_configured_plan();
            #[cfg_attr(
                not(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql")),
                allow(unused_mut)
            )]
            let mut plan = match settings.plan_mode {
                SagaPlanMode::Managed => {
                    if configured_plan.is_some() {
                        return Err(saga_error(
                            ApplicationPhase::Ready,
                            "managed Saga cannot accept a custom runtime plan",
                        ));
                    }
                    let mut plan = Self::build_managed_plan(&application, &settings).await?;
                    // managed 模式由 Saga 组件在 Outbox Ready 前移交唯一 publisher；
                    // 移交成功后计划不再保留第二个可替换的发布权威。
                    if !plan.outboxes.is_empty() {
                        for outbox in plan.take_outbox_plans()? {
                            application.outbox_runtime().configure(outbox)?;
                        }
                    } else if settings.role != Some(SagaRole::Client) {
                        return Err(saga_error(
                            ApplicationPhase::Ready,
                            "managed Saga data role requires an Outbox publishing plan",
                        ));
                    }
                    plan
                }
                SagaPlanMode::Custom => configured_plan.ok_or_else(|| {
                    saga_error(
                        ApplicationPhase::Ready,
                        "custom Saga requires configure_saga during the application startup hook",
                    )
                })?,
            };
            #[cfg(feature = "web")]
            let http_server = if settings.plan_mode == SagaPlanMode::Managed {
                build_managed_http_server(&application, &settings).await?
            } else {
                None
            };
            let definition_publish = plan.definition_publish.take();
            if settings.plan_mode == SagaPlanMode::Managed
                && settings.role == Some(SagaRole::Client)
            {
                plan.validate()?;
                #[cfg(feature = "web")]
                if http_server.is_some() {
                    return Err(saga_error(
                        ApplicationPhase::Ready,
                        "managed direct Saga client cannot expose inbound Saga HTTP routes",
                    ));
                }
                // client 不接收 command/result，也不持有全局状态机；可靠模式的本地 start-intent
                // 由后续 Outbox 组件接管。先压入停机保护再发布远程能力，后续失败可立即关闭新调用。
                context.activate(Box::new(SagaShutdown {
                    state: Arc::clone(&state),
                }));
                state.publish(plan)?;
                let contributor = self.contributor.as_ref().cloned().ok_or_else(|| {
                    saga_error(
                        ApplicationPhase::Ready,
                        "saga readiness contributor is missing",
                    )
                })?;
                contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
                let catalog_contributor =
                    self.catalog_contributor.as_ref().cloned().ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "Saga Catalog readiness contributor is missing",
                        )
                    })?;
                catalog_contributor.observe(
                    DependencyState::Ready,
                    reason::HEALTHY,
                    Instant::now(),
                );
                self.capability_contributor
                    .as_ref()
                    .ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "Saga capability readiness contributor is missing",
                        )
                    })?
                    .observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
                let definition_contributor = self
                    .definition_publish_contributor
                    .as_ref()
                    .cloned()
                    .ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "Saga definition publication readiness contributor is missing",
                        )
                    })?;
                if let Some(publish) = definition_publish {
                    self.critical_task = Some(Box::pin(run_definition_publish_loop(
                        application.clone(),
                        publish,
                        definition_contributor,
                    )));
                } else {
                    definition_contributor.observe(
                        DependencyState::Ready,
                        reason::HEALTHY,
                        Instant::now(),
                    );
                }
                #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
                self.stream_contributor
                    .as_ref()
                    .ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "saga stream readiness contributor is missing",
                        )
                    })?
                    .observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
                return Ok(());
            }
            let multi_datasource_participant = settings.plan_mode == SagaPlanMode::Managed
                && settings.role == Some(SagaRole::Participant)
                && !settings.participant.bindings.is_empty();
            if multi_datasource_participant {
                if plan.participants.len() != settings.participant.bindings.len() {
                    return Err(saga_error(
                        ApplicationPhase::Ready,
                        "managed participant bindings do not match the published runtimes",
                    ));
                }
                for (binding, binding_settings) in &settings.participant.bindings {
                    let datasource =
                        binding_settings.datasource_ref.as_deref().ok_or_else(|| {
                            saga_error(
                                ApplicationPhase::Ready,
                                "every participant binding requires datasource_ref",
                            )
                        })?;
                    let expected = natx_core::DatasourceRef::new(datasource).map_err(|error| {
                        saga_source_error(
                            ApplicationPhase::Ready,
                            "participant binding datasource_ref is invalid",
                            error,
                        )
                    })?;
                    if plan
                        .participants
                        .get(binding)
                        .is_none_or(|runtime| runtime.datasource_ref() != &expected)
                    {
                        return Err(saga_error(
                            ApplicationPhase::Ready,
                            "participant binding runtime does not match its datasource_ref",
                        ));
                    }
                }
            } else {
                let configured_datasource_name =
                    settings.primary_datasource_ref().ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "current Saga role does not expose a single runtime datasource",
                        )
                    })?;
                // 配置引用先命中当前 Application 的受管 pool，再复验 UserHook 提交的全部 runtime；
                // 任一不同源都在 descriptor 扫描或首条 SQL 前拒绝，不得自动切库。
                let runtime_driver = plan
                    .orchestrator
                    .as_ref()
                    .map(|entry| entry.runtime.driver())
                    .or_else(|| {
                        plan.participants
                            .values()
                            .next()
                            .map(ManagedParticipant::driver)
                    })
                    .ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "saga plan does not contain a runtime backend",
                        )
                    })?;
                match runtime_driver {
                    natx_core::DatabaseDriver::MySql => {
                        #[cfg(feature = "saga")]
                        {
                            let _pool = application.datasource(configured_datasource_name).await?;
                        }
                        #[cfg(not(feature = "saga"))]
                        return Err(saga_error(
                            ApplicationPhase::Ready,
                            "saga plan requires the MySQL runtime capability",
                        ));
                    }
                    natx_core::DatabaseDriver::PostgreSql => {
                        #[cfg(feature = "saga-pgsql")]
                        {
                            let _pool = application
                                .pg_datasource(configured_datasource_name)
                                .await?;
                        }
                        #[cfg(not(feature = "saga-pgsql"))]
                        return Err(saga_error(
                            ApplicationPhase::Ready,
                            "saga plan requires the PostgreSQL runtime capability",
                        ));
                    }
                }
                if settings.plan_mode == SagaPlanMode::Custom {
                    // custom 只替换运行计划，不取得跳过角色建表与结构复验的权限。
                    ensure_custom_role_schema(
                        configured_datasource_name,
                        runtime_driver,
                        settings.role.ok_or_else(|| {
                            saga_error(ApplicationPhase::Ready, "saga.role is required")
                        })?,
                    )
                    .await?;
                }
                let configured_datasource =
                    natx_core::DatasourceRef::new(configured_datasource_name).map_err(|error| {
                        saga_source_error(
                            ApplicationPhase::Ready,
                            "saga datasource_ref is invalid",
                            error,
                        )
                    })?;
                if plan
                    .orchestrator
                    .as_ref()
                    .is_some_and(|entry| entry.runtime.datasource_ref() != &configured_datasource)
                    || plan
                        .participants
                        .values()
                        .any(|runtime| runtime.datasource_ref() != &configured_datasource)
                {
                    return Err(saga_error(
                        ApplicationPhase::Ready,
                        "saga runtime datasource does not match saga.datasource_ref",
                    ));
                }
                if crate::outbox::datasource_ref(&application, ApplicationPhase::Ready)?
                    != configured_datasource_name
                {
                    return Err(saga_error(
                    ApplicationPhase::Ready,
                    "saga.datasource_ref must match outbox.datasource_ref for the atomic event chain",
                ));
                }
            }
            // Redis transport 属组件生命周期所有权,不随计划进入只读能力发布;必须在
            // publish 前取走。
            #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
            let redis_transport = plan.redis_transport.take();
            let catalog_watch = plan.catalog_watch.take();
            let capability_publish = plan.capability_publish.take();
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            let grpc_capability_publish = plan.grpc_capability_publish.take();

            if let Some(orchestrator) = plan.orchestrator.as_ref() {
                // definition/descriptor 与历史非终态实例必须在能力发布前同时通过；否则旧实例可能被
                // 新合同驱动，或同一步骤存在多个 handler，均不得进入可接流状态。
                orchestrator
                    .runtime
                    .verify_startup()
                    .await
                    .map_err(|error| {
                        saga_source_error(
                            ApplicationPhase::Ready,
                            "saga orchestrator startup verification failed",
                            error,
                        )
                    })?;
            } else {
                // 纯参与方没有全局 definition 注册表，仍用空注册表校验本 binary 内 descriptor 的
                // 唯一性与字段合法性；远端 definition 投影由 ParticipantRuntime 信任合同约束。
                nasaga_runtime::verify_descriptors(&DefinitionRegistry::new()).map_err(
                    |error| {
                        saga_source_error(
                            ApplicationPhase::Ready,
                            "saga participant descriptor verification failed",
                            error,
                        )
                    },
                )?;
            }

            // 停机 action 必须先入栈，再发布能力；这样后续任一组件 Ready 失败时反向清理一定能先
            // 关闭 Saga 新请求入口，不会留下数据库尚可用但 Saga 权限游离的半完成状态。
            context.activate(Box::new(SagaShutdown {
                state: Arc::clone(&state),
            }));
            if catalog_watch.is_some() {
                state.revoke_catalog_authority();
                state.result_authority.revoke();
            }
            let orchestrator = state.publish(plan)?;
            #[cfg(feature = "web")]
            if let Some(http_server) = http_server {
                state.publish_http_server(http_server)?;
            }
            let contributor = self.contributor.as_ref().cloned().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "saga readiness contributor is missing",
                )
            })?;
            contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());

            let catalog_contributor =
                self.catalog_contributor.as_ref().cloned().ok_or_else(|| {
                    saga_error(
                        ApplicationPhase::Ready,
                        "Saga Catalog readiness contributor is missing",
                    )
                })?;
            let timer_task: Option<ApplicationFuture<'static>> =
                if let Some(orchestrator) = orchestrator {
                    Some(Box::pin(run_timer_loop(
                        application.clone(),
                        orchestrator.runtime,
                        orchestrator.timer_owner,
                        settings,
                        contributor,
                    )))
                } else {
                    None
                };
            let catalog_task: Option<ApplicationFuture<'static>> = match catalog_watch {
                Some(watch) => Some(Box::pin(run_catalog_watch_loop(
                    application.clone(),
                    watch,
                    catalog_contributor,
                )) as ApplicationFuture<'static>),
                None => {
                    catalog_contributor.observe(
                        DependencyState::Ready,
                        reason::HEALTHY,
                        Instant::now(),
                    );
                    None
                }
            };
            let capability_task: Option<ApplicationFuture<'static>> = {
                let capability_contributor = self
                    .capability_contributor
                    .as_ref()
                    .cloned()
                    .ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "Saga capability readiness contributor is missing",
                        )
                    })?;
                let mut task: Option<ApplicationFuture<'static>> = None;
                if let Some(publish) = capability_publish {
                    task = Some(Box::pin(run_capability_publish_loop(
                        application.clone(),
                        publish,
                        capability_contributor.clone(),
                    )) as ApplicationFuture<'static>);
                }
                #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
                if let Some(publish) = grpc_capability_publish {
                    if task.is_some() {
                        return Err(saga_error(
                            ApplicationPhase::Ready,
                            "Saga capability publication has more than one protocol authority",
                        ));
                    }
                    task = Some(Box::pin(run_grpc_capability_publish_loop(
                        application.clone(),
                        publish,
                        capability_contributor.clone(),
                    )) as ApplicationFuture<'static>);
                }
                if task.is_none() {
                    capability_contributor.observe(
                        DependencyState::Ready,
                        reason::HEALTHY,
                        Instant::now(),
                    );
                }
                task
            };

            let definition_publish_task: Option<ApplicationFuture<'static>> = {
                let definition_contributor = self
                    .definition_publish_contributor
                    .as_ref()
                    .cloned()
                    .ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "Saga definition publication readiness contributor is missing",
                        )
                    })?;
                match definition_publish {
                    Some(publish) => Some(Box::pin(run_definition_publish_loop(
                        application.clone(),
                        publish,
                        definition_contributor,
                    )) as ApplicationFuture<'static>),
                    None => {
                        definition_contributor.observe(
                            DependencyState::Ready,
                            reason::HEALTHY,
                            Instant::now(),
                        );
                        None
                    }
                }
            };

            #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
            let stream_task: Option<ApplicationFuture<'static>> = {
                let stream_contributor =
                    self.stream_contributor.as_ref().cloned().ok_or_else(|| {
                        saga_error(
                            ApplicationPhase::Ready,
                            "saga stream readiness contributor is missing",
                        )
                    })?;
                match redis_transport {
                    Some(transport) => {
                        // Ready 前用真实客户端统一探测:PING、配置合同、group 幂等创建
                        // (兼 ACL 探测)。任何失败都拒绝 Ready——不能带着无法领取消息的
                        // 消费拓扑对外宣布可用。
                        let client = application.redis(&transport.client_name).await?;
                        let configs: Vec<&nasaga_runtime::SagaStreamConsumerConfig> = transport
                            .pollers
                            .iter()
                            .map(|poller| poller.config())
                            .collect();
                        nasaga_runtime::verify_stream_transport_ready(&client, &configs)
                            .await
                            .map_err(|error| {
                                saga_source_error(
                                    ApplicationPhase::Ready,
                                    "saga redis stream transport readiness probe failed",
                                    error,
                                )
                            })?;
                        let runtimes: Vec<Arc<StreamRuntime>> = transport
                            .pollers
                            .iter()
                            .map(|poller| {
                                let config = poller.config();
                                Arc::new(StreamRuntime::new(
                                    &config.stream,
                                    &config.group,
                                    &config.consumer,
                                ))
                            })
                            .collect();
                        state.publish_streams(runtimes.clone())?;
                        // poller 集合随计划在此冻结,最坏序列数可精确承诺:
                        // 每个 stream runtime 十二个带标识 family 各一条,外加一个全局 family。
                        let worst_case_series = runtimes
                            .len()
                            .saturating_mul(SAGA_STREAM_SERIES_PER_RUNTIME)
                            .saturating_add(1);
                        application
                            .metrics_hub()
                            .register_legacy_source_reserved(
                                Arc::new(SagaStreamMetricsSource {
                                    state: Arc::clone(&state),
                                }),
                                worst_case_series,
                            )
                            .map_err(|error| match error {
                                nametrics_core::MetricSourceRegistrationError::Conflict(
                                    conflict,
                                ) => saga_error(
                                    ApplicationPhase::Ready,
                                    format!(
                                        "saga stream metric descriptor `{}` conflicts with an existing registration",
                                        conflict.name
                                    ),
                                ),
                                nametrics_core::MetricSourceRegistrationError::SeriesBudgetExceeded => {
                                    saga_error(
                                        ApplicationPhase::Ready,
                                        "saga stream metric series reservation exceeds the process limit",
                                    )
                                }
                            })?;
                        stream_contributor.observe(
                            DependencyState::Ready,
                            reason::HEALTHY,
                            Instant::now(),
                        );
                        Some(Box::pin(run_stream_poll_loop(
                            application.clone(),
                            client,
                            transport.pollers,
                            runtimes,
                            transport.poll_idle_ms,
                            transport.error_backoff_ms,
                            stream_contributor,
                        )) as ApplicationFuture<'static>)
                    }
                    None => {
                        // 计划不含 Redis transport:Start 注册的占位贡献项一次性置绿,
                        // 不影响未启用者的 readiness。
                        stream_contributor.observe(
                            DependencyState::Ready,
                            reason::HEALTHY,
                            Instant::now(),
                        );
                        None
                    }
                }
            };
            #[cfg(not(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql")))]
            let stream_task: Option<ApplicationFuture<'static>> = None;

            let tasks = [
                timer_task,
                stream_task,
                catalog_task,
                capability_task,
                definition_publish_task,
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
            self.critical_task = (!tasks.is_empty())
                .then(|| Box::pin(run_saga_supervised_loops(tasks)) as ApplicationFuture<'static>);
            Ok(())
        })
    }

    /// 业务作用：把唯一 durable timer 轮询任务移交 Runner 监督。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：托管 Orchestrator 时首次调用返回任务；纯参与方或重复调用返回 `None`。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        // 标签固定:任务内容(timer/stream 消费)由 Ready 阶段组装,Runner 只看单一
        // 受监督入口;任一内部循环异常退出都会以本任务失败触发统一停机。
        self.critical_task
            .take()
            .map(|task| ("saga-runtime-loops", task))
    }
}

/// 业务作用：在依赖资源释放前关闭新的 Saga 能力访问，维持逆序停机边界。
struct SagaShutdown {
    state: Arc<SagaRuntimeState>,
}

impl ShutdownAction for SagaShutdown {
    /// 业务作用：返回不含业务身份和配置值的稳定清理动作名。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：固定 Saga 能力关闭标签。
    fn label(&self) -> &'static str {
        "saga-runtime"
    }

    /// 业务作用：先关闭新的 Saga 能力访问，再允许反向清理继续释放数据库和 transport。
    ///
    /// 参数说明：
    /// - `_context`：Runner 提供的共享停机预算；本动作只做原子发布，不消耗外部等待。
    ///
    /// 返回：保护态发布完成后立即成功。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            self.state.stop();
            Ok(())
        })
    }
}

#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
impl StreamRuntime {
    /// 业务作用：创建单条受管 stream 的零值观测状态。
    ///
    /// 参数说明：
    /// - `stream`: 源 stream 名(冻结标签值)。
    /// - `group`: consumer group 名(冻结标签值)。
    ///
    /// 返回：全零计数、初始健康的状态。
    fn new(stream: &str, group: &str, consumer: &str) -> Self {
        Self {
            stream: stream.to_string(),
            group: group.to_string(),
            consumer: consumer.to_string(),
            acked: std::sync::atomic::AtomicU64::new(0),
            dead_lettered: std::sync::atomic::AtomicU64::new(0),
            retained: std::sync::atomic::AtomicU64::new(0),
            reclaimed: std::sync::atomic::AtomicU64::new(0),
            deleted_pending: std::sync::atomic::AtomicU64::new(0),
            auth_rejected: std::sync::atomic::AtomicU64::new(0),
            failed_rounds: std::sync::atomic::AtomicU64::new(0),
            handled: std::sync::atomic::AtomicU64::new(0),
            handler_micros_sum: std::sync::atomic::AtomicU64::new(0),
            pending: std::sync::atomic::AtomicU64::new(0),
            oldest_pel_age_ms: std::sync::atomic::AtomicU64::new(0),
            healthy: AtomicBool::new(true),
        }
    }

    /// 业务作用：吸收一轮消费报告到累计计数。
    ///
    /// 参数说明：
    /// - `report`: 单轮 poll 报告。
    ///
    /// 返回：无返回值。
    fn absorb(&self, report: &nasaga_runtime::StreamPollReport) {
        self.acked.fetch_add(report.acked, Ordering::Relaxed);
        self.dead_lettered
            .fetch_add(report.dead_lettered, Ordering::Relaxed);
        self.retained.fetch_add(report.retained, Ordering::Relaxed);
        self.reclaimed
            .fetch_add(report.reclaimed, Ordering::Relaxed);
        self.deleted_pending
            .fetch_add(report.deleted_pending, Ordering::Relaxed);
        self.auth_rejected
            .fetch_add(report.auth_rejected, Ordering::Relaxed);
        self.handled.fetch_add(report.handled, Ordering::Relaxed);
        self.handler_micros_sum
            .fetch_add(report.handler_micros_sum, Ordering::Relaxed);
    }

    /// 业务作用：刷新本流的积压 gauge——pending 数与最老 PEL 年龄。
    ///
    /// 参数说明：
    /// - `pending`: 当前 PEL 数。
    /// - `oldest_age_ms`: 最老 pending entry 年龄;PEL 为空时归零。
    ///
    /// 返回：无返回值。
    fn set_backlog(&self, pending: u64, oldest_age_ms: Option<u64>) {
        self.pending.store(pending, Ordering::Relaxed);
        self.oldest_pel_age_ms
            .store(oldest_age_ms.unwrap_or(0), Ordering::Relaxed);
    }
}

#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
impl SagaRuntimeState {
    /// 业务作用：Ready 时一次性发布冻结的受管 stream 观测集合。
    ///
    /// 参数说明：
    /// - `runtimes`: 与消费者一一对应的观测状态。
    ///
    /// 返回：首次发布成功;重复发布返回 Ready 错误。
    fn publish_streams(&self, runtimes: Vec<Arc<StreamRuntime>>) -> ApplicationResult<()> {
        self.streams.set(Arc::new(runtimes)).map_err(|_| {
            saga_error(
                ApplicationPhase::Ready,
                "saga stream runtimes were already published",
            )
        })
    }

    /// 业务作用：读取冻结的受管 stream 观测集合,供低基数指标渲染。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Ready 前或未启用 transport 时为空集合。
    pub(crate) fn stream_runtimes(&self) -> Arc<Vec<Arc<StreamRuntime>>> {
        self.streams
            .get()
            .cloned()
            .unwrap_or_else(|| Arc::new(Vec::new()))
    }
}

/// 将受管 Redis Streams 进程计数接入唯一指标目录的兼容源。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
struct SagaStreamMetricsSource {
    state: Arc<SagaRuntimeState>,
}

#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
impl nametrics_core::LegacyMetricsSource for SagaStreamMetricsSource {
    /// 业务作用：返回 Saga Streams 固定 family 与冻结 transport label 目录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：启动期登记并用于结构化样本校验的全部 Streams descriptor。
    fn descriptors(&self) -> &'static [&'static nametrics_core::MetricDescriptor] {
        &SAGA_STREAM_DESCRIPTORS
    }

    /// 业务作用：读取每个冻结 consumer 的当前原子快照，不清零计数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：始终为 `Some`，其中是按 stream、group、consumer 标识的计数器与 gauge 当前值。
    fn snapshot(&self) -> Option<Vec<nametrics_core::MetricSample>> {
        let runtimes = self.state.stream_runtimes();
        let mut samples =
            Vec::with_capacity(runtimes.len() * 12 + usize::from(!runtimes.is_empty()));
        for runtime in runtimes.iter() {
            let labels = vec![
                ("stream", runtime.stream.clone()),
                ("group", runtime.group.clone()),
                ("consumer", runtime.consumer.clone()),
            ];
            for (name, value) in [
                (STREAM_ACKED.name, runtime.acked.load(Ordering::Relaxed)),
                (
                    STREAM_DEAD_LETTERED.name,
                    runtime.dead_lettered.load(Ordering::Relaxed),
                ),
                (
                    STREAM_RETAINED.name,
                    runtime.retained.load(Ordering::Relaxed),
                ),
                (
                    STREAM_RECLAIMED.name,
                    runtime.reclaimed.load(Ordering::Relaxed),
                ),
                (
                    STREAM_DELETED_PENDING.name,
                    runtime.deleted_pending.load(Ordering::Relaxed),
                ),
                (
                    STREAM_AUTH_REJECTED.name,
                    runtime.auth_rejected.load(Ordering::Relaxed),
                ),
                (
                    STREAM_FAILED_ROUNDS.name,
                    runtime.failed_rounds.load(Ordering::Relaxed),
                ),
                (STREAM_HANDLED.name, runtime.handled.load(Ordering::Relaxed)),
                (
                    STREAM_HANDLER_MICROS.name,
                    runtime.handler_micros_sum.load(Ordering::Relaxed),
                ),
            ] {
                samples.push(stream_metric_sample(
                    name,
                    labels.clone(),
                    nametrics_core::MetricValue::Counter(value),
                ));
            }
            for (name, value) in [
                (STREAM_PENDING.name, runtime.pending.load(Ordering::Relaxed)),
                (
                    STREAM_OLDEST_PEL_AGE.name,
                    runtime.oldest_pel_age_ms.load(Ordering::Relaxed),
                ),
                (
                    STREAM_HEALTHY.name,
                    u64::from(runtime.healthy.load(Ordering::Relaxed)),
                ),
            ] {
                samples.push(stream_metric_sample(
                    name,
                    labels.clone(),
                    nametrics_core::MetricValue::Gauge(value as f64),
                ));
            }
        }
        if !runtimes.is_empty() {
            samples.push(stream_metric_sample(
                STREAM_PUBLISHER_DUPLICATES.name,
                Vec::new(),
                nametrics_core::MetricValue::Counter(
                    nasaga_runtime::publisher_duplicate_hints_total(),
                ),
            ));
        }
        Some(samples)
    }

    /// 业务作用：在尚未发布 stream 运行时保留空文本语义；非空快照由 hub 统一渲染。
    ///
    /// 参数说明：
    /// - `output`: 接收旧空运行时兼容文本的缓冲区。
    ///
    /// 返回：无；存在结构化样本时统一 hub 不调用本入口。
    fn render_prometheus(&self, output: &mut String) {
        output.push_str(&render_stream_metrics(&self.state));
    }
}

/// 业务作用：构造一个带冻结 transport label 的 Saga Stream 指标样本。
///
/// 参数说明：
/// - `name`: 已登记 family 名。
/// - `labels`: 固定 stream、group 与 consumer 名值对，或进程级空 label。
/// - `value`: counter 或 gauge 当前值。
///
/// 返回：可经唯一 descriptor 校验的结构化样本。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
fn stream_metric_sample(
    name: &'static str,
    labels: Vec<(&'static str, String)>,
    value: nametrics_core::MetricValue,
) -> nametrics_core::MetricSample {
    nametrics_core::MetricSample {
        name,
        labels,
        value,
    }
}

/// 业务作用：渲染受管 stream 消费的低基数 Prometheus 文本——标签值来自 Ready 冻结的
/// (stream, group) 集合;`deleted_pending` 非零表示 entry 在确认前被外部删除,必须告警。
///
/// 参数说明：
/// - `state`: Saga 运行时状态。
///
/// 返回：按 stream 分组的指标文本;未启用 transport 时为空串。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
pub(crate) fn render_stream_metrics(state: &SagaRuntimeState) -> String {
    /// 业务作用：转义 Prometheus label 值，阻止冻结配置中的特殊字符破坏 exposition 边界。
    ///
    /// 参数说明：`value` 是已经过配置门禁的 stream、group 或 consumer 标签值。
    ///
    /// 返回：反斜线、引号与换行均已转义的标签文本。
    fn escape_label(value: &str) -> String {
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
    }
    let mut output = String::new();
    for runtime in state.stream_runtimes().iter() {
        // 标签含 consumer 维度:同一 (stream, group) 允许多个消费身份,缺它会导出
        // 多条完全相同 label set 的 series。三个标签值都来自 Ready 冻结集合,基数有界。
        let labels = format!(
            "{{stream=\"{}\",group=\"{}\",consumer=\"{}\"}}",
            escape_label(&runtime.stream),
            escape_label(&runtime.group),
            escape_label(&runtime.consumer)
        );
        output.push_str(&format!(
            "napp_saga_stream_acked_total{labels} {}\n\
             napp_saga_stream_dead_lettered_total{labels} {}\n\
             napp_saga_stream_retained_total{labels} {}\n\
             napp_saga_stream_reclaimed_total{labels} {}\n\
             napp_saga_stream_deleted_pending_total{labels} {}\n\
             napp_saga_stream_auth_rejected_total{labels} {}\n\
             napp_saga_stream_failed_rounds_total{labels} {}\n\
             napp_saga_stream_handled_total{labels} {}\n\
             napp_saga_stream_handler_micros_sum{labels} {}\n\
             napp_saga_stream_pending{labels} {}\n\
             napp_saga_stream_oldest_pel_age_ms{labels} {}\n\
             napp_saga_stream_healthy{labels} {}\n",
            runtime.acked.load(Ordering::Relaxed),
            runtime.dead_lettered.load(Ordering::Relaxed),
            runtime.retained.load(Ordering::Relaxed),
            runtime.reclaimed.load(Ordering::Relaxed),
            runtime.deleted_pending.load(Ordering::Relaxed),
            runtime.auth_rejected.load(Ordering::Relaxed),
            runtime.failed_rounds.load(Ordering::Relaxed),
            runtime.handled.load(Ordering::Relaxed),
            runtime.handler_micros_sum.load(Ordering::Relaxed),
            runtime.pending.load(Ordering::Relaxed),
            runtime.oldest_pel_age_ms.load(Ordering::Relaxed),
            u8::from(runtime.healthy.load(Ordering::Relaxed)),
        ));
    }
    if !output.is_empty() {
        // 发布端重复提示是进程级计数(publisher 不绑定单一 stream 标签),随流指标一并导出。
        output.push_str(&format!(
            "napp_saga_stream_publisher_duplicate_hints_total {}\n",
            nasaga_runtime::publisher_duplicate_hints_total()
        ));
    }
    output
}

/// 业务作用：持续重试 workflow owner 的不可变定义，直到全部取得匹配收据与所需生命周期。
///
/// 参数说明：`application` 提供停机状态，`plan` 冻结签名产物和协议，`contributor` 控制 owner 接流。
///
/// 返回：全部产物一旦获得持久收据便保持 Ready 至停机；暂时失败会摘流并保留原始 seal 重报。
async fn run_definition_publish_loop(
    application: Application,
    plan: ManagedDefinitionPublishPlan,
    contributor: ReadinessContributor,
) -> ApplicationResult<()> {
    let mut published = false;
    loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                return Ok(());
            }
            ApplicationState::Starting => {
                sleep_observing_state(&application, 50).await;
                continue;
            }
            ApplicationState::Ready => {}
        }
        if published {
            sleep_observing_state(&application, 1_000).await;
            continue;
        }
        let mut accepted = true;
        for artifact in &plan.artifacts {
            let result = match &plan.transport {
                ManagedDefinitionPublishTransport::Http { client, target } => {
                    post_managed_http_definition(client, target, &plan.producer, artifact).await
                }
                #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
                ManagedDefinitionPublishTransport::Grpc { target } => {
                    post_managed_grpc_definition(target, artifact).await
                }
            };
            match result {
                Ok(active) if active || !plan.require_active => {}
                Ok(_) => {
                    accepted = false;
                    break;
                }
                Err(error) => {
                    tracing::warn!(error = %error, "Saga definition 发布尚未取得受信收据");
                    accepted = false;
                    break;
                }
            }
        }
        if accepted {
            published = true;
            contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
        } else {
            contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
            sleep_observing_state(&application, 1_000).await;
        }
    }
}

/// 业务作用：通过 HMAC 认证的 HTTP 控制面提交一份 Ed25519 签名 definition，并复验持久记录。
///
/// 参数说明：client/target 固定地址与凭据，`producer` 是 workflow owner，`artifact` 是签名原文。
///
/// 返回：收据 key 与 seal 匹配时返回是否 Active；网络、权限、冲突或畸形收据返回可重报错误。
async fn post_managed_http_definition(
    client: &reqwest::Client,
    target: &ManagedHttpTarget,
    producer: &nasaga_runtime::ServiceIdentity,
    artifact: &ManagedSignedDefinitionArtifact,
) -> ApplicationResult<bool> {
    let (target, remaining) = target.resolve().await?;
    let body = serde_json::to_vec(artifact).map_err(|error| {
        saga_source_error(
            ApplicationPhase::Running,
            "definition publication envelope serialization failed",
            error,
        )
    })?;
    let timestamp_ms = u64::try_from(epoch_millis()?).map_err(|error| {
        saga_source_error(
            ApplicationPhase::Running,
            "definition publication clock is unavailable",
            error,
        )
    })?;
    let nonce = nasaga_runtime::SagaHttpMessageAuthenticator::issue_nonce();
    let signature =
        target
            .authenticator
            .sign(producer, &target.signed_path, timestamp_ms, &nonce, &body);
    let response = client
        .post(target.url.clone())
        .timeout(remaining)
        .header("content-type", "application/json")
        .header("x-saga-producer", producer.as_str())
        .header("x-saga-timestamp", timestamp_ms.to_string())
        .header("x-saga-nonce", nonce)
        .header("x-saga-signature", signature)
        .body(body)
        .send()
        .await
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Running,
                "definition registry HTTP delivery is uncertain",
                error,
            )
        })?;
    if !response.status().is_success() {
        return Err(saga_error(
            ApplicationPhase::Running,
            format!(
                "definition registry rejected the artifact with HTTP status {}",
                response.status()
            ),
        ));
    }
    let record: nasaga_runtime::DefinitionRecord = response.json().await.map_err(|error| {
        saga_source_error(
            ApplicationPhase::Running,
            "definition registry HTTP receipt is invalid",
            error,
        )
    })?;
    let expected: nasaga_runtime::DefinitionArtifact =
        serde_json::from_str(&artifact.canonical_document).map_err(|error| {
            saga_source_error(
                ApplicationPhase::Running,
                "signed definition document is invalid",
                error,
            )
        })?;
    if record.artifact.tenant != expected.tenant
        || record.artifact.workflow != expected.workflow
        || record.artifact.definition_version != expected.definition_version
        || record.artifact.seal != expected.seal
    {
        return Err(saga_error(
            ApplicationPhase::Running,
            "definition registry HTTP receipt does not match the signed artifact",
        ));
    }
    Ok(record.lifecycle == nasaga_runtime::DefinitionLifecycle::Active)
}

/// 业务作用：通过 generated gRPC Registry client 提交签名 definition，并复验 disposition、key 与 seal。
///
/// 参数说明：`target` 固定 mTLS channel，`artifact` 保留 canonical document 与 detached signature。
///
/// 返回：Committed/Duplicate 且记录匹配时返回是否 Active；其它 status 或收据均拒绝前移。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
async fn post_managed_grpc_definition(
    target: &ManagedGrpcTarget,
    artifact: &ManagedSignedDefinitionArtifact,
) -> ApplicationResult<bool> {
    use nasaga_runtime::orchestrator_proto as proto;
    let expected: nasaga_runtime::DefinitionArtifact =
        serde_json::from_str(&artifact.canonical_document).map_err(|error| {
            saga_source_error(
                ApplicationPhase::Running,
                "signed gRPC definition document is invalid",
                error,
            )
        })?;
    let mut client = proto::saga_definition_registry_client::SagaDefinitionRegistryClient::new(
        target.channel.clone(),
    );
    let response = client
        .publish_definition(proto::PublishDefinitionRequest {
            artifact: Some(proto::DefinitionArtifact {
                key: Some(proto::DefinitionKey {
                    tenant_id: expected.tenant.clone(),
                    workflow: expected.workflow.clone(),
                    definition_version: expected.definition_version,
                }),
                format: artifact.format.clone(),
                canonical_document: artifact.canonical_document.as_bytes().to_vec(),
                sha256: artifact.sha256.clone(),
                signing_key_id: artifact.signing_key_id.clone(),
                signature: artifact.signature.as_bytes().to_vec(),
            }),
        })
        .await
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Running,
                "definition registry gRPC delivery is uncertain",
                error,
            )
        })?
        .into_inner();
    if !matches!(
        proto::DefinitionPublishDisposition::try_from(response.disposition),
        Ok(proto::DefinitionPublishDisposition::Committed
            | proto::DefinitionPublishDisposition::Duplicate)
    ) {
        return Err(saga_error(
            ApplicationPhase::Running,
            "definition registry gRPC disposition is not committable",
        ));
    }
    let record = response.definition.ok_or_else(|| {
        saga_error(
            ApplicationPhase::Running,
            "definition registry gRPC receipt is missing",
        )
    })?;
    let key = record.key.ok_or_else(|| {
        saga_error(
            ApplicationPhase::Running,
            "definition registry gRPC receipt key is missing",
        )
    })?;
    if key.tenant_id != expected.tenant
        || key.workflow != expected.workflow
        || key.definition_version != expected.definition_version
        || record.sha256 != expected.seal
    {
        return Err(saga_error(
            ApplicationPhase::Running,
            "definition registry gRPC receipt does not match the signed artifact",
        ));
    }
    Ok(record.lifecycle == proto::DefinitionLifecycle::Active as i32)
}

/// 业务作用：在 listener 地址发布后为 HTTP/gRPC 控制面统一封闭本地能力的完整数据面路由。
///
/// 参数说明：`application` 提供生命周期与监听地址，`descriptors` 是待发布能力，
/// `advertised_endpoint` 可指定 HTTP 数据面的对外 origin。
///
/// 返回：完整路由校验通过返回 true；应用先行停机返回 false，非法合同返回错误。
async fn prepare_managed_capability_routes(
    application: &Application,
    descriptors: &mut [nasaga_runtime::CapabilityDescriptor],
    advertised_endpoint: Option<&str>,
) -> ApplicationResult<bool> {
    // 后台任务可以在启动阶段被调度；listener 完成绑定并发布地址之前不能构造可路由的能力。
    loop {
        match application.state() {
            ApplicationState::Starting => sleep_observing_state(application, 50).await,
            ApplicationState::Ready => break,
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                return Ok(false);
            }
        }
    }
    let endpoint = if descriptors
        .iter()
        .any(|descriptor| descriptor.transport == "http")
    {
        Some(resolve_managed_capability_endpoint(
            application,
            advertised_endpoint,
        )?)
    } else {
        None
    };
    for descriptor in descriptors {
        if descriptor.transport == "http" {
            descriptor.endpoint.clone_from(
                endpoint
                    .as_ref()
                    .expect("HTTP capability endpoint was resolved before publication"),
            );
        }
        // listener 地址已确定，此时封闭完整路由合同，非法地址不能进入外部 Catalog。
        nasaga_runtime::validate_capability_route_contract(descriptor).map_err(|error| {
            saga_source_error(
                ApplicationPhase::Running,
                "managed Saga capability route contract is invalid",
                error,
            )
        })?;
    }
    Ok(true)
}

/// 业务作用：在 Web listener 取得真实端点后持续登记本副本步骤能力，租约中断时立即摘除 Saga Ready。
///
/// 参数说明：`application` 提供生命周期与真实监听地址，`plan` 是冻结发布合同，`contributor` 控制参与方摘流。
///
/// 返回：应用停机时正常退出；可恢复的网络或控制面失败在循环内摘流并退避。
async fn run_capability_publish_loop(
    application: Application,
    mut plan: ManagedCapabilityPublishPlan,
    contributor: ReadinessContributor,
) -> ApplicationResult<()> {
    if !prepare_managed_capability_routes(
        &application,
        &mut plan.descriptors,
        plan.advertised_endpoint.as_deref(),
    )
    .await?
    {
        contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
        return Ok(());
    }
    loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                return Ok(());
            }
            ApplicationState::Starting => {
                sleep_observing_state(&application, 50).await;
                continue;
            }
            ApplicationState::Ready => {}
        }
        // 新配置改变结果凭据后，续租必须报告当前真实材料，旧合同不能无限续期。
        plan.result_source.refresh(&mut plan.descriptors)?;
        let mut accepted = true;
        let mut earliest_deadline = None;
        for index in 0..plan.descriptors.len() {
            let mut descriptor = plan.descriptors[index].clone();
            let started = Instant::now();
            match post_managed_capability(&plan, &descriptor).await {
                Ok(receipt) => {
                    // 收据中的代际是 Catalog 行锁内分配的路由权威；先吸收它再复验摘要，
                    // 后续续租和不确定重报才能携带数据库已经提交的完整 descriptor。
                    descriptor.route_generation = receipt.route_generation;
                    if receipt.capability_digest != descriptor.digest() {
                        accepted = false;
                        break;
                    }
                    // 请求等待、响应延迟与串行批次都消耗有效期；过期收据不能开放路由。
                    match managed_capability_receipt_deadline(&receipt, &descriptor, started) {
                        Ok(deadline) => {
                            earliest_deadline = Some(
                                earliest_deadline
                                    .map_or(deadline, |previous: Instant| previous.min(deadline)),
                            );
                        }
                        Err(error) => {
                            tracing::warn!(error = %error, "Saga capability 租约证据不可用");
                            accepted = false;
                            break;
                        }
                    }
                    plan.descriptors[index] = descriptor;
                }
                Err(error) => {
                    tracing::warn!(error = %error, "Saga capability 登记未取得受信收据");
                    accepted = false;
                    break;
                }
            }
        }
        // 整批完成后复验最早期限，后续请求成功不能延长先前 descriptor 的租约。
        let deadline = earliest_deadline.filter(|deadline| *deadline > Instant::now());
        if let Some(deadline) = deadline.filter(|_| accepted) {
            contributor.observe_ready_until(deadline);
            let delay = deadline
                .saturating_duration_since(Instant::now())
                .as_millis()
                / 3;
            sleep_observing_state(&application, (delay as u64).min(plan.lease_ms / 3).max(1)).await;
        } else {
            contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
            sleep_observing_state(&application, 1_000).await;
        }
    }
}

/// 业务作用：持续通过 generated gRPC Registry client 登记本副本能力，租约中断时立即撤销 Saga Ready。
///
/// 参数说明：`application` 提供生命周期，`plan` 冻结 mTLS channel、租户和 descriptor，`contributor` 控制摘流。
///
/// 返回：应用停机时正常退出；暂时网络或收据异常在循环内摘流并按固定上限退避。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
async fn run_grpc_capability_publish_loop(
    application: Application,
    mut plan: ManagedGrpcCapabilityPublishPlan,
    contributor: ReadinessContributor,
) -> ApplicationResult<()> {
    if !prepare_managed_capability_routes(
        &application,
        &mut plan.descriptors,
        plan.advertised_endpoint.as_deref(),
    )
    .await?
    {
        contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
        return Ok(());
    }
    loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                return Ok(());
            }
            ApplicationState::Starting => {
                sleep_observing_state(&application, 50).await;
                continue;
            }
            ApplicationState::Ready => {}
        }
        // 新配置改变结果凭据后，续租必须报告当前真实材料，旧合同不能无限续期。
        plan.result_source.refresh(&mut plan.descriptors)?;
        let mut accepted = true;
        let mut earliest_deadline = None;
        'publishing: for index in 0..plan.descriptors.len() {
            let mut descriptor = plan.descriptors[index].clone();
            let started = Instant::now();
            match post_managed_grpc_capability(&plan, &descriptor).await {
                Ok(receipt) => {
                    // gRPC 与 HTTP 共享数据库分配的代际，publisher 不再把墙上时钟当作单调序列。
                    descriptor.route_generation = receipt.route_generation;
                    if receipt.capability_digest != descriptor.digest() {
                        accepted = false;
                        break 'publishing;
                    }
                    // gRPC 与 HTTP 使用同一租约门禁，成功 status 不代表批次完成时仍持有权威。
                    match managed_capability_receipt_deadline(&receipt, &descriptor, started) {
                        Ok(deadline) => {
                            earliest_deadline = Some(
                                earliest_deadline
                                    .map_or(deadline, |previous: Instant| previous.min(deadline)),
                            );
                        }
                        Err(error) => {
                            tracing::warn!(error = %error, "Saga gRPC capability 租约证据不可用");
                            accepted = false;
                            break 'publishing;
                        }
                    }
                    plan.descriptors[index] = descriptor;
                }
                Err(error) => {
                    tracing::warn!(error = %error, "Saga gRPC capability 登记未取得受信收据");
                    accepted = false;
                    break 'publishing;
                }
            }
        }
        // 发布完整批次之后才按最早到期的证据恢复就绪。
        let deadline = earliest_deadline.filter(|deadline| *deadline > Instant::now());
        if let Some(deadline) = deadline.filter(|_| accepted) {
            contributor.observe_ready_until(deadline);
            let delay = deadline
                .saturating_duration_since(Instant::now())
                .as_millis()
                / 3;
            sleep_observing_state(
                &application,
                (delay as u64)
                    .min(u64::from(plan.lease_seconds) * 1_000 / 3)
                    .max(1),
            )
            .await;
        } else {
            contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
            sleep_observing_state(&application, 1_000).await;
        }
    }
}

/// 业务作用：把服务端租约收据转为本地单调截止时间，约束响应延迟与时钟回拨后的就绪时长。
///
/// 参数说明：`receipt` 是已提交的收据，`descriptor` 提供租约预算，`started` 是请求开始时刻。
///
/// 返回：绝对期限与请求预算中更早的截止时间；过期、零代际或时钟异常均拒绝收据。
fn managed_capability_receipt_deadline(
    receipt: &nasaga_runtime::CapabilityReceipt,
    descriptor: &nasaga_runtime::CapabilityDescriptor,
    started: Instant,
) -> ApplicationResult<Instant> {
    // 未取得有效租期或路由代际时不能将成功响应解释为控制权威。
    if receipt.route_generation == 0 {
        return Err(saga_error(
            ApplicationPhase::Running,
            "capability receipt lease has expired or has no route generation",
        ));
    }
    capability_lease::receipt_deadline(
        receipt.accepted_until_ms,
        descriptor.requested_lease_ms,
        started,
        epoch_millis()?,
    )
    .ok_or_else(|| {
        saga_error(
            ApplicationPhase::Running,
            "capability receipt lease has expired or exceeded the local lease budget",
        )
    })
}

/// 业务作用：向受信 gRPC Definition Registry 发送一个 canonical capability 并校验持久租约收据。
///
/// 参数说明：`plan` 固定 mTLS channel 与租约，`descriptor` 包含授权租户和本副本完整能力。
///
/// 返回：服务端明确提交租约时返回协议无关收据；网络、status 或字段异常均保留下一轮重报。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
async fn post_managed_grpc_capability(
    plan: &ManagedGrpcCapabilityPublishPlan,
    descriptor: &nasaga_runtime::CapabilityDescriptor,
) -> ApplicationResult<nasaga_runtime::CapabilityReceipt> {
    use nasaga_runtime::orchestrator_proto as proto;
    let canonical_document = serde_json::to_vec(descriptor).map_err(|error| {
        saga_source_error(
            ApplicationPhase::Running,
            "gRPC capability descriptor serialization failed",
            error,
        )
    })?;
    let mut client = proto::saga_definition_registry_client::SagaDefinitionRegistryClient::new(
        plan.target.channel.clone(),
    );
    let response = client
        .register_capability(proto::RegisterCapabilityRequest {
            capability: Some(proto::CapabilityDescriptor {
                definition: Some(proto::DefinitionKey {
                    tenant_id: descriptor.tenant.clone(),
                    workflow: descriptor.workflow.clone(),
                    definition_version: descriptor.definition_version,
                }),
                step: descriptor.step.clone(),
                owner: descriptor.owner.clone(),
                descriptor_format: "nasaga-capability-json".to_owned(),
                sha256: managed_document_sha256(&canonical_document),
                canonical_document,
                requested_lease_seconds: plan.lease_seconds,
            }),
            registration_id: descriptor.replica_identity.clone(),
        })
        .await
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Running,
                "gRPC capability registry is unavailable",
                error,
            )
        })?
        .into_inner();
    let accepted_until_ms = response
        .accepted_until
        .as_ref()
        .map(managed_grpc_timestamp_ms)
        .transpose()
        .map_err(|error| {
            saga_error(
                ApplicationPhase::Running,
                format!("gRPC capability receipt deadline is invalid: {error}"),
            )
        })?
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Running,
                "gRPC capability receipt deadline is missing",
            )
        })?;
    if response.registration_id != descriptor.replica_identity {
        return Err(saga_error(
            ApplicationPhase::Running,
            "gRPC capability receipt identity does not match",
        ));
    }
    if response.route_generation == 0 {
        return Err(saga_error(
            ApplicationPhase::Running,
            "gRPC capability receipt route generation is invalid",
        ));
    }
    Ok(nasaga_runtime::CapabilityReceipt {
        accepted_until_ms,
        capability_digest: response.capability_digest,
        route_generation: response.route_generation,
        catalog_generation: 0,
    })
}

/// 业务作用：把显式广播 origin 或 Web 真实监听地址规范化为 capability 中唯一的实例 origin。
///
/// 参数说明：`application` 提供已绑定地址，`advertised_endpoint` 可显式指定受信对外地址。
///
/// 返回：不含路径、凭据、query 或 fragment 的 HTTP(S) origin；地址尚未发布或形态不安全时返回错误。
fn resolve_managed_capability_endpoint(
    application: &Application,
    advertised_endpoint: Option<&str>,
) -> ApplicationResult<String> {
    let candidate = match advertised_endpoint {
        Some(endpoint) => endpoint.to_owned(),
        None => {
            let mut address = application.web_addr().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Running,
                    "Web listener address is unavailable for capability publication",
                )
            })?;
            if address.ip().is_unspecified() {
                address.set_ip(if address.is_ipv4() {
                    std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
                } else {
                    std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
                });
            }
            format!("http://{address}")
        }
    };
    normalize_managed_capability_endpoint(&candidate, ApplicationPhase::Running)
}

/// 业务作用：统一规范化显式广播地址与 listener 地址，拒绝会改变签名路由边界的 URL 成分。
///
/// 参数说明：`candidate` 是待验证地址，`phase` 指定失败所属生命周期阶段。
///
/// 返回：规范化 HTTP(S) origin；路径、凭据、query、fragment 或非法地址导致失败。
fn normalize_managed_capability_endpoint(
    candidate: &str,
    phase: ApplicationPhase,
) -> ApplicationResult<String> {
    let url = reqwest::Url::parse(candidate).map_err(|error| {
        saga_source_error(phase, "capability advertised endpoint is invalid", error)
    })?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(saga_error(
            phase,
            "capability advertised endpoint must be an HTTP origin without path or credentials",
        ));
    }
    Ok(url.origin().ascii_serialization())
}

/// 业务作用：使用与 command/result 相同的原始字节签名合同提交单个 capability descriptor。
///
/// 参数说明：`plan` 固定目标、主体和凭据，`descriptor` 是待登记的逐实例能力。
///
/// 返回：服务端明确接受并返回持久租约收据时成功；网络、状态或收据异常时返回脱敏错误。
async fn post_managed_capability(
    plan: &ManagedCapabilityPublishPlan,
    descriptor: &nasaga_runtime::CapabilityDescriptor,
) -> ApplicationResult<nasaga_runtime::CapabilityReceipt> {
    let (target, remaining) = plan.target.resolve().await?;
    let body = serde_json::to_vec(descriptor).map_err(|error| {
        saga_source_error(
            ApplicationPhase::Running,
            "capability descriptor serialization failed",
            error,
        )
    })?;
    let timestamp_ms = u64::try_from(epoch_millis()?).map_err(|error| {
        saga_source_error(
            ApplicationPhase::Running,
            "capability publication clock is unavailable",
            error,
        )
    })?;
    let nonce = nasaga_runtime::SagaHttpMessageAuthenticator::issue_nonce();
    let signature = target.authenticator.sign(
        &plan.producer,
        &target.signed_path,
        timestamp_ms,
        &nonce,
        &body,
    );
    let response = plan
        .client
        .post(target.url.clone())
        .timeout(remaining)
        .header("content-type", "application/json")
        .header("x-saga-producer", plan.producer.as_str())
        .header("x-saga-timestamp", timestamp_ms.to_string())
        .header("x-saga-nonce", nonce)
        .header("x-saga-signature", signature)
        .body(body)
        .send()
        .await
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Running,
                "capability registry delivery is uncertain",
                error,
            )
        })?;
    if !response.status().is_success() {
        let status = response.status();
        return Err(saga_error(
            ApplicationPhase::Running,
            format!("capability registry rejected the descriptor with HTTP status {status}"),
        ));
    }
    response.json().await.map_err(|error| {
        saga_source_error(
            ApplicationPhase::Running,
            "capability registry receipt is invalid",
            error,
        )
    })
}

/// 业务作用：持续装载共享 Catalog，并按“先 route、后 registry”的顺序发布完整 generation。
///
/// 新 definition 在 route 可用前不会进入 start 快照；deprecated definition 仍保留在 registry。
/// 结果接收可独立复验已发布定义，但不能替代 command 路由与副本确认对执行资格的约束。
///
/// 参数说明：`application` 提供停机状态，`watch` 是冻结控制面计划，`contributor` 控制摘流。
///
/// 返回：应用停机时正常退出；循环内可恢复读取失败只摘流并退避，不丢失当前安全快照。
async fn run_catalog_watch_loop(
    application: Application,
    mut watch: ManagedCatalogWatchPlan,
    contributor: ReadinessContributor,
) -> ApplicationResult<()> {
    let activation_source = watch.activation.clone();
    if !application
        .saga_runtime()
        .catalog_authority
        .bind_security(Arc::new(activation_source.clone()))
        || !application
            .saga_runtime()
            .result_authority
            .bind_security(Arc::new(activation_source.clone()))
    {
        return Err(saga_error(
            ApplicationPhase::Running,
            "Saga Catalog security authority was already bound",
        ));
    }
    loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                // 退役写入可能等待数据库，先撤销本地资格，停机不能继续依赖尚未完成的外部动作。
                application.saga_runtime().revoke_catalog_authority();
                application.saga_runtime().result_authority.revoke();
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                retire_managed_catalog_replica(&watch).await;
                return Ok(());
            }
            ApplicationState::Starting => {
                sleep_observing_state(&application, watch.interval_ms).await;
                continue;
            }
            ApplicationState::Ready => {}
        }
        let next_activation = activation_source.current()?;
        if next_activation.security_generation != watch.activation.security_generation
            || next_activation.publisher_contract_digest
                != watch.activation.publisher_contract_digest
            || next_activation.trusted_result_contracts != watch.activation.trusted_result_contracts
        {
            // 新发布代际即使恢复相同材料也不能沿用旧确认；先摘流，再根据本代快照重新确认。
            application.saga_runtime().revoke_catalog_authority();
            application.saga_runtime().result_authority.revoke();
            contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
        }
        watch.activation = next_activation;
        let publication = publish_managed_local_definitions(&watch).await;
        // candidate 在参与方能力尚未齐全时不能激活，但能力注册本身会推进 Catalog generation。
        // 即使本轮激活被拒绝，也必须继续装载并确认新代，否则副本租约会停在旧代，后续永远
        // 无法证明能力所在 generation 已被全部 Ready 副本接受。
        let result_deadline = Instant::now()
            + Duration::from_millis(watch.interval_ms.saturating_mul(4).clamp(5_000, 60_000));
        let mut result_evidence_valid = false;
        let loaded = load_managed_dynamic_catalog(
            watch.driver,
            &watch.datasource,
            ApplicationPhase::Running,
        )
        .await;
        let applied = match loaded {
            Ok(snapshot) => {
                let result = async {
                    if snapshot.generation < watch.generation {
                        return Err(saga_error(
                            ApplicationPhase::Running,
                            "Saga catalog generation moved backwards",
                        ));
                    }
                    if snapshot.generation != watch.generation
                        || snapshot.snapshot_digest != watch.snapshot_digest
                    {
                        // 完整快照尚未确认时先摘流，不能沿用旧 generation 的执行资格。
                        application.saga_runtime().revoke_catalog_authority();
                        contributor.observe(
                            DependencyState::NotReady,
                            reason::NOT_READY,
                            Instant::now(),
                        );
                    }
                    watch
                        .runtime
                        .verify_registry_snapshot(&snapshot.registry)
                        .await
                        .map_err(|error| {
                            saga_source_error(
                                ApplicationPhase::Running,
                                "Saga catalog snapshot is incompatible with active instances",
                                error,
                            )
                        })?;
                    validate_managed_registry_result_contract(
                        &watch.activation,
                        &snapshot.registry,
                        ApplicationPhase::Running,
                    )?;
                    // 仅 capability 变化不能阻断已提交结果。定义集合必须与已发布 registry 完全一致，
                    // 不能借结果恢复提前发布尚未经过 route 与副本确认的新 definition。
                    result_evidence_valid = watch
                        .result_registry
                        .definitions_with_tenants()
                        .map(|(tenant, definition)| {
                            (
                                tenant,
                                definition.name(),
                                definition.version(),
                                definition.digest(),
                            )
                        })
                        .eq(snapshot.registry.definitions_with_tenants().map(
                            |(tenant, definition)| {
                                (
                                    tenant,
                                    definition.name(),
                                    definition.version(),
                                    definition.digest(),
                                )
                            },
                        ));
                    if result_evidence_valid {
                        // 期限从本轮读取前起算，数据库等待不会延长旧证据；凭据变化仍在每次入口即时复验。
                        application
                            .saga_runtime()
                            .result_authority
                            .confirm_with_security(
                                result_deadline,
                                catalog_authority::CatalogSecurityEpoch::epoch(&watch.activation),
                            );
                    } else {
                        // 定义有增减或摘要变化时，旧结果资格不能沿用；等待完整 registry 安全发布后再开放。
                        application.saga_runtime().result_authority.revoke();
                    }
                    #[cfg(feature = "web")]
                    if let Some(routing) = watch.http_routing.as_ref() {
                        let authenticator = watch.http_authenticator.as_ref().ok_or_else(|| {
                            saga_error(
                                ApplicationPhase::Running,
                                "dynamic Saga HTTP routing has no command credential",
                            )
                        })?;
                        let next_routes = build_dynamic_http_routing(
                            &snapshot.registry,
                            &snapshot.capabilities,
                            authenticator,
                            watch.activation.address_policy.as_ref().ok_or_else(|| {
                                saga_error(
                                    ApplicationPhase::Running,
                                    "dynamic Saga HTTP routing has no address policy",
                                )
                            })?,
                            false,
                        )?;
                        // 新 definition 的 route 必须先于 start 快照可见，避免创建实例后首条 command 无法投递。
                        *routing
                            .write()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) =
                            ManagedHttpRoutingSnapshot {
                                command_targets: next_routes,
                            };
                    }
                    #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
                    if let Some(routing) = watch.kafka_routing.as_ref() {
                        let command_prefix =
                            watch.kafka_command_prefix.as_deref().ok_or_else(|| {
                                saga_error(
                                    ApplicationPhase::Running,
                                    "dynamic Saga Kafka routing has no command topic prefix",
                                )
                            })?;
                        let next_topics = build_managed_kafka_command_topics(
                            &snapshot.registry,
                            &snapshot.capabilities,
                            command_prefix,
                            true,
                            watch.activation.address_policy.as_ref(),
                            false,
                        )?;
                        // 同代 Kafka topic 映射先于 definition registry 发布，确保新实例首条命令有唯一目标。
                        *routing
                            .write()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) =
                            ManagedKafkaRoutingSnapshot {
                                command_topics: next_topics,
                            };
                    }
                    #[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
                    if let Some(routing) = watch.redis_routing.as_ref() {
                        let client = watch.redis_client.as_ref().ok_or_else(|| {
                            saga_error(
                                ApplicationPhase::Running,
                                "dynamic Saga Redis routing has no managed client",
                            )
                        })?;
                        let auth = watch.redis_command_auth.as_ref().ok_or_else(|| {
                            saga_error(
                                ApplicationPhase::Running,
                                "dynamic Saga Redis routing has no command signing key",
                            )
                        })?;
                        let next_publishers = build_dynamic_redis_command_publishers(
                            &snapshot.registry,
                            &snapshot.capabilities,
                            Arc::clone(client),
                            auth,
                            watch.redis_key_tag.as_deref(),
                            watch.activation.address_policy.as_ref().ok_or_else(|| {
                                saga_error(
                                    ApplicationPhase::Running,
                                    "dynamic Saga Redis routing has no address policy",
                                )
                            })?,
                            false,
                        )?;
                        // 新代 stream 映射先于 definition 快照发布，避免首条 command 命中空路由。
                        *routing
                            .write()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) =
                            ManagedRedisRoutingSnapshot {
                                command_publishers: next_publishers,
                            };
                    }
                    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
                    if let Some(routing) = watch.grpc_routing.as_ref() {
                        let credential = watch.grpc_credential.as_deref().ok_or_else(|| {
                            saga_error(
                                ApplicationPhase::Running,
                                "dynamic Saga gRPC routing has no client credential",
                            )
                        })?;
                        let timeout = watch.grpc_timeout.ok_or_else(|| {
                            saga_error(
                                ApplicationPhase::Running,
                                "dynamic Saga gRPC routing has no request deadline",
                            )
                        })?;
                        let next_targets = build_dynamic_grpc_command_targets(
                            &snapshot.registry,
                            &snapshot.capabilities,
                            credential,
                            timeout,
                            watch.activation.address_policy.as_ref().ok_or_else(|| {
                                saga_error(
                                    ApplicationPhase::Running,
                                    "dynamic Saga gRPC routing has no address policy",
                                )
                            })?,
                            false,
                        )?;
                        for target in next_targets.values() {
                            probe_managed_grpc_target(target, ApplicationPhase::Running).await?;
                        }
                        // 同代平衡 channel 先于 definition registry 发布，连接可用性由 channel 独立管理。
                        *routing
                            .write()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) =
                            ManagedGrpcRoutingSnapshot {
                                command_targets: next_targets,
                            };
                    }
                    // 只有当前 transport 的路由、安全政策与远端探测全部成功后才确认快照，
                    // 否则其他副本可能把“已读取但不可投递”误判为可激活证据。
                    let deadline = acknowledge_managed_catalog_snapshot(&watch, &snapshot).await?;
                    // 任一有效 Ready 副本尚未确认同代同摘要同 publisher 合同时，本副本也不能发布
                    // active registry；否则 approved definition 会在不同副本形成并行权威。
                    if !managed_catalog_generation_is_acknowledged(&watch, &snapshot).await? {
                        return Err(saga_error(
                            ApplicationPhase::Running,
                            "Saga Catalog generation has not converged across Ready replicas",
                        ));
                    }
                    if activate_managed_validated_candidate(&watch, &snapshot).await? {
                        // 激活会原子推进 Catalog generation，旧快照不得再发布为权威运行态。
                        application.saga_runtime().revoke_catalog_authority();
                        application.saga_runtime().result_authority.revoke();
                        return Ok(None);
                    }
                    watch.result_registry = snapshot.registry.clone();
                    watch.runtime.replace_registry(snapshot.registry);
                    watch.generation = snapshot.generation;
                    watch.snapshot_digest = snapshot.snapshot_digest;
                    Ok(Some(deadline))
                }
                .await;
                result
            }
            Err(error) => Err(error),
        };
        match (publication, applied) {
            (Ok(()), Ok(Some(deadline))) => {
                // 完整 registry 已发布后才允许新增定义接收结果，结果资格不延长读取前固定的期限。
                application
                    .saga_runtime()
                    .result_authority
                    .confirm_with_security(
                        result_deadline,
                        catalog_authority::CatalogSecurityEpoch::epoch(&watch.activation),
                    );
                // registry 已经发布；只有完整确认仍在租期内才同时开放业务门禁和探针。
                if application
                    .saga_runtime()
                    .catalog_authority
                    .confirm_with_security(
                        deadline,
                        catalog_authority::CatalogSecurityEpoch::epoch(&watch.activation),
                    )
                {
                    contributor.observe_ready_until(deadline);
                } else {
                    contributor.observe(
                        DependencyState::NotReady,
                        reason::NOT_READY,
                        Instant::now(),
                    );
                }
            }
            (Ok(()), Ok(None)) => {
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
            }
            (Err(error), Ok(_)) => {
                // 定义发布未完成时不恢复本地执行资格，探针与业务门禁保持同一结论。
                application.saga_runtime().revoke_catalog_authority();
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                tracing::warn!(error = %error, "Saga Catalog 本地定义发布未完成");
            }
            (_, Err(error)) => {
                if !result_evidence_valid {
                    // Catalog 读取、在途合同或结果身份验证失败时没有独立证据，必须同时关闭结果入口。
                    application.saga_runtime().result_authority.revoke();
                }
                // 无法确认新快照时立即关闭资格，保留旧数据只用于下一轮完整复验。
                application.saga_runtime().revoke_catalog_authority();
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                tracing::warn!(error = %error, "Saga Catalog 快照装载失败，保留上一代安全快照");
            }
        }
        sleep_observing_state(&application, watch.interval_ms).await;
    }
}

/// 业务作用：使用对应数据库权威持久确认本副本已复验的完整 Catalog 快照。
///
/// 参数说明：`watch` 固定副本身份与 datasource，`snapshot` 是待发布的同代内容。
///
/// 返回：确认已提交且期限仍有效时返回单调截止时刻；代际竞争、过期或数据库失败返回运行期错误。
async fn acknowledge_managed_catalog_snapshot(
    watch: &ManagedCatalogWatchPlan,
    snapshot: &nasaga_runtime::DynamicCatalogSnapshot,
) -> ApplicationResult<Instant> {
    let lease_ms = watch.interval_ms.saturating_mul(4).clamp(5_000, 60_000);
    let gate = managed_definition_activation_gate(
        &watch.service_identity,
        snapshot.generation,
        snapshot.snapshot_digest.clone(),
        &watch.activation,
        ApplicationPhase::Running,
    )?;
    let started = Instant::now();
    let lease_until_ms = match watch.driver {
        natx_core::DatabaseDriver::MySql => {
            #[cfg(feature = "saga")]
            {
                nasaga_runtime::acknowledge_catalog_generation_for(
                    &watch.datasource,
                    &watch.service_identity,
                    &watch.replica_identity,
                    snapshot.generation,
                    &snapshot.snapshot_digest,
                    gate.activation_contract_digest(),
                    lease_ms,
                )
                .await
                .map_err(|error| {
                    saga_source_error(
                        ApplicationPhase::Running,
                        "managed MySQL Catalog acknowledgement failed",
                        error,
                    )
                })?
            }
            #[cfg(not(feature = "saga"))]
            return Err(saga_error(
                ApplicationPhase::Running,
                "MySQL Catalog acknowledgement is unavailable",
            ));
        }
        natx_core::DatabaseDriver::PostgreSql => {
            #[cfg(feature = "saga-pgsql")]
            {
                nasaga_runtime_pgsql::acknowledge_catalog_generation_for(
                    &watch.datasource,
                    &watch.service_identity,
                    &watch.replica_identity,
                    snapshot.generation,
                    &snapshot.snapshot_digest,
                    gate.activation_contract_digest(),
                    lease_ms,
                )
                .await
                .map_err(|error| {
                    saga_source_error(
                        ApplicationPhase::Running,
                        "managed PostgreSQL Catalog acknowledgement failed",
                        error,
                    )
                })?
            }
            #[cfg(not(feature = "saga-pgsql"))]
            return Err(saga_error(
                ApplicationPhase::Running,
                "PostgreSQL Catalog acknowledgement is unavailable",
            ));
        }
    };
    // 数据库成功回包仅证明提交；网络延迟和本轮后续等待不能延长确认期限。
    capability_lease::receipt_deadline(lease_until_ms, lease_ms, started, epoch_millis()?)
        .ok_or_else(|| {
            saga_error(
                ApplicationPhase::Running,
                "Catalog acknowledgement lease has expired or exceeded the local lease budget",
            )
        })
}

/// 业务作用：在退出 watcher 前撤销当前副本的 Catalog Ready 租约，避免排空实例继续阻塞激活。
///
/// 参数说明：`watch` 提供精确数据库与副本身份。
///
/// 返回：无返回值；停机路径只记录数据库失败并依赖既有租约自然到期。
async fn retire_managed_catalog_replica(watch: &ManagedCatalogWatchPlan) {
    let result = match watch.driver {
        natx_core::DatabaseDriver::MySql => {
            #[cfg(feature = "saga")]
            {
                nasaga_runtime::retire_catalog_replica_for(
                    &watch.datasource,
                    &watch.service_identity,
                    &watch.replica_identity,
                )
                .await
            }
            #[cfg(not(feature = "saga"))]
            Ok(())
        }
        natx_core::DatabaseDriver::PostgreSql => {
            #[cfg(feature = "saga-pgsql")]
            {
                nasaga_runtime_pgsql::retire_catalog_replica_for(
                    &watch.datasource,
                    &watch.service_identity,
                    &watch.replica_identity,
                )
                .await
            }
            #[cfg(not(feature = "saga-pgsql"))]
            Ok(())
        }
    };
    if let Err(error) = result {
        tracing::warn!(error = %error, "Saga Catalog 副本租约将在截止时间后失效");
    }
}

/// 业务作用：幂等发布当前 workflow owner 链接的完整定义，激活统一由同代快照门禁裁决。
///
/// 参数说明：`watch` 同时固定数据源、发布主体、产物集合与激活策略。
///
/// 返回：全部定义已首次写入或幂等命中时成功；任一发布裁决不明时返回错误。
async fn publish_managed_local_definitions(
    watch: &ManagedCatalogWatchPlan,
) -> ApplicationResult<()> {
    let Some(publisher) = watch.definition_publisher.as_ref() else {
        return Ok(());
    };
    for artifact in &watch.definition_artifacts {
        match watch.driver {
            natx_core::DatabaseDriver::MySql => {
                #[cfg(feature = "saga")]
                {
                    nasaga_runtime::publish_definition_for(&watch.datasource, publisher, artifact)
                        .await
                        .map(|_| ())
                        .map_err(|error| {
                            saga_source_error(
                                ApplicationPhase::Running,
                                "managed MySQL definition publication failed",
                                error,
                            )
                        })?
                }
                #[cfg(not(feature = "saga"))]
                return Err(saga_error(
                    ApplicationPhase::Running,
                    "MySQL definition publication is unavailable",
                ));
            }
            natx_core::DatabaseDriver::PostgreSql => {
                #[cfg(feature = "saga-pgsql")]
                {
                    nasaga_runtime_pgsql::publish_definition_for(
                        &watch.datasource,
                        publisher,
                        artifact,
                    )
                    .await
                    .map(|_| ())
                    .map_err(|error| {
                        saga_source_error(
                            ApplicationPhase::Running,
                            "managed PostgreSQL definition publication failed",
                            error,
                        )
                    })?
                }
                #[cfg(not(feature = "saga-pgsql"))]
                return Err(saga_error(
                    ApplicationPhase::Running,
                    "PostgreSQL definition publication is unavailable",
                ));
            }
        };
    }
    Ok(())
}

/// 业务作用：从刚被本副本确认的快照中选取一个能力完整候选定义并原子激活。
///
/// 参数说明：`watch` 固定 Orchestrator 身份、transport 与后端，`snapshot` 同时
/// 携带 candidate、有效 capability 和全副本确认所需的摘要。
///
/// 返回：已推进 generation 时返回真；策略不自动激活、能力未齐或副本未闭合时返回假；
/// 数据库裁决不明时返回运行期错误。
async fn activate_managed_validated_candidate(
    watch: &ManagedCatalogWatchPlan,
    snapshot: &nasaga_runtime::DynamicCatalogSnapshot,
) -> ApplicationResult<bool> {
    if watch.activation_policy != "validated" {
        return Ok(false);
    }
    let mut candidate = None;
    for record in &snapshot.definition_records {
        if record.lifecycle != nasaga_runtime::DefinitionLifecycle::Candidate {
            continue;
        }
        let complete =
            managed_candidate_capabilities_are_routable(watch, snapshot, &record.artifact)?;
        if complete {
            candidate = Some(record);
            break;
        }
    }
    let Some(record) = candidate else {
        return Ok(false);
    };
    probe_managed_candidate_grpc_routes(
        &watch.activation,
        snapshot,
        &record.artifact,
        ApplicationPhase::Running,
    )
    .await?;
    if !managed_catalog_generation_is_acknowledged(watch, snapshot).await? {
        return Ok(false);
    }
    let gate = managed_definition_activation_gate(
        &watch.service_identity,
        snapshot.generation,
        snapshot.snapshot_digest.clone(),
        &watch.activation,
        ApplicationPhase::Running,
    )?;
    let operation = nasaga_runtime::DefinitionLifecycleOperation::for_validated_activation(
        &record.artifact,
        snapshot.generation,
        "validated activation policy confirmed the complete Catalog snapshot",
    )
    .map_err(|error| {
        saga_source_error(
            ApplicationPhase::Running,
            "managed Saga automatic activation operation is invalid",
            error,
        )
    })?;
    match watch.driver {
        natx_core::DatabaseDriver::MySql => {
            #[cfg(feature = "saga")]
            nasaga_runtime::activate_definition_with_operation_for(
                &watch.datasource,
                &watch.service_identity,
                &gate,
                &record.artifact.tenant,
                &record.artifact.workflow,
                record.artifact.definition_version,
                &operation,
            )
            .await
            .map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Running,
                    "managed MySQL definition activation failed",
                    error,
                )
            })?;
            #[cfg(not(feature = "saga"))]
            return Err(saga_error(
                ApplicationPhase::Running,
                "MySQL definition activation is unavailable",
            ));
        }
        natx_core::DatabaseDriver::PostgreSql => {
            #[cfg(feature = "saga-pgsql")]
            nasaga_runtime_pgsql::activate_definition_with_operation_for(
                &watch.datasource,
                &watch.service_identity,
                &gate,
                &record.artifact.tenant,
                &record.artifact.workflow,
                record.artifact.definition_version,
                &operation,
            )
            .await
            .map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Running,
                    "managed PostgreSQL definition activation failed",
                    error,
                )
            })?;
            #[cfg(not(feature = "saga-pgsql"))]
            return Err(saga_error(
                ApplicationPhase::Running,
                "PostgreSQL definition activation is unavailable",
            ));
        }
    }
    Ok(true)
}

/// 业务作用：用与实际 publisher 一致的语义确认 candidate 的每个步骤已有可发布路由。
///
/// 参数说明：`watch` 固定 transport 和地址政策，`snapshot` 提供当前有效租约，
/// `artifact` 提供已封签的 owner 与步骤语义。
///
/// 返回：HTTP 每步至少一个合法实例，或其他 transport 每步恰有一个合法端点时为真；
/// 缺失、歧义、语义漂移或越过地址政策时为假。
fn managed_candidate_capabilities_are_routable(
    watch: &ManagedCatalogWatchPlan,
    snapshot: &nasaga_runtime::DynamicCatalogSnapshot,
    artifact: &nasaga_runtime::DefinitionArtifact,
) -> ApplicationResult<bool> {
    candidate_capabilities_are_routable(
        &watch.activation,
        snapshot,
        artifact,
        ApplicationPhase::Running,
    )
}

/// 业务作用：按指定 transport 与地址政策复验一个候选定义的完整可路由性。
///
/// 参数说明：`activation` 冻结当前数据面、Redis 同槽配置、result 凭据或后端、地址政策和接收身份，
/// `snapshot` 提供有效 capability，`artifact` 提供已封签流程，`phase` 标记调用阶段。
///
/// 返回：HTTP 每步至少一个合法实例，或其他 transport 每步恰有一个合法端点时为真；
/// 定义无效时返回阶段错误，其他路由缺口返回假。
fn candidate_capabilities_are_routable(
    activation: &ManagedDefinitionActivationContract,
    snapshot: &nasaga_runtime::DynamicCatalogSnapshot,
    artifact: &nasaga_runtime::DefinitionArtifact,
    phase: ApplicationPhase,
) -> ApplicationResult<bool> {
    let transport = activation.transport;
    let definition = artifact.to_definition().map_err(|error| {
        saga_source_error(phase, "managed Saga candidate definition is invalid", error)
    })?;
    Ok(definition.steps().iter().all(|step| {
        let matching = snapshot
            .capabilities
            .iter()
            .filter(|capability| {
                capability.descriptor.transport == transport.as_str()
                    && capability
                        .descriptor
                        .matches_step(&artifact.tenant, &definition, step)
            })
            .collect::<Vec<_>>();
        // 一条语义匹配但 route 非法或越界的有效租约也会进入实际 publisher 的完整快照；
        // candidate 必须保持保护态，不能用同步骤的另一条合法租约掩盖它。
        if matching.is_empty()
            || matching.iter().any(|capability| {
                nasaga_runtime::validate_capability_route_contract(&capability.descriptor).is_err()
                    || (transport == SagaTransportKind::Kafka
                        && capability.descriptor.result_contract_digest.as_deref()
                            != activation.result_backend_digest.as_deref())
                    || (transport != SagaTransportKind::Kafka
                        && !capability
                            .descriptor
                            .result_contract_digest
                            .as_deref()
                            .is_some_and(|digest| {
                                activation
                                    .trusted_result_contracts
                                    .get(step.owner().as_str())
                                    .is_some_and(|contracts| contracts.contains(digest))
                            }))
                    || (transport == SagaTransportKind::RedisStream
                        && activation.redis_key_tag.as_deref().is_none_or(|key_tag| {
                            nasaga_runtime::validate_redis_stream_route(
                                &capability.descriptor.endpoint,
                                Some(key_tag),
                            )
                            .is_err()
                        }))
                    || activation.address_policy.as_ref().is_some_and(|policy| {
                        validate_capability_address(policy, &capability.descriptor, phase).is_err()
                    })
            })
        {
            return false;
        }
        if transport == SagaTransportKind::Http {
            return true;
        }
        nasaga_runtime::select_capability_route(
            &snapshot.capabilities,
            transport.as_str(),
            &artifact.tenant,
            &definition,
            step,
        )
        .is_ok_and(|descriptor| descriptor.is_some())
    }))
}

/// 业务作用：在自动激活 candidate 前证明全部有效 Ready 副本确认指定同代同摘要快照。
///
/// 参数说明：`watch` 固定逻辑服务与数据库后端，`snapshot` 提供本轮刚确认的代际和摘要。
///
/// 返回：集群确认闭合时返回真；查询失败返回运行期错误。
async fn managed_catalog_generation_is_acknowledged(
    watch: &ManagedCatalogWatchPlan,
    snapshot: &nasaga_runtime::DynamicCatalogSnapshot,
) -> ApplicationResult<bool> {
    let gate = managed_definition_activation_gate(
        &watch.service_identity,
        snapshot.generation,
        snapshot.snapshot_digest.clone(),
        &watch.activation,
        ApplicationPhase::Running,
    )?;
    match watch.driver {
        natx_core::DatabaseDriver::MySql => {
            #[cfg(feature = "saga")]
            return nasaga_runtime::catalog_generation_fully_acknowledged_for(
                &watch.datasource,
                &watch.service_identity,
                snapshot.generation,
                &snapshot.snapshot_digest,
                gate.activation_contract_digest(),
            )
            .await
            .map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Running,
                    "managed MySQL Catalog acknowledgement query failed",
                    error,
                )
            });
            #[cfg(not(feature = "saga"))]
            return Err(saga_error(
                ApplicationPhase::Running,
                "MySQL Catalog acknowledgement query is unavailable",
            ));
        }
        natx_core::DatabaseDriver::PostgreSql => {
            #[cfg(feature = "saga-pgsql")]
            return nasaga_runtime_pgsql::catalog_generation_fully_acknowledged_for(
                &watch.datasource,
                &watch.service_identity,
                snapshot.generation,
                &snapshot.snapshot_digest,
                gate.activation_contract_digest(),
            )
            .await
            .map_err(|error| {
                saga_source_error(
                    ApplicationPhase::Running,
                    "managed PostgreSQL Catalog acknowledgement query failed",
                    error,
                )
            });
            #[cfg(not(feature = "saga-pgsql"))]
            return Err(saga_error(
                ApplicationPhase::Running,
                "PostgreSQL Catalog acknowledgement query is unavailable",
            ));
        }
    }
}

/// 业务作用：把 timer、stream 与 Catalog 循环收敛为单一受监督入口——任一循环异常退出都
/// 视为关键任务失败,由 Runner 触发统一停机;正常停机时两循环各自观察应用状态退出。
///
/// 参数说明：
/// - `tasks`: 已按当前角色构造的非空受监督循环集合。
///
/// 返回：全部循环正常退出返回 `Ok`;任一循环错误或 panic 返回关键任务错误。
async fn run_saga_supervised_loops(
    tasks: Vec<ApplicationFuture<'static>>,
) -> ApplicationResult<()> {
    let mut set = tokio::task::JoinSet::new();
    for task in tasks {
        set.spawn(task);
    }
    while let Some(joined) = set.join_next().await {
        joined.map_err(|_| {
            saga_error(
                ApplicationPhase::Running,
                "saga supervised loop terminated abnormally",
            )
        })??;
    }
    Ok(())
}

/// 业务作用：持续轮询受管 stream 消费者,按封闭裁决推进并以 readiness 表达连续故障。
///
/// 停机语义固定:观察到应用停止即**先关领取**——不再发起新的 XREADGROUP/XAUTOCLAIM;
/// 在途轮次内已接管的消息由 `poll_once` 自身排空(handler 完成或超时留 PEL),未确认
/// 消息留在 PEL 交由重启后重领;Redis 连接由更早启动的 Redis 组件在本组件之后释放。
///
/// 参数说明：
/// - `application`: 观察统一停机状态。
/// - `client`: 受管 Redis 客户端。
/// - `pollers`: 冻结的消费者集合。
/// - `runtimes`: 与消费者一一对应的观测状态。
/// - `poll_idle_ms`: 轮间让位间歇。
/// - `error_backoff_ms`: 故障退避。
/// - `contributor`: stream 消费独占的动态就绪贡献项。
///
/// 返回：应用进入停机态时正常退出;系统时钟不可表示时返回关键任务错误。
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
async fn run_stream_poll_loop(
    application: Application,
    client: Arc<nadis::RedisClient>,
    pollers: Vec<Arc<dyn SagaStreamPoller>>,
    runtimes: Vec<Arc<StreamRuntime>>,
    poll_idle_ms: u64,
    error_backoff_ms: u64,
    contributor: ReadinessContributor,
) -> ApplicationResult<()> {
    loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                return Ok(());
            }
            ApplicationState::Starting => {
                sleep_observing_state(&application, poll_idle_ms).await;
                continue;
            }
            ApplicationState::Ready => {}
        }
        let now_ms = epoch_millis()?;
        let mut round_healthy = true;
        for (poller, runtime) in pollers.iter().zip(runtimes.iter()) {
            // 停机信号在流与流之间复查:先关领取,不把停机窗口拖长到整轮结束。
            if !matches!(application.state(), ApplicationState::Ready) {
                break;
            }
            match poller.poll_once(&client, now_ms).await {
                Ok(report) => {
                    runtime.absorb(&report);
                    // 积压 gauge 与消费同轮刷新:pending 与最老 PEL 年龄是"消费是否
                    // 追得上"的直接证据;探测失败与消费失败同等计入轮失败,不导出
                    // 陈旧假数据。
                    let config = poller.config();
                    match nasaga_runtime::stream_group_backlog(
                        &client,
                        &config.stream,
                        &config.group,
                        now_ms,
                    )
                    .await
                    {
                        Ok((pending, oldest_age_ms)) => {
                            runtime.set_backlog(pending, oldest_age_ms);
                            runtime.healthy.store(true, Ordering::Relaxed);
                        }
                        Err(_) => {
                            round_healthy = false;
                            runtime.failed_rounds.fetch_add(1, Ordering::Relaxed);
                            runtime.healthy.store(false, Ordering::Relaxed);
                        }
                    }
                }
                Err(_) => {
                    // Redis 往返失败:消息原位保留(PEL/stream 不动),只退避重试;
                    // 摘流由 contributor 阈值统一裁决,不在单轮内武断退出。
                    round_healthy = false;
                    runtime.failed_rounds.fetch_add(1, Ordering::Relaxed);
                    runtime.healthy.store(false, Ordering::Relaxed);
                }
            }
        }
        let delay = if round_healthy {
            contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
            poll_idle_ms
        } else {
            contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
            error_backoff_ms
        };
        // 停机不等退避:分片睡眠把响应上界固定在一个分片内,停机信号落在退避中途
        // 也立即收口;循环顶部据状态退出,未确认消息留在 PEL 交给重启后重领。
        sleep_observing_state(&application, delay).await;
    }
}

/// 业务作用：可被停机打断的分片睡眠——把任意长的退避/轮询间歇切成 ≤200ms 片,
/// 每片后复查应用状态。停机信号无论落在睡眠的哪个时刻,响应上界都固定在一个分片,
/// 不会被 60 秒级故障退避拖满。
///
/// 参数说明：
/// - `application`: 观察统一停机状态。
/// - `total_ms`: 期望睡眠总时长(毫秒)。
///
/// 返回：睡满或状态离开 Ready 提前返回;由调用方循环顶部统一裁决去留。
async fn sleep_observing_state(application: &Application, total_ms: u64) {
    let mut remaining = total_ms;
    while remaining > 0 {
        let slice = remaining.min(200);
        tokio::time::sleep(Duration::from_millis(slice)).await;
        remaining -= slice;
        if !matches!(application.state(), ApplicationState::Ready) {
            return;
        }
    }
}

/// 业务作用：持续领取并裁决到期 timer，以 readiness 表达持久化依赖的连续故障与恢复。
///
/// 参数说明：
/// - `application`：用于观察统一停机状态，确保失权后立即退出轮询。
/// - `orchestrator`：唯一通过 Ready 门禁的推进运行时。
/// - `timer_owner`：当前副本的 fencing 身份。
/// - `settings`：正常轮询、错误退避和摘流阈值。
/// - `contributor`：Saga 独占的动态就绪贡献项。
///
/// 返回：应用进入停机态时正常退出；系统时钟不可表示时返回关键任务错误并触发失败停机。
async fn run_timer_loop(
    application: Application,
    orchestrator: SagaOrchestratorApi,
    timer_owner: String,
    settings: SagaSettings,
    contributor: ReadinessContributor,
) -> ApplicationResult<()> {
    let (poll_interval_ms, error_backoff_ms, operation_timeout_ms, _) = settings.timer_settings();
    loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                return Ok(());
            }
            ApplicationState::Starting => {
                sleep_observing_state(&application, poll_interval_ms).await;
                continue;
            }
            ApplicationState::Ready => {}
        }

        if application.saga_runtime().ensure_ready().is_err() {
            contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
            sleep_observing_state(&application, poll_interval_ms).await;
            continue;
        }

        let now_ms = epoch_millis()?;
        let operation = tokio::time::timeout(
            Duration::from_millis(operation_timeout_ms),
            orchestrator.run_due_timers(&timer_owner, now_ms),
        )
        .await;
        let delay = match operation {
            Ok(Ok(_)) => {
                contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
                poll_interval_ms
            }
            Ok(Err(_)) | Err(_) => {
                // timer 读取或提交失败时保留持久化事实并退避；阈值由 contributor 统一裁决摘流，
                // 绝不把未提交轮次当成成功，也不因瞬时存储故障主动伪造业务超时结论。
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                error_backoff_ms
            }
        };
        // 退避同样必须可被停机打断:分片睡眠保证失权后一个分片内退出轮询。
        sleep_observing_state(&application, delay).await;
    }
}

/// 业务作用：从同一配置快照读取 Saga 段并校验角色、预算与可靠发起的事务域约束。
///
/// 参数说明：
/// - `application`：提供同版本不可变配置快照。
/// - `phase`：错误发生的真实生命周期阶段。
///
/// 返回：角色与预算合法且可靠发起的 Outbox 配置同源时返回设置；缺少配置、字段非法或事务域冲突返回阶段错误。
fn read_saga_settings(
    application: &Application,
    phase: ApplicationPhase,
) -> ApplicationResult<SagaSettings> {
    let snapshot = application.config();
    let settings = match snapshot.value().get("saga") {
        Some(value) => serde_json::from_value(value.clone()).map_err(|error| {
            saga_source_error(phase, "invalid saga configuration section", error)
        })?,
        None => {
            return Err(saga_error(
                phase,
                "declaring the saga component requires a saga configuration section",
            ))
        }
    };
    validate_settings(&settings, phase)?;
    validate_reliable_client_outbox_binding(snapshot.value(), &settings, phase)?;
    Ok(settings)
}

/// 业务作用：在配置发布前验证 Saga 段，阻止不可执行预算与可靠发起的跨事务域配置进入运行快照。
///
/// 参数说明：
/// - `tree`：合并完成但尚未发布的候选配置树。
/// - `phase`：启动或运行期配置校验阶段。
///
/// 返回：段缺失或设置合法时成功；反序列化、预算和可靠发起的事务域冲突拒绝整帧配置。
pub(crate) fn validate_saga_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let settings = match tree.get("saga") {
        Some(value) => serde_json::from_value(value.clone()).map_err(|error| {
            saga_source_error(phase, "invalid saga configuration section", error)
        })?,
        None => return Ok(()),
    };
    validate_settings(&settings, phase)?;
    validate_reliable_client_outbox_binding(tree, &settings, phase)
}

/// 业务作用：拒绝可靠 client 的显式 Outbox 数据源歧义，保证已受理事件的写入与扫描归属一致。
///
/// 参数说明：
/// - `tree`：包含 Saga 与 Outbox 段的同一配置快照。
/// - `settings`：已通过角色和字段校验的 Saga 设置。
/// - `phase`：配置校验所在的生命周期阶段。
///
/// 返回：非受管可靠 client 或显式数据源一致时成功；冲突时返回配置键诊断，不发布该配置。
fn validate_reliable_client_outbox_binding(
    tree: &serde_json::Value,
    settings: &SagaSettings,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    if settings.plan_mode != SagaPlanMode::Managed
        || settings.role != Some(SagaRole::Client)
        || !settings.client.reliable_start
    {
        return Ok(());
    }
    // 仅轮询预算不声明数据源；可靠计划始终绑定 client 事务域。显式冲突必须在发布能力前拒绝，
    // 避免把配置错误隐藏为 Ready 后永久滞留的可靠事件。
    if let Some(datasource) = tree.pointer("/outbox/datasource_ref") {
        if datasource.as_str() != settings.client.datasource_ref.as_deref() {
            return Err(saga_error(
                phase,
                "reliable Saga client requires outbox.datasource_ref to match saga.client.datasource_ref when explicitly configured",
            ));
        }
    }
    Ok(())
}

/// 业务作用：约束 timer 周期和失败阈值，防止忙循环或超长失联窗口。
///
/// 参数说明：
/// - `settings`：待校验的 Saga 运行设置。
/// - `phase`：形成错误时的真实生命周期阶段。
///
/// 返回：所有值位于封闭范围时成功，否则返回不含配置原值的错误。
fn validate_settings(settings: &SagaSettings, phase: ApplicationPhase) -> ApplicationResult<()> {
    if !(1..=3_600_000).contains(&settings.credential_overlap_ms) {
        return Err(saga_error(
            phase,
            "Saga credential overlap must be between 1 and 3600000 milliseconds",
        ));
    }
    let role = settings
        .role
        .ok_or_else(|| saga_error(phase, "saga.role is required"))?;
    if role == SagaRole::Combined && !settings.allow_combined_role {
        return Err(saga_error(
            phase,
            "saga.role=combined requires saga.allow_combined_role=true",
        ));
    }
    if settings.plan_mode == SagaPlanMode::Managed && settings.datasource_ref.is_some() {
        return Err(saga_error(
            phase,
            "managed Saga requires datasource_ref inside the selected role",
        ));
    }
    let datasource_refs = role_datasource_refs(settings, role, phase)?;
    for reference in datasource_refs {
        natx_core::DatasourceRef::new(reference).map_err(|error| {
            saga_source_error(
                phase,
                "Saga role datasource_ref is not a canonical datasource qualifier",
                error,
            )
        })?;
    }
    if settings.database_bootstrap == SagaDatabaseBootstrap::UserHook
        && settings.primary_datasource_ref() != Some(crate::db::DEFAULT_DATASOURCE)
    {
        return Err(saga_error(
            phase,
            "saga.database_bootstrap=user_hook supports only datasource_ref=default",
        ));
    }
    let (poll_interval_ms, error_backoff_ms, operation_timeout_ms, failure_threshold) =
        settings.timer_settings();
    if !(10..=MAX_TIMER_INTERVAL_MS).contains(&poll_interval_ms) {
        return Err(saga_error(
            phase,
            "saga timer poll interval is outside the supported range",
        ));
    }
    if !(10..=MAX_TIMER_INTERVAL_MS).contains(&error_backoff_ms) {
        return Err(saga_error(
            phase,
            "saga timer error backoff is outside the supported range",
        ));
    }
    if !(10..=MAX_TIMER_INTERVAL_MS).contains(&operation_timeout_ms) {
        return Err(saga_error(
            phase,
            "saga timer operation timeout is outside the supported range",
        ));
    }
    if !(1..=MAX_TIMER_FAILURE_THRESHOLD).contains(&failure_threshold) {
        return Err(saga_error(
            phase,
            "saga timer failure threshold is outside the supported range",
        ));
    }
    if let Some(path) = settings.http.base_path.as_deref() {
        validate_saga_base_path(path, phase)?;
    }
    if settings.plan_mode == SagaPlanMode::Managed {
        validate_managed_role_settings(settings, role, phase)?;
        validate_managed_transport_settings(settings, role, phase)?;
        validate_managed_api_settings(settings, role, phase)?;
    }
    Ok(())
}

/// 业务作用：枚举当前角色实际拥有的本地事务域，禁止顶层默认数据源替代角色授权。
///
/// 参数说明：
/// - `settings`: 已完成结构解析的 Saga 配置。
/// - `role`: 当前部署的封闭角色。
/// - `phase`: 形成错误时的生命周期阶段。
///
/// 返回：角色数据源完整时返回全部显式引用；缺失或含混时拒绝启动。
fn role_datasource_refs<'a>(
    settings: &'a SagaSettings,
    role: SagaRole,
    phase: ApplicationPhase,
) -> ApplicationResult<Vec<&'a str>> {
    let required = |value: Option<&'a str>, message: &'static str| {
        value
            .map(|value| vec![value])
            .ok_or_else(|| saga_error(phase, message))
    };
    match role {
        SagaRole::Orchestrator => required(
            settings.orchestrator_datasource_ref(),
            "saga.orchestrator.datasource_ref is required",
        ),
        SagaRole::Participant => {
            if settings.participant.bindings.is_empty() {
                required(
                    settings.participant_datasource_ref(),
                    "saga.participant.datasource_ref is required for a single transaction domain",
                )
            } else {
                if settings.participant.datasource_ref.is_some()
                    || settings.datasource_ref.is_some()
                {
                    return Err(saga_error(
                        phase,
                        "participant bindings cannot share a fallback datasource_ref",
                    ));
                }
                let mut references = Vec::with_capacity(settings.participant.bindings.len());
                for binding in settings.participant.bindings.values() {
                    references.push(binding.datasource_ref.as_deref().ok_or_else(|| {
                        saga_error(phase, "every participant binding requires datasource_ref")
                    })?);
                }
                Ok(references)
            }
        }
        SagaRole::Client if settings.client.reliable_start => required(
            settings.client.datasource_ref.as_deref(),
            "reliable Saga client requires saga.client.datasource_ref",
        ),
        SagaRole::Client => Ok(Vec::new()),
        SagaRole::Combined => {
            let mut references = required(
                settings.orchestrator_datasource_ref(),
                "combined role requires saga.orchestrator.datasource_ref",
            )?;
            if settings.participant.bindings.is_empty() {
                references.extend(required(
                    settings.participant_datasource_ref(),
                    "combined role requires saga.participant.datasource_ref",
                )?);
            } else {
                for binding in settings.participant.bindings.values() {
                    references.push(binding.datasource_ref.as_deref().ok_or_else(|| {
                        saga_error(phase, "every participant binding requires datasource_ref")
                    })?);
                }
            }
            Ok(references)
        }
    }
}

/// 业务作用：验证 managed 角色取得运行权威所需的身份、Catalog、transport 与 API 合同完整。
///
/// 参数说明：
/// - `settings`: 待形成自动计划的配置。
/// - `role`: 已解析的部署角色。
/// - `phase`: 错误归属阶段。
///
/// 返回：自动构造所需字段完整时成功；任何缺失都在资源创建前拒绝。
fn validate_managed_role_settings(
    settings: &SagaSettings,
    role: SagaRole,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    validate_registry_client_settings(settings, phase)?;
    let validate_identity = |value: Option<&str>, message: &'static str| {
        let value = value.ok_or_else(|| saga_error(phase, message))?;
        nasaga_runtime::ServiceIdentity::new(value)
            .map(|_| ())
            .map_err(|error| saga_source_error(phase, message, error))
    };
    if matches!(role, SagaRole::Orchestrator | SagaRole::Combined) {
        validate_identity(
            settings.service_identity.as_deref(),
            "managed orchestrator requires saga.service_identity",
        )?;
        let replica = settings.replica_identity.as_deref().ok_or_else(|| {
            saga_error(phase, "managed orchestrator requires saga.replica_identity")
        })?;
        validate_runtime_name(replica, "replica identity", phase)?;
        if !settings.api.http.enabled && !settings.api.grpc.enabled {
            return Err(saga_error(
                phase,
                "managed orchestrator requires explicit saga.api exposure",
            ));
        }
        validate_catalog_settings(settings, phase)?;
    }
    if matches!(role, SagaRole::Participant | SagaRole::Combined) {
        validate_identity(
            settings.participant.service_identity.as_deref(),
            "managed participant requires saga.participant.service_identity",
        )?;
        validate_identity(
            settings.participant.orchestrator_identity.as_deref(),
            "managed participant requires saga.participant.orchestrator_identity",
        )?;
        let consumer = settings
            .participant
            .consumer_identity
            .as_deref()
            .ok_or_else(|| {
                saga_error(
                    phase,
                    "managed participant requires saga.participant.consumer_identity",
                )
            })?;
        validate_runtime_name(consumer, "participant consumer identity", phase)?;
    }
    if matches!(role, SagaRole::Client) {
        validate_identity(
            settings.client.service_identity.as_deref(),
            "managed client requires saga.client.service_identity",
        )?;
        validate_identity(
            settings.client.orchestrator_identity.as_deref(),
            "managed client requires saga.client.orchestrator_identity",
        )?;
        if settings.client.orchestrator_discovery_ref.is_none()
            || settings.client.credential_ref.is_none()
        {
            return Err(saga_error(
                phase,
                "managed client requires discovery and credential references",
            ));
        }
    }
    if matches!(
        role,
        SagaRole::Orchestrator | SagaRole::Participant | SagaRole::Combined
    ) && settings
        .transport
        .command_result
        .as_ref()
        .and_then(|transport| transport.kind.as_ref())
        .is_none()
    {
        return Err(saga_error(
            phase,
            "managed Saga data roles require saga.transport.command_result.kind",
        ));
    }
    Ok(())
}

/// 业务作用：校验远程 capability/definition 控制面只有一套完整地址、凭据与可选签名身份。
///
/// 参数说明：`settings` 是完整 Saga 配置，`phase` 标记拒绝发生的生命周期阶段。
///
/// 返回：未配置或全部必填字段完整时成功；半配置协议或私钥引用时拒绝启动。
fn validate_registry_client_settings(
    settings: &SagaSettings,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let Some(client) = settings.definition_catalog.registry_client.as_ref() else {
        return Ok(());
    };
    if client.protocol.is_none()
        || client.discovery_ref.is_none()
        || client.credential_ref.is_none()
    {
        return Err(saga_error(
            phase,
            "definition registry_client requires protocol, discovery_ref and credential_ref",
        ));
    }
    if client.definition_signing_key_id.is_some() != client.definition_signing_key_ref.is_some() {
        return Err(saga_error(
            phase,
            "definition registry_client signing key id and secret reference must be configured together",
        ));
    }
    if let Some(key_id) = client.definition_signing_key_id.as_deref() {
        validate_runtime_name(key_id, "definition signing key id", phase)?;
    }
    Ok(())
}

/// 业务作用：把 command/result 协议选择与唯一专属配置块绑定，防止残留的第二协议成为暗中权威。
///
/// 参数说明：`settings` 是完整 Saga 配置，`role` 决定入站与出站必需字段，`phase` 用于错误归属。
///
/// 返回：协议配置单一且角色合同完整时成功；缺失、混配或无界预算时拒绝。
fn validate_managed_transport_settings(
    settings: &SagaSettings,
    role: SagaRole,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    if role == SagaRole::Client {
        if settings.transport.command_result.is_some() {
            return Err(saga_error(
                phase,
                "client role cannot own command_result transport",
            ));
        }
        return Ok(());
    }
    let transport = settings
        .transport
        .command_result
        .as_ref()
        .ok_or_else(|| saga_error(phase, "managed Saga data roles require command_result"))?;
    let kind = transport.kind.ok_or_else(|| {
        saga_error(
            phase,
            "managed Saga data roles require saga.transport.command_result.kind",
        )
    })?;
    let blocks = [
        transport.http.is_some(),
        transport.grpc.is_some(),
        transport.kafka.is_some(),
        transport.redis_stream.is_some(),
    ];
    if blocks.into_iter().filter(|present| *present).count() != 1 {
        return Err(saga_error(
            phase,
            "command_result must contain exactly one protocol configuration block",
        ));
    }
    let selected_present = match kind {
        SagaTransportKind::Http => transport.http.is_some(),
        SagaTransportKind::Grpc => transport.grpc.is_some(),
        SagaTransportKind::Kafka => transport.kafka.is_some(),
        SagaTransportKind::RedisStream => transport.redis_stream.is_some(),
    };
    if !selected_present {
        return Err(saga_error(
            phase,
            "command_result.kind does not match its protocol configuration block",
        ));
    }
    match kind {
        SagaTransportKind::Http => {
            let http = transport.http.as_ref().expect("selected block was checked");
            if http.shared_replay_claim.is_none()
                || http.command_credential_ref.is_none()
                || http.result_credential_ref.is_none()
            {
                return Err(saga_error(
                    phase,
                    "managed Saga HTTP requires shared replay claim and command/result credentials",
                ));
            }
            // 编排端必须先拥有可验签的结果主体，再开放受管 HTTP 数据面；空 Catalog 不能掩盖
            // 缺失的信任边，否则后续定义可以产生命令，却没有任何结果能被认证并推进状态。
            if matches!(role, SagaRole::Orchestrator | SagaRole::Combined)
                && http.producer_credentials.is_empty()
            {
                return Err(saga_error(
                    phase,
                    "managed Saga HTTP Orchestrator requires non-empty producer_credentials",
                ));
            }
            validate_routing_settings(&http.routing, role, phase)?;
            if matches!(role, SagaRole::Orchestrator | SagaRole::Combined)
                && http.routing.mode.as_deref() == Some("capability-registry")
            {
                resolve_managed_address_policy(
                    settings,
                    &http.routing,
                    SagaTransportKind::Http,
                    phase,
                )?;
            }
            if matches!(role, SagaRole::Participant | SagaRole::Combined)
                && http.orchestrator_discovery_ref.is_none()
            {
                return Err(saga_error(
                    phase,
                    "participant HTTP transport requires orchestrator_discovery_ref",
                ));
            }
            validate_optional_transport_budget(
                http.request_timeout_ms,
                "HTTP request timeout",
                phase,
            )?;
            if http.body_limit_bytes == Some(0) || http.concurrency_limit == Some(0) {
                return Err(saga_error(
                    phase,
                    "managed Saga HTTP body and concurrency limits must be positive",
                ));
            }
        }
        SagaTransportKind::Grpc => {
            let grpc = transport.grpc.as_ref().expect("selected block was checked");
            if grpc.credential_ref.is_none() {
                return Err(saga_error(
                    phase,
                    "managed Saga gRPC requires credential_ref",
                ));
            }
            validate_routing_settings(&grpc.routing, role, phase)?;
            if matches!(role, SagaRole::Orchestrator | SagaRole::Combined)
                && grpc.routing.mode.as_deref() == Some("capability-registry")
            {
                resolve_managed_address_policy(
                    settings,
                    &grpc.routing,
                    SagaTransportKind::Grpc,
                    phase,
                )?;
            }
            if matches!(role, SagaRole::Participant | SagaRole::Combined)
                && grpc.orchestrator_discovery_ref.is_none()
            {
                return Err(saga_error(
                    phase,
                    "participant gRPC transport requires orchestrator_discovery_ref",
                ));
            }
            validate_optional_transport_budget(
                grpc.request_timeout_ms,
                "gRPC request timeout",
                phase,
            )?;
        }
        SagaTransportKind::Kafka => {
            let kafka = transport
                .kafka
                .as_ref()
                .expect("selected block was checked");
            if kafka.client_ref.is_none()
                || kafka.command_topic.is_none()
                || kafka.result_topic.is_none()
                || kafka.result_group.is_none()
                || kafka.result_dlt_topic.is_none()
            {
                return Err(saga_error(
                    phase,
                    "managed Saga Kafka requires client, topics, group and DLT references",
                ));
            }
            validate_routing_settings(&kafka.routing, role, phase)?;
            if matches!(role, SagaRole::Orchestrator | SagaRole::Combined)
                && kafka.routing.mode.as_deref() == Some("capability-registry")
            {
                resolve_managed_address_policy(
                    settings,
                    &kafka.routing,
                    SagaTransportKind::Kafka,
                    phase,
                )?;
            }
        }
        SagaTransportKind::RedisStream => {
            let stream = transport
                .redis_stream
                .as_ref()
                .expect("selected block was checked");
            if stream.client_ref.is_none()
                || stream.key_tag.is_none()
                || stream.credential_ref.is_none()
            {
                return Err(saga_error(
                    phase,
                    "managed Saga Redis Streams requires client, key_tag and credential references",
                ));
            }
            validate_routing_settings(&stream.routing, role, phase)?;
            if matches!(role, SagaRole::Orchestrator | SagaRole::Combined)
                && stream.routing.mode.as_deref() == Some("capability-registry")
            {
                resolve_managed_address_policy(
                    settings,
                    &stream.routing,
                    SagaTransportKind::RedisStream,
                    phase,
                )?;
            }
            if matches!(role, SagaRole::Orchestrator | SagaRole::Combined)
                && (stream.result_stream.is_none()
                    || stream.result_group.is_none()
                    || stream.result_consumer.is_none()
                    || stream.result_dlt_stream.is_none())
            {
                return Err(saga_error(
                    phase,
                    "orchestrator Redis Streams transport requires complete result consumer fields",
                ));
            }
            if matches!(role, SagaRole::Participant | SagaRole::Combined)
                && (stream.command_stream.is_none()
                    || stream.command_group.is_none()
                    || stream.command_consumer.is_none()
                    || stream.command_dlt_stream.is_none()
                    || stream.result_stream.is_none())
            {
                return Err(saga_error(
                    phase,
                    "participant Redis Streams transport requires complete command consumer and result publisher fields",
                ));
            }
        }
    }
    Ok(())
}

/// 业务作用：校验 owner route 只使用 capability registry 或显式静态开发路由中的一种权威。
///
/// 参数说明：`routing` 是待冻结的 route 段，`role` 决定是否需要 owner 地址集，`phase` 用于错误归属。
///
/// 返回：模式封闭且必需政策完整时成功；双重或空路由权威时拒绝。
fn validate_routing_settings(
    routing: &SagaRoutingSettings,
    role: SagaRole,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    match routing.mode.as_deref() {
        Some("capability-registry") => {
            if !routing.static_routes.is_empty() {
                return Err(saga_error(
                    phase,
                    "capability-registry routing cannot contain static_routes",
                ));
            }
            if matches!(role, SagaRole::Orchestrator | SagaRole::Combined)
                && routing.address_policy_ref.is_none()
            {
                return Err(saga_error(
                    phase,
                    "capability-registry owner routing requires address_policy_ref",
                ));
            }
            Ok(())
        }
        Some("static") => {
            if routing.static_routes.is_empty() || routing.address_policy_ref.is_some() {
                Err(saga_error(
                    phase,
                    "static routing requires only a non-empty static_routes map",
                ))
            } else {
                Ok(())
            }
        }
        _ => Err(saga_error(
            phase,
            "Saga routing.mode must be capability-registry or static",
        )),
    }
}

/// 业务作用：解析 capability-registry route 引用的唯一受信地址政策。
///
/// 参数说明：`settings` 保存受信政策表，`routing` 保存稳定引用，`kind` 选择
/// 必须非空的协议边界，`phase` 标记拒绝阶段。
///
/// 返回：引用存在且协议边界完整时返回冻结政策；缺失或宽泛配置拒绝启动。
fn resolve_managed_address_policy<'a>(
    settings: &'a SagaSettings,
    routing: &SagaRoutingSettings,
    kind: SagaTransportKind,
    phase: ApplicationPhase,
) -> ApplicationResult<&'a SagaAddressPolicySettings> {
    let reference = routing.address_policy_ref.as_deref().ok_or_else(|| {
        saga_error(
            phase,
            "capability-registry owner routing requires address_policy_ref",
        )
    })?;
    let policy = settings
        .transport
        .address_policies
        .get(reference)
        .ok_or_else(|| saga_error(phase, "Saga routing address policy reference is unknown"))?;
    validate_managed_address_policy(policy, kind, phase)?;
    Ok(policy)
}

/// 业务作用：确认地址政策只开放当前 transport 所需的明确协议与命名空间。
///
/// 参数说明：`policy` 是配置候选，`kind` 是当前唯一数据面，`phase` 标记拒绝阶段。
///
/// 返回：所选协议具有非空且规范的 allowlist 时成功；空边界、通配或非法名称拒绝。
fn validate_managed_address_policy(
    policy: &SagaAddressPolicySettings,
    kind: SagaTransportKind,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let valid_prefix = |value: &str, max_len: usize| {
        !value.is_empty()
            && value.len() <= max_len
            && value.trim() == value
            && !value.contains('*')
            && !value.chars().any(char::is_control)
    };
    let valid_host = |host: &str| {
        if host.is_empty()
            || host.len() > 253
            || host != host.to_ascii_lowercase()
            || host.contains('*')
        {
            return false;
        }
        if host.parse::<std::net::IpAddr>().is_ok() {
            return true;
        }
        host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
    };
    let valid_ports = |ports: &[u16]| ports.iter().all(|port| *port > 0);
    let valid = match kind {
        SagaTransportKind::Http => {
            !policy.http_schemes.is_empty()
                && policy
                    .http_schemes
                    .iter()
                    .all(|scheme| matches!(scheme.as_str(), "http" | "https"))
                && !policy.http_hosts.is_empty()
                && policy.http_hosts.iter().all(|host| valid_host(host))
                && valid_ports(&policy.http_ports)
        }
        SagaTransportKind::Grpc => {
            !policy.grpc_hosts.is_empty()
                && policy.grpc_hosts.iter().all(|host| valid_host(host))
                && valid_ports(&policy.grpc_ports)
        }
        SagaTransportKind::Kafka => {
            !policy.kafka_topic_prefixes.is_empty()
                && policy
                    .kafka_topic_prefixes
                    .iter()
                    .all(|prefix| valid_prefix(prefix, 249))
        }
        SagaTransportKind::RedisStream => {
            !policy.redis_stream_prefixes.is_empty()
                && policy
                    .redis_stream_prefixes
                    .iter()
                    .all(|prefix| valid_prefix(prefix, 190))
        }
    };
    if valid {
        Ok(())
    } else {
        Err(saga_error(
            phase,
            "Saga routing address policy is empty or invalid for the selected transport",
        ))
    }
}

/// 业务作用：在 capability 进入激活或路由快照前复验其目标仍位于受信范围。
///
/// 参数说明：`policy` 是 Ready 时冻结的 allowlist，`descriptor` 是目录内不可信路由，
/// `phase` 标记首次构造或运行期切代。
///
/// 返回：协议、主机、可选端口或命名空间全部命中时成功；任一越界目标拒绝。
fn validate_capability_address(
    policy: &SagaAddressPolicySettings,
    descriptor: &nasaga_runtime::CapabilityDescriptor,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let namespace_matches = |value: &str, prefix: &str, separator: char| {
        value == prefix
            || value
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with(separator))
    };
    let allowed = match descriptor.transport.as_str() {
        "http" | "grpc" => {
            let url = reqwest::Url::parse(&descriptor.endpoint).map_err(|error| {
                saga_source_error(phase, "Saga capability endpoint is invalid", error)
            })?;
            let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
            let port = url.port_or_known_default();
            let canonical_origin = !host.is_empty()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && matches!(url.path(), "" | "/");
            if descriptor.transport == "http" {
                canonical_origin
                    && policy.http_schemes.iter().any(|item| item == url.scheme())
                    && policy.http_hosts.iter().any(|item| item == &host)
                    && (policy.http_ports.is_empty()
                        || port.is_some_and(|port| policy.http_ports.contains(&port)))
            } else {
                canonical_origin
                    && url.scheme() == "https"
                    && policy.grpc_hosts.iter().any(|item| item == &host)
                    && (policy.grpc_ports.is_empty()
                        || port.is_some_and(|port| policy.grpc_ports.contains(&port)))
            }
        }
        "kafka" => policy
            .kafka_topic_prefixes
            .iter()
            .any(|prefix| namespace_matches(&descriptor.endpoint, prefix, '.')),
        "redis-stream" => policy
            .redis_stream_prefixes
            .iter()
            .any(|prefix| namespace_matches(&descriptor.endpoint, prefix, ':')),
        _ => false,
    };
    if allowed {
        Ok(())
    } else {
        Err(saga_error(
            phase,
            "Saga capability endpoint is outside the configured address policy",
        ))
    }
}

/// 业务作用：校验可选 transport 往返预算始终为正且有界。
///
/// 参数说明：`value` 是毫秒预算，`kind` 是稳定字段类别，`phase` 用于错误归属。
///
/// 返回：未覆盖或位于十毫秒到一分钟时成功；其它值拒绝。
fn validate_optional_transport_budget(
    value: Option<u64>,
    kind: &'static str,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    if value.is_none_or(|value| (10..=MAX_TIMER_INTERVAL_MS).contains(&value)) {
        Ok(())
    } else {
        Err(saga_error(
            phase,
            format!("Saga {kind} is outside the supported range"),
        ))
    }
}

/// 业务作用：在服务注册前完整校验 gRPC API 的 mTLS 主体与最小权限闭包。
///
/// 参数说明：`api` 是 gRPC API 暴露与 caller 配置，`phase` 用于错误归属。
///
/// 返回：启用面具备合法 principal、身份、租户和所需高权限 grant 时成功；闲置或不完整配置拒绝。
fn validate_managed_grpc_api_settings(
    api: &SagaGrpcApiSettings,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    if !api.enabled {
        if api.expose_admin || api.expose_definition_registry || !api.callers.is_empty() {
            return Err(saga_error(
                phase,
                "Saga gRPC API exposure is configured while the API is disabled",
            ));
        }
        return Ok(());
    }
    if !cfg!(any(feature = "saga-grpc", feature = "saga-grpc-pgsql")) {
        return Err(saga_error(
            phase,
            "managed Saga gRPC API requires a Saga gRPC feature",
        ));
    }
    if api.callers.is_empty() {
        return Err(saga_error(
            phase,
            "managed Saga gRPC API requires at least one authenticated caller",
        ));
    }
    let allowed_permissions = ["start", "read", "audit", "admin", "registry"];
    for (principal, caller) in &api.callers {
        validate_managed_grpc_principal(principal, phase)?;
        let identity = caller
            .service_identity
            .as_deref()
            .ok_or_else(|| saga_error(phase, "managed Saga gRPC API caller identity is missing"))?;
        nasaga_runtime::ServiceIdentity::new(identity).map_err(|error| {
            saga_source_error(phase, "Saga gRPC API caller identity is invalid", error)
        })?;
        if caller.tenants.is_empty()
            || caller.permissions.is_empty()
            || caller
                .permissions
                .iter()
                .any(|permission| !allowed_permissions.contains(&permission.as_str()))
        {
            return Err(saga_error(
                phase,
                "every Saga gRPC API caller requires tenant and known permission grants",
            ));
        }
        if caller.tenants.iter().any(|tenant| {
            tenant != "*" && nasaga_runtime::__private::core::TenantId::new(tenant).is_err()
        }) {
            return Err(saga_error(
                phase,
                "Saga gRPC API caller contains an invalid tenant grant",
            ));
        }
        let has_registry = caller.permissions.iter().any(|value| value == "registry");
        if has_registry == caller.workflows.is_empty()
            || caller.workflows.iter().any(|workflow| {
                workflow != "*"
                    && nasaga_runtime::__private::core::WorkflowName::new(workflow).is_err()
            })
        {
            return Err(saga_error(
                phase,
                "Saga gRPC registry callers require only valid workflow grants",
            ));
        }
    }
    if api.expose_admin
        && !api
            .callers
            .values()
            .any(|caller| caller.permissions.iter().any(|value| value == "admin"))
    {
        return Err(saga_error(
            phase,
            "Saga gRPC admin exposure requires an admin caller grant",
        ));
    }
    if api.expose_definition_registry
        && !api
            .callers
            .values()
            .any(|caller| caller.permissions.iter().any(|value| value == "registry"))
    {
        return Err(saga_error(
            phase,
            "Saga gRPC definition registry exposure requires a registry caller grant",
        ));
    }
    Ok(())
}

/// 业务作用：校验标准 API 只由 Orchestrator 角色暴露，且每个已启用协议都有独立授权政策。
///
/// 参数说明：`settings` 含 API 与 HTTP 保留路径，`role` 是当前部署角色，`phase` 用于错误归属。
///
/// 返回：暴露面与角色、授权和路径权威一致时成功；越权或闲置路径配置时拒绝。
fn validate_managed_api_settings(
    settings: &SagaSettings,
    role: SagaRole,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let orchestrator_role = matches!(role, SagaRole::Orchestrator | SagaRole::Combined);
    if !orchestrator_role && (settings.api.http.enabled || settings.api.grpc.enabled) {
        return Err(saga_error(
            phase,
            "participant and client roles cannot expose Orchestrator APIs",
        ));
    }
    if (settings.api.http.enabled || settings.api.grpc.enabled)
        && settings.api.page_token_key_ref.is_none()
    {
        return Err(saga_error(
            phase,
            "managed Saga API requires saga.api.page_token_key_ref",
        ));
    }
    for (enabled, policy, kind) in [
        (
            settings.api.http.enabled,
            settings.api.http.authorization_policy_ref.as_ref(),
            "HTTP",
        ),
        (
            settings.api.grpc.enabled,
            settings.api.grpc.authorization_policy_ref.as_ref(),
            "gRPC",
        ),
    ] {
        if enabled {
            let policy = policy.ok_or_else(|| {
                saga_error(
                    phase,
                    format!("managed Saga {kind} API requires authorization_policy_ref"),
                )
            })?;
            validate_runtime_name(policy, "API authorization policy reference", phase)?;
        } else if policy.is_some() {
            return Err(saga_error(
                phase,
                format!("Saga {kind} API policy is configured while the API is disabled"),
            ));
        }
    }
    if settings.api.http.enabled {
        if settings.api.http.callers.is_empty() {
            return Err(saga_error(
                phase,
                "managed Saga HTTP API requires at least one authenticated caller",
            ));
        }
        let allowed_permissions = ["start", "read", "audit", "admin", "registry"];
        for (identity, caller) in &settings.api.http.callers {
            nasaga_runtime::ServiceIdentity::new(identity).map_err(|error| {
                saga_source_error(phase, "Saga HTTP API caller identity is invalid", error)
            })?;
            if caller.credential_ref.is_none()
                || caller.tenants.is_empty()
                || caller.permissions.is_empty()
                || caller
                    .permissions
                    .iter()
                    .any(|permission| !allowed_permissions.contains(&permission.as_str()))
            {
                return Err(saga_error(
                    phase,
                    "every Saga HTTP API caller requires credential, tenant and known permission grants",
                ));
            }
            if caller.tenants.iter().any(|tenant| {
                tenant != "*" && nasaga_runtime::__private::core::TenantId::new(tenant).is_err()
            }) {
                return Err(saga_error(
                    phase,
                    "Saga HTTP API caller contains an invalid tenant grant",
                ));
            }
            let has_registry = caller.permissions.iter().any(|value| value == "registry");
            if has_registry == caller.workflows.is_empty()
                || caller.workflows.iter().any(|workflow| {
                    workflow != "*"
                        && nasaga_runtime::__private::core::WorkflowName::new(workflow).is_err()
                })
            {
                return Err(saga_error(
                    phase,
                    "Saga HTTP registry callers require only valid workflow grants",
                ));
            }
        }
        if settings.api.http.expose_admin
            && !settings
                .api
                .http
                .callers
                .values()
                .any(|caller| caller.permissions.iter().any(|value| value == "admin"))
        {
            return Err(saga_error(
                phase,
                "Saga HTTP admin exposure requires an admin caller grant",
            ));
        }
        if settings.api.http.expose_definition_registry
            && !settings
                .api
                .http
                .callers
                .values()
                .any(|caller| caller.permissions.iter().any(|value| value == "registry"))
        {
            return Err(saga_error(
                phase,
                "Saga HTTP definition registry exposure requires a registry caller grant",
            ));
        }
    } else if settings.api.http.expose_admin
        || settings.api.http.expose_definition_registry
        || !settings.api.http.callers.is_empty()
    {
        return Err(saga_error(
            phase,
            "Saga HTTP API exposure is configured while the API is disabled",
        ));
    }
    validate_managed_grpc_api_settings(&settings.api.grpc, phase)?;
    let http_transport = settings
        .transport
        .command_result
        .as_ref()
        .and_then(|transport| transport.kind)
        == Some(SagaTransportKind::Http);
    if settings.http.base_path.is_some() && !settings.api.http.enabled && !http_transport {
        return Err(saga_error(
            phase,
            "saga.http.base_path is configured without an HTTP Saga ingress",
        ));
    }
    Ok(())
}

/// 业务作用：校验动态 Catalog 不依赖匿名发布、未命名 capability 或无界 watch。
///
/// 参数说明：`settings` 是完整 Saga 配置，`phase` 是错误归属阶段。
///
/// 返回：静态模式直接成功；动态模式所需引用与预算完整时成功。
fn validate_catalog_settings(
    settings: &SagaSettings,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let catalog = &settings.definition_catalog;
    validate_registry_client_settings(settings, phase)?;
    let dynamic_catalog = catalog.mode == DefinitionCatalogMode::Dynamic;
    let dynamic_routing = managed_uses_capability_registry(settings);
    if dynamic_catalog != dynamic_routing {
        return Err(saga_error(
            phase,
            "managed Saga Orchestrator requires static Catalog with static routing or dynamic Catalog with capability-registry routing",
        ));
    }
    if managed_uses_capability_registry(settings) && managed_registry_protocol(settings).is_none() {
        return Err(saga_error(
            phase,
            "capability-registry routing requires an HTTP or gRPC registry client",
        ));
    }
    // 仅持有参与方能力的角色发布租约；纯协调者从 Registry 接收能力，不枚举业务租户。
    if matches!(
        settings.role,
        Some(SagaRole::Participant | SagaRole::Combined)
    ) && managed_uses_capability_registry(settings)
        && managed_registry_protocol(settings) == Some(SagaClientProtocol::Grpc)
        && catalog.publish_tenants.is_empty()
    {
        return Err(saga_error(
            phase,
            "gRPC capability publication requires definition_catalog.publish_tenants",
        ));
    }
    if settings.definition_catalog.mode == DefinitionCatalogMode::Static {
        return Ok(());
    }
    if catalog.datasource_ref.is_none()
        || catalog.activation_policy.is_none()
        || catalog.capability_registry_ref.is_none()
        || catalog.publisher_authorization_policy_ref.is_none()
    {
        return Err(saga_error(
            phase,
            "dynamic definition catalog requires datasource, activation, capability and publisher policy references",
        ));
    }
    let watch = catalog.watch_interval_ms.ok_or_else(|| {
        saga_error(
            phase,
            "dynamic definition catalog requires watch_interval_ms",
        )
    })?;
    if !(10..=MAX_TIMER_INTERVAL_MS).contains(&watch) {
        return Err(saga_error(
            phase,
            "definition catalog watch interval is outside the supported range",
        ));
    }
    match catalog.activation_policy.as_deref() {
        Some("validated" | "approved") => Ok(()),
        _ => Err(saga_error(
            phase,
            "definition catalog activation_policy must be validated or approved",
        )),
    }
}

/// 业务作用：验证 Saga HTTP 子路径已经是唯一、无歧义的规范形式。
///
/// 参数说明：`path` 是 context 内相对保留路径，`phase` 是错误归属阶段。
///
/// 返回：路径可安全逐段拼接时成功；根路径、尾斜杠、编码或特殊段拒绝。
fn validate_saga_base_path(path: &str, phase: ApplicationPhase) -> ApplicationResult<()> {
    let invalid = path == "/"
        || !path.starts_with('/')
        || path.ends_with('/')
        || !path.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.' | b'~')
        })
        || path.contains("//")
        || path.contains('%')
        || path.contains('?')
        || path.contains('#')
        || path.contains('*')
        || path.contains('{')
        || path.contains('}')
        || path.split('/').any(|segment| matches!(segment, "." | ".."));
    if invalid {
        Err(saga_error(
            phase,
            "saga.http.base_path is not a canonical context-relative path",
        ))
    } else {
        Ok(())
    }
}

/// 业务作用：校验参与方名称与 timer owner 的稳定低基数身份合同。
///
/// 参数说明：
/// - `value`：未经信任的候选名称。
/// - `kind`：固定字段类别，只用于脱敏错误摘要。
/// - `phase`：该名称实际被校验的应用生命周期阶段。
///
/// 返回：1 至 128 字节的小写 canonical 名称成功；其它输入归属到调用阶段。
fn validate_runtime_name(
    value: &str,
    kind: &'static str,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value.trim() == value
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'.')
        });
    if valid {
        Ok(())
    } else {
        Err(saga_error(
            phase,
            format!("saga {kind} must satisfy the canonical identity contract"),
        ))
    }
}

/// 业务作用：读取可用于持久化 Saga 裁决的当前 epoch 毫秒。
///
/// 参数说明: 无。
///
/// 返回：系统时间可表示为 `i64` 毫秒时成功；回拨到 epoch 前或溢出时返回关键任务错误。
fn epoch_millis() -> ApplicationResult<i64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Running,
                "saga timer clock is before the Unix epoch",
                error,
            )
        })?
        .as_millis();
    i64::try_from(millis).map_err(|error| {
        saga_source_error(
            ApplicationPhase::Running,
            "saga timer clock exceeds the supported range",
            error,
        )
    })
}

/// 业务作用：创建不带底层错误正文的 Saga 生命周期错误。
///
/// 参数说明：
/// - `phase`：失败所属生命周期阶段。
/// - `message`：不含业务输入的稳定摘要。
///
/// 返回：归因到 Saga 组件的统一错误。
fn saga_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Saga, phase, message)
}

/// 业务作用：创建保留诊断链、公开摘要保持脱敏的 Saga 生命周期错误。
///
/// 参数说明：
/// - `phase`：失败所属生命周期阶段。
/// - `message`：不含业务输入的稳定摘要。
/// - `source`：只进入统一诊断链的底层错误。
///
/// 返回：归因到 Saga 组件的统一错误。
fn saga_source_error(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Saga, phase, message, source)
}
