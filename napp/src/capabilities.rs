#[cfg(feature = "kafka")]
use std::collections::BTreeMap;
#[cfg(any(feature = "log", feature = "scheduling"))]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(any(
    feature = "log",
    feature = "kafka",
    feature = "nacos-config",
    feature = "ws",
    feature = "nacos-discovery",
    feature = "scheduling"
))]
use std::sync::Arc;
#[cfg(any(
    feature = "nacos-config",
    feature = "ws",
    feature = "nacos-discovery",
    feature = "scheduling-cluster"
))]
use std::sync::{OnceLock, Weak};

#[cfg(feature = "ws")]
use std::net::SocketAddr;

#[cfg(any(
    feature = "log",
    feature = "kafka",
    feature = "nacos-config",
    feature = "ws",
    feature = "nacos-discovery",
    feature = "scheduling"
))]
use crate::state::StateCell;
use crate::ApplicationState;
#[cfg(any(
    feature = "kafka",
    feature = "nacos-config",
    feature = "ws",
    feature = "nacos-discovery",
    feature = "scheduling"
))]
use crate::{ApplicationError, ApplicationPhase, ApplicationResult, ComponentId};

/// 组件能力相对于统一应用状态机的只读生命周期状态，不代替组件自己的资源关闭或动态健康证据。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentLifecycleState {
    /// 应用仍在启动，组件的底层对象可能尚未发布。
    Starting,
    /// 应用初始就绪动作已经完成；具体操作仍需检查自身动态健康与准入条件。
    Ready,
    /// 应用正在摘流并按逆序释放组件资源。
    Draining,
    /// 应用已进入 Stopped；需要证明底层资源退出的能力应另行复验关闭结果。
    Closed,
    /// 启动、运行或清理发生主故障且生命周期已经收敛。
    Failed,
}

impl ComponentLifecycleState {
    /// 业务作用：返回适合管理端和指标标签使用的稳定名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前枚举值的静态名称，不包含故障详情或业务输入，也不重新读取应用状态。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Draining => "draining",
            Self::Closed => "closed",
            Self::Failed => "failed",
        }
    }

    /// 业务作用：判断已取得的生命周期读数是否为应用就绪阶段。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只有 `Ready` 为真；不会探测底层连接，也不为后续操作预留准入。
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::Ready)
    }
}

impl From<ApplicationState> for ComponentLifecycleState {
    /// 业务作用：把应用状态机读数映射为组件能力生命周期状态。
    ///
    /// 参数说明：
    ///
    /// - `state`：从 Application 共享状态单元读取的当前状态。
    ///
    /// 返回：对应的组件生命周期枚举；Stopping 映射为 Draining，Stopped 映射为 Closed，
    /// 不额外核实资源是否已经释放，具体能力需要时应再检查自身关闭结果。
    fn from(state: ApplicationState) -> Self {
        match state {
            ApplicationState::Starting => Self::Starting,
            ApplicationState::Ready => Self::Ready,
            ApplicationState::Stopping => Self::Draining,
            ApplicationState::Stopped => Self::Closed,
            ApplicationState::Failed => Self::Failed,
        }
    }
}

/// 业务作用：创建能力尚未发布或已经关闭时使用的统一组件错误。
///
/// 参数说明：
///
/// - `component`：能力所属的组件身份。
/// - `state`：用于选择错误阶段的统一应用状态。
/// - `message`：不包含配置秘密或业务负载的稳定说明。
///
/// 返回：带组件身份和失败阶段的错误；Starting 归入 Ready 阶段，终态归入 Stopped 阶段。
/// 不改变应用状态，也不自动脱敏传入的说明。
#[cfg(any(
    feature = "kafka",
    feature = "nacos-config",
    feature = "ws",
    feature = "nacos-discovery",
    feature = "scheduling"
))]
fn unavailable_error(
    component: ComponentId,
    state: ApplicationState,
    message: impl Into<String>,
) -> ApplicationError {
    let phase = match state {
        ApplicationState::Starting => ApplicationPhase::Ready,
        ApplicationState::Ready => ApplicationPhase::Running,
        ApplicationState::Stopping => ApplicationPhase::Stopping,
        ApplicationState::Stopped | ApplicationState::Failed => ApplicationPhase::Stopped,
    };
    ApplicationError::new(component, phase, message)
}

/// Kafka client 最近一次由 Ready 门禁或运行期 monitor 发布的脱敏健康快照。
#[cfg(feature = "kafka")]
#[derive(Clone, Debug)]
pub struct KafkaReadinessSnapshot {
    /// 配置确定的稳定 client name；不包含 broker 地址或安全配置。
    pub client_name: String,
    /// 采样时 client 的全部 ReadyRule 是否同时满足，不是下一次操作的准入凭证。
    pub ready: bool,
    /// 按 resolved group id 稳定排序的健康快照；producer-only client 为空。
    pub groups: Vec<nafka::GroupHealth>,
}

/// Kafka 组件私有能力根；保存原始 proxy，但只通过 [`KafkaHandle`] 开放受控操作。
#[cfg(feature = "kafka")]
pub(crate) struct KafkaClientCapability {
    /// 配置确定且进程内唯一的 client name。
    client_name: Arc<str>,
    /// 仅组件和 shutdown action 可克隆的原始运行时句柄。
    proxy: nafka::KafkaProxy,
    /// 最终清理无论成功或耗尽预算都撤销已导出句柄的新准入。
    access_scope: nafka::KafkaAccessScope,
    /// 宿主按 client 显式授权的管理写能力；普通 admin 视图始终只读。
    admin_write: bool,
    /// 动态就绪聚合项；更新顺序为先写快照、后发布该原子结论。
    contributor: crate::readiness::ReadinessContributor,
    /// 供管理面读取的最近一次脱敏快照。
    readiness: std::sync::RwLock<KafkaReadinessSnapshot>,
    /// Start 注入 proxy、UserHook 安装真实 sink 的内部时序桥。
    metrics: Arc<crate::kafka::KafkaMetricsBridge>,
    #[cfg(feature = "ws-kafka")]
    /// 冻结的 WS 消费归属标记；允许协议运行时接管消费时不能同时开放普通 collected consumers。
    ws_consumer_owner: bool,
}

#[cfg(feature = "kafka")]
impl KafkaClientCapability {
    /// 业务作用：创建初始未就绪的 Kafka client 能力根。
    ///
    /// 参数说明：
    ///
    /// - `client_name`：通过 KafkaConfig 校验的稳定 client name。
    /// - `proxy`：Start 已完成本地构造、尚未启动 consumer 的原始运行时。
    /// - `contributor`：Application readiness registry 为本 client 分配的独占贡献句柄。
    /// - `metrics`：Start 时注入 proxy 的内部可替换指标桥。
    /// - `access_scope`：与最终清理 action 共享的单向业务准入域。
    /// - `admin_write`：是否允许业务显式取得管理写视图，仍需 broker ACL 授权。
    /// - `_ws_consumer_owner`：WS 子能力是否可独占本 client 的消费登记。
    ///
    /// 返回：初始 ready 为假且组列表为空的能力根；不探测 broker，也不启动 consumer。
    /// 调用方负责把该值放入共享所有权并登记生命周期清理动作。
    pub(crate) fn new(
        client_name: Arc<str>,
        proxy: nafka::KafkaProxy,
        contributor: crate::readiness::ReadinessContributor,
        metrics: Arc<crate::kafka::KafkaMetricsBridge>,
        access_scope: nafka::KafkaAccessScope,
        admin_write: bool,
        _ws_consumer_owner: bool,
    ) -> Self {
        Self {
            readiness: std::sync::RwLock::new(KafkaReadinessSnapshot {
                client_name: client_name.to_string(),
                ready: false,
                groups: Vec::new(),
            }),
            client_name,
            proxy,
            access_scope,
            admin_write,
            contributor,
            metrics,
            #[cfg(feature = "ws-kafka")]
            ws_consumer_owner: _ws_consumer_owner,
        }
    }

    /// 业务作用：返回稳定 client name。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：借用与 capability 共同存活，不创建新字符串。
    pub(crate) fn client_name(&self) -> &str {
        &self.client_name
    }

    /// 业务作用：先替换 client 的健康快照，再更新应用的动态就绪贡献。
    ///
    /// 参数说明：
    ///
    /// - `ready`：全部受管 group 当前是否满足各自 ReadyRule。
    /// - `groups`：按 resolved group id 稳定排序的最新健康快照。
    ///
    /// 返回：组列表按名称排序后在写锁内替换，锁中毒时仍取回状态继续写入；随后发布 contributor 结论。
    /// 两个对象不是一次原子更新，独立读取快照和聚合就绪时可能跨越两次发布。
    pub(crate) fn publish_readiness(&self, ready: bool, mut groups: Vec<nafka::GroupHealth>) {
        groups.sort_by(|left, right| left.group.cmp(&right.group));
        *self
            .readiness
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = KafkaReadinessSnapshot {
            client_name: self.client_name.to_string(),
            ready,
            groups,
        };
        // 先保存对应的健康证据，再改变对外就绪贡献，避免先开放就绪却只留下上一轮快照。
        self.contributor.set_ready(ready);
    }

    /// 业务作用：返回最近一次完整发布的脱敏就绪快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：读锁内克隆的最近快照；锁中毒时仍读取保留内容，不重新探测 broker，
    /// 与随后读取的 contributor 结论不构成跨对象原子快照。
    pub(crate) fn readiness(&self) -> KafkaReadinessSnapshot {
        self.readiness
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// 业务作用：尝试在 UserHook 为本 client 安装真实指标 sink。
    ///
    /// 参数说明：
    ///
    /// - `sink`：业务提供的无阻塞共享指标出口。
    ///
    /// 返回：首次安装且 bridge 尚未封口时返回 true；重复安装或 Ready 已封口返回 false。
    pub(crate) fn install_metrics(&self, sink: Arc<dyn nafka::MetricsSink>) -> bool {
        self.metrics.install(sink)
    }

    /// 业务作用：在 Ready 取走全部 UserHook 定制后封口指标安装入口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本方法无返回值；封口后既有 sink 保持有效，后续安装稳定失败。
    pub(crate) fn seal_metrics(&self) {
        // Ready 前收束指标定制，防止运行期间替换出口改变已发布的观测含义。
        self.metrics.seal();
    }
}

/// Kafka 组件对业务开放的受控发布、健康和运行控制句柄。
///
/// 句柄不公开原始 `KafkaProxy`、consumer registry、连接入口或 shutdown；producer、admin 与消费
/// 控制能力都派生自容器已创建的同一 client，即使业务长期持有句柄，容器 action 仍能独立完成两段停机。
#[cfg(feature = "kafka")]
#[derive(Clone)]
pub struct KafkaHandle {
    /// Kafka 组件私有能力根。
    capability: Arc<KafkaClientCapability>,
    /// 与 Application 同源的生命周期状态，用于阻止 Draining 后创建新操作。
    application_state: Arc<StateCell>,
}

#[cfg(feature = "kafka")]
impl KafkaHandle {
    /// 业务作用：把关闭权移交给同宿主的 WS 协议 owner，拒绝争用普通消费注册表。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：client 显式关闭 collected consumers 时提供内部 proxy；其它配置拒绝。
    #[cfg(feature = "ws-kafka")]
    pub(crate) fn ws_runtime_proxy(&self) -> ApplicationResult<nafka::KafkaProxy> {
        self.ensure_operation_open("ws cluster assembly", true)?;
        // 原始 proxy 带有运行控制能力，只有冻结配置明确交给 WS 的来源才能绕过普通消费登记入口。
        if !self.capability.ws_consumer_owner {
            return Err(ApplicationError::new(
                ComponentId::Kafka,
                ApplicationPhase::Ready,
                "WS Kafka requires container.consumers=disabled on its named client",
            ));
        }
        Ok(self.capability.proxy.clone())
    }
    /// 业务作用：从组件私有能力根创建业务句柄。
    ///
    /// 参数说明：
    ///
    /// - `capability`：Start 已发布的目标 client 能力根。
    /// - `application_state`：与所属 Application 同源的状态单元。
    ///
    /// 返回：不拥有任何 shutdown action 的轻量共享句柄。
    pub(crate) fn new(
        capability: Arc<KafkaClientCapability>,
        application_state: Arc<StateCell>,
    ) -> Self {
        Self {
            capability,
            application_state,
        }
    }

    /// 业务作用：返回本句柄绑定的稳定 client name。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：借用与句柄共同存活，不包含 broker 或安全配置。
    pub fn client_name(&self) -> &str {
        self.capability.client_name()
    }

    /// 业务作用：在受管应用中分别观察Kafka构建支持、配置启用和真实组就绪，不授予管理写权限。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不含凭据的只读快照；停止后仍可诊断，空组表不表示Ready。
    pub fn capabilities(&self) -> ApplicationResult<nafka::KafkaCapabilities> {
        self.capability
            .proxy
            .capabilities()
            .map_err(|error| self.operation_error("capabilities query", error))
    }

    /// 业务作用：向同一组件内的受管协议适配器暴露 Kafka 已冻结的 DLT 后缀。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：消费端实际按 `<source-topic><suffix>` 持久死信时使用的后缀。
    #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
    pub(crate) fn dead_letter_topic_suffix(&self) -> &str {
        &self
            .capability
            .proxy
            .config()
            .behavior
            .dead_letter_topic_suffix
    }

    /// 业务作用：读取 Kafka 消费端是否把 DLT 持久成功作为前移强制条件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：死信不可达时保留源 offset 则返回真；可用性优先跳过时返回假。
    #[cfg(any(feature = "saga-kafka", feature = "saga-kafka-pgsql"))]
    pub(crate) fn dead_letter_required(&self) -> bool {
        self.capability.proxy.config().behavior.dead_letter_required
    }

    /// 业务作用：返回 Kafka 能力相对于统一 Application 的当前生命周期。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Starting/Ready/Draining/Closed/Failed 之一；宿主已停但资源尚未退出时为 Failed，不能据宿主退出声称 Closed。
    pub fn lifecycle(&self) -> ComponentLifecycleState {
        let state = ComponentLifecycleState::from(self.application_state.load());
        // Application 的首次退出原因与资源清理结果独立；未完成的 Kafka 责任不能伪装成关闭成功。
        if state == ComponentLifecycleState::Closed && !self.capability.proxy.is_closed() {
            ComponentLifecycleState::Failed
        } else {
            state
        }
    }

    /// 业务作用：在宿主已结束最终清理后继续等待未退出的 Kafka 责任，不能用于停止活跃源。
    ///
    /// 参数说明：
    /// - `deadline`：本次等待和资源收尾共用的绝对截止时刻。
    ///
    /// 返回：实际资源全部退出后成功；宿主尚未交还最终清理权时拒绝，超时保留责任且不重开业务准入。
    pub async fn await_closed(&self, deadline: std::time::Instant) -> ApplicationResult<()> {
        // 只接续已经授权且结束的宿主清理；业务持有句柄不能提前关闭仍在运行的整个源。
        if !matches!(
            self.application_state.load(),
            ApplicationState::Stopped | ApplicationState::Failed
        ) || !self.capability.access_scope.is_closed()
        {
            return Err(self.operation_error(
                "await resource closure",
                nafka::NafkaError::Lifecycle("Application 最终清理尚未结束".into()),
            ));
        }
        if self.capability.proxy.is_closed() {
            return Ok(());
        }
        self.capability
            .proxy
            .shutdown_until(deadline)
            .await
            .map_err(|error| self.operation_error("await resource closure", error))
    }

    /// 业务作用：返回最近一次 Ready 门禁或运行期 monitor 发布的健康快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：拥有型脱敏副本；producer-only client 的 groups 为空，退出阶段强制撤销 ready，不把历史采样当作流量许可。
    pub fn readiness(&self) -> KafkaReadinessSnapshot {
        let mut snapshot = self.capability.readiness();
        // monitor 或 consumer 清理 action 可能已停止采样；宿主退出事实优先于最后一次健康结论。
        if matches!(
            self.application_state.load(),
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed
        ) {
            snapshot.ready = false;
        }
        snapshot
    }

    /// 业务作用：获取一个无关闭权的 producer lane。
    ///
    /// 参数说明：
    ///
    /// - `name`：配置中冻结的 lane 名；默认 lane 使用 `default`。
    ///
    /// 返回：Starting 或 Ready 阶段返回共享 lane；克隆不会新建底层 producer。
    ///
    /// # 错误
    ///
    /// Application 已进入 Draining/终态或 lane 不存在时返回 Kafka 组件错误。
    pub fn producer_lane(&self, name: &str) -> ApplicationResult<nafka::ProducerLane> {
        self.ensure_operation_open("producer lane access", true)?;
        self.capability
            .proxy
            .producer_lane(name)
            .map(|lane| lane.with_access_scope(&self.capability.access_scope))
            .map_err(|error| self.operation_error("producer lane access", error))
    }

    /// 业务作用：获取受管 client 的只读管理视图，供 metadata、位点观测和 topic 合同核验。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Starting/Ready 时提供无管理写权、无关闭权的视图；克隆保持限制，已取得视图可用于 graceful shutdown 尾部查询，最终清理后永久拒绝。
    pub fn admin(&self) -> ApplicationResult<nafka::KafkaAdmin> {
        self.ensure_operation_open("admin access", true)?;
        Ok(self
            .capability
            .proxy
            .admin()
            .with_access_scope(&self.capability.access_scope)
            .read_only())
    }

    /// 业务作用：在宿主显式授权的 client 上取得管理写视图，不授予整源关闭权。
    ///
    /// 参数说明: 无。
    /// 权限来自冻结的 container.admin_write 配置。
    ///
    /// 返回：Starting/Ready 且显式授权时返回同源管理视图，未授权为 Authorization；实际操作仍受 broker ACL 和最终清理门约束。
    pub fn administration(&self) -> ApplicationResult<nafka::KafkaAdmin> {
        self.ensure_operation_open("administration access", true)?;
        // 业务不能通过普通 getter 将只读能力提升为写能力；配置授权与 broker ACL 必须同时成立。
        if !self.capability.admin_write {
            return Err(
                self.operation_error("administration access", nafka::NafkaError::Authorization)
            );
        }
        Ok(self
            .capability
            .proxy
            .admin()
            .with_access_scope(&self.capability.access_scope))
    }

    /// 业务作用：在容器来源上登记独立回放任务，关闭责任仍归 Application 所有。
    ///
    /// 参数说明：
    /// - `plan`：冻结合同。
    /// - `store`：持久后端。
    /// - `identity`：当前资源证明。
    /// - `processor`：同步成功合同。
    ///
    /// 返回：Starting/Ready 时允许登记；Draining 后拒绝新任务，已登记任务由两段停机真实排干。
    pub fn replay(
        &self,
        plan: nafka::ReplayPlan,
        store: Arc<dyn nafka::CheckpointStore>,
        identity: Arc<dyn nafka::ReplayIdentityProvider>,
        processor: Arc<dyn nafka::ReplayProcessor>,
    ) -> ApplicationResult<nafka::ReplayHandle> {
        self.ensure_operation_open("replay execution", true)?;
        self.capability
            .proxy
            .replay(plan, store, identity, processor)
            .map_err(|error| self.operation_error("replay execution", error))
    }

    /// 业务作用：在两个受管 Kafka 来源间登记有限转发，由双方停机共同排干实际交付。
    ///
    /// 参数说明：
    /// - `plan`：冻结来源范围、目标物理身份、分区映射和负载策略的转发计划。
    /// - `destination`：提供目标 producer 的受管 Kafka 句柄，需独立通过宿主准入检查。
    /// - `store`：保存任务进度并提供独占 checkpoint 租约的持久后端。
    /// - `source_identity`：按来源集群身份提供 topic 当前代次，用于复验计划中的来源资源。
    /// - `destination_identity`：按目标集群身份提供 topic 当前代次，用于复验计划中的目标资源。
    /// - `registries`：使用 Confluent 负载策略时绑定的来源与目标 schema registry 客户端。
    ///
    /// 返回：双方 Starting/Ready 才接受新任务；进入 Draining 后拒绝，已登记责任继续收尾。
    pub fn transfer(
        &self,
        plan: nafka::TransferPlan,
        destination: &KafkaHandle,
        store: Arc<dyn nafka::CheckpointStore>,
        source_identity: Arc<dyn nafka::ReplayIdentityProvider>,
        destination_identity: Arc<dyn nafka::ReplayIdentityProvider>,
        registries: Option<nafka::TransferRegistries>,
    ) -> ApplicationResult<nafka::TransferHandle> {
        self.ensure_operation_open("transfer execution", true)?;
        destination.ensure_operation_open("transfer destination", true)?;
        self.capability
            .proxy
            .transfer(
                plan,
                &destination.capability.proxy,
                store,
                source_identity,
                destination_identity,
                registries,
            )
            .map_err(|error| self.operation_error("transfer execution", error))
    }

    /// 业务作用：在两个受管 Kafka 来源间登记有限DLT回投，由双方停机共同排干实际交付。
    ///
    /// 参数说明：
    /// - `plan`：固定来源过滤、回投与隔离合同。
    /// - `destination`：提供回投目标 producer 的受管句柄，需独立通过宿主准入检查。
    /// - `store`：持久保存 DLT 回投进度并提供任务租约的 checkpoint 后端。
    /// - `source_identity`：按 DLT 来源集群身份查询 topic 当前代次，防止恢复时沿用已重建来源。
    /// - `destination_identity`：按回投目标集群身份查询 topic 当前代次，复验计划指定的接收资源。
    /// - `registries`：Confluent 负载转换所需的来源、目标 schema registry 名称与客户端绑定。
    ///
    /// 返回：双方 Starting/Ready 才接受新任务；进入 Draining 后拒绝，已登记责任继续收尾。
    pub fn replay_dlt(
        &self,
        plan: nafka::DltReplayPlan,
        destination: &KafkaHandle,
        store: Arc<dyn nafka::CheckpointStore>,
        source_identity: Arc<dyn nafka::ReplayIdentityProvider>,
        destination_identity: Arc<dyn nafka::ReplayIdentityProvider>,
        registries: Option<nafka::TransferRegistries>,
    ) -> ApplicationResult<nafka::DltReplayHandle> {
        self.ensure_operation_open("DLT replay execution", true)?;
        destination.ensure_operation_open("DLT replay destination", true)?;
        self.capability
            .proxy
            .replay_dlt(
                plan,
                &destination.capability.proxy,
                store,
                source_identity,
                destination_identity,
                registries,
            )
            .map_err(|error| self.operation_error("DLT replay execution", error))
    }

    /// 业务作用：在受管来源启动专属同集群消费、输出与位点原子事务。
    ///
    /// 参数说明：
    /// - `plan`：稳定部署身份和完整白名单。
    /// - `processor`：同步有界整批处理。
    ///
    /// 返回：Starting/Ready 才接受新任务，事务回调和原生提交全部纳入两段停机。
    pub async fn transact(
        &self,
        plan: nafka::TransactionPlan,
        processor: Arc<dyn nafka::TransactionProcessor>,
    ) -> ApplicationResult<nafka::TransactionHandle> {
        self.ensure_operation_open("transaction execution", true)?;
        self.capability
            .proxy
            .transact(plan, processor)
            .await
            .map_err(|error| self.operation_error("transaction execution", error))
    }

    /// 业务作用：为单分区事务任务装配由组分配决定的主备启动，不让空分配备用实例触发 fencing。
    ///
    /// 参数说明：
    /// - `plan`：单输入 topic、稳定事务身份和完整输出白名单。
    /// - `processor`：同步有界批次入口。
    ///
    /// 返回：纳入两段停机的句柄；输入 topic 不是单分区、来源关闭或身份已占用时拒绝。
    pub async fn transact_on_assignment(
        &self,
        plan: nafka::TransactionPlan,
        processor: Arc<dyn nafka::TransactionProcessor>,
    ) -> ApplicationResult<nafka::TransactionHandle> {
        self.ensure_operation_open("assignment transaction execution", true)?;
        self.capability
            .proxy
            .transact_on_assignment(plan, processor)
            .await
            .map_err(|error| self.operation_error("assignment transaction execution", error))
    }

    /// 业务作用：查询一个 resolved group 的当前健康快照。
    ///
    /// 参数说明：
    ///
    /// - `group`：Ready 阶段解析出的最终 group id。
    ///
    /// 返回： nafka 自有的脱敏 GroupHealth 副本。
    ///
    /// # 错误
    ///
    /// Application 尚未 Ready、已开始停机或 group 不存在时返回错误。
    pub async fn group_health(&self, group: &str) -> ApplicationResult<nafka::GroupHealth> {
        self.ensure_operation_open("group health query", false)?;
        self.capability
            .proxy
            .group_health(group)
            .await
            .map_err(|error| self.operation_error("group health query", error))
    }

    /// 业务作用：在容器准入边界内解释当前组的接流量条件。
    ///
    /// 参数说明：
    /// - `group`：当前源组名。
    /// - `requirement`：分配要求。
    ///
    /// 返回：同次观测的要求、原因和健康；容器未 Ready、已停止、未知组或非法条件拒绝。
    pub async fn group_readiness(
        &self,
        group: &str,
        requirement: nafka::ReadyRequirement,
    ) -> ApplicationResult<nafka::ReadyStatus> {
        // 诊断入口不授予新运行权威，仍须遵守受管容器的生命周期。
        self.ensure_operation_open("group readiness query", false)?;
        self.capability
            .proxy
            .group_readiness(group, requirement)
            .await
            .map_err(|error| self.operation_error("group readiness query", error))
    }

    /// 业务作用：查询受管消费组各 owner 槽的独立健康，定位被组聚合状态覆盖的容量与归属差异。
    ///
    /// 参数说明：
    /// - `group`：Ready 阶段解析出的最终 group id。
    ///
    /// 返回：按稳定槽号排序的快照；容器未 Ready、停机或组不存在时拒绝。
    pub async fn group_slots_health(
        &self,
        group: &str,
    ) -> ApplicationResult<Vec<nafka::GroupHealth>> {
        // 受管观察仍受容器准入约束，不能由外部句柄越过未 Ready 或已 Draining 的边界。
        self.ensure_operation_open("group slots health query", false)?;
        self.capability
            .proxy
            .group_slots_health(group)
            .await
            .map_err(|error| self.operation_error("group slots health query", error))
    }

    /// 业务作用：观察受管 Kafka 指标出口的丢弃、失败和实际活动责任。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前源的进程内快照；容器未 Ready 或已进入 Draining 时拒绝，不绕过受管准入。
    pub fn metrics_export_stats(&self) -> ApplicationResult<nafka::MetricsExportStats> {
        // 观测句柄仍服从容器权威，不能借诊断入口重新开放关闭中的资源。
        self.ensure_operation_open("metrics export stats query", false)?;
        Ok(self.capability.proxy.metrics_export_stats())
    }

    /// 业务作用：开放停机期间及之后的只读责任诊断，不授予活跃宿主整源关闭权。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：源实际资源与未确认输入的独立快照；查询不重建原生资源，也不经过业务准入。
    pub fn close_report(&self) -> nafka::CloseReport {
        self.capability.proxy.close_report()
    }
    /// 业务作用：让宿主采样该源的剩余责任；已停止的业务句柄仍可取得关闭证据。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：关闭快照；异步指标可能丢弃，不授予停止或管理写权限。
    pub fn sample_metrics(&self) -> nafka::CloseReport {
        self.capability.proxy.sample_metrics()
    }

    /// 业务作用：查询一个 resolved group 的当前 assignment。
    ///
    /// 参数说明：
    ///
    /// - `group`：Ready 阶段解析出的最终 group id。
    ///
    /// 返回：排序语义由 nafka 控制面的分区列表副本。
    ///
    /// # 错误
    ///
    /// Application 不处于 Ready、group 不存在或控制命令失败时返回错误。
    pub async fn assignment(&self, group: &str) -> ApplicationResult<Vec<nafka::Tp>> {
        self.ensure_operation_open("group assignment query", false)?;
        self.capability
            .proxy
            .assignment(group)
            .await
            .map_err(|error| self.operation_error("group assignment query", error))
    }

    /// 业务作用：查询指定分区的当前消费位置。
    ///
    /// 参数说明：
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    /// - `partitions`：待查询的 topic-partition 集合；所有权交给控制命令。
    ///
    /// 返回：每个分区到可选下一消费 offset 的有序映射。
    ///
    /// # 错误
    ///
    /// Application 不处于 Ready、group 不存在或底层位置查询失败时返回错误。
    pub async fn position(
        &self,
        group: &str,
        partitions: Vec<nafka::Tp>,
    ) -> ApplicationResult<BTreeMap<nafka::Tp, Option<i64>>> {
        self.ensure_operation_open("group position query", false)?;
        self.capability
            .proxy
            .position(group, partitions)
            .await
            .map_err(|error| self.operation_error("group position query", error))
    }

    /// 业务作用：把一个分区定位到显式非负 offset。
    ///
    /// 参数说明：
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    /// - `partition`：需要重新定位的 topic-partition。
    /// - `offset`：新的非负消费位置。
    ///
    /// 返回：owner 确认 seek 已完成时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// Application 不处于 Ready、offset 非法或 owner 无法确认命令结果时返回错误。
    pub async fn seek(
        &self,
        group: &str,
        partition: nafka::Tp,
        offset: i64,
    ) -> ApplicationResult<()> {
        self.ensure_operation_open("group seek", false)?;
        self.capability
            .proxy
            .seek(group, partition, offset)
            .await
            .map_err(|error| self.operation_error("group seek", error))
    }

    /// 业务作用：把给定分区定位到最早可用 offset。
    ///
    /// 参数说明：
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    /// - `partitions`：需要重新定位的 topic-partition 集合。
    ///
    /// 返回：全部分区完成定位时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// Application 不处于 Ready、group 不存在或任一分区定位失败时返回错误。
    pub async fn seek_to_beginning(
        &self,
        group: &str,
        partitions: Vec<nafka::Tp>,
    ) -> ApplicationResult<()> {
        self.ensure_operation_open("group seek to beginning", false)?;
        self.capability
            .proxy
            .seek_to_beginning(group, partitions)
            .await
            .map_err(|error| self.operation_error("group seek to beginning", error))
    }

    /// 业务作用：把给定分区定位到当前末尾 offset。
    ///
    /// 参数说明：
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    /// - `partitions`：需要重新定位的 topic-partition 集合。
    ///
    /// 返回：全部分区完成定位时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// Application 不处于 Ready、group 不存在或任一分区定位失败时返回错误。
    pub async fn seek_to_end(
        &self,
        group: &str,
        partitions: Vec<nafka::Tp>,
    ) -> ApplicationResult<()> {
        self.ensure_operation_open("group seek to end", false)?;
        self.capability
            .proxy
            .seek_to_end(group, partitions)
            .await
            .map_err(|error| self.operation_error("group seek to end", error))
    }

    /// 业务作用：按 Unix epoch 毫秒时间戳重新定位多个分区。
    ///
    /// 参数说明：
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    /// - `timestamps`：分区到目标毫秒时间戳的有序映射。
    ///
    /// 返回：全部查询和定位完成时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// Application 不处于 Ready、时间戳查询或 owner 命令失败时返回错误。
    pub async fn seek_to_timestamp(
        &self,
        group: &str,
        timestamps: BTreeMap<nafka::Tp, i64>,
    ) -> ApplicationResult<()> {
        self.ensure_operation_open("group seek to timestamp", false)?;
        self.capability
            .proxy
            .seek_to_timestamp(group, timestamps)
            .await
            .map_err(|error| self.operation_error("group seek to timestamp", error))
    }

    /// 业务作用：把给定分区恢复到 group 已提交位点。
    ///
    /// 参数说明：
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    /// - `partitions`：需要恢复的 topic-partition 集合。
    ///
    /// 返回：全部分区恢复完成时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// Application 不处于 Ready、提交位点查询或 owner 命令失败时返回错误。
    pub async fn seek_to_committed(
        &self,
        group: &str,
        partitions: Vec<nafka::Tp>,
    ) -> ApplicationResult<()> {
        self.ensure_operation_open("group seek to committed", false)?;
        self.capability
            .proxy
            .seek_to_committed(group, partitions)
            .await
            .map_err(|error| self.operation_error("group seek to committed", error))
    }

    /// 业务作用：为给定分区增加业务暂停原因。
    ///
    /// 参数说明：
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    /// - `partitions`：需要暂停的 topic-partition 集合。
    ///
    /// 返回：owner 确认全部暂停时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// Application 不处于 Ready、group 不存在或底层暂停失败时返回错误。
    pub async fn pause(&self, group: &str, partitions: Vec<nafka::Tp>) -> ApplicationResult<()> {
        self.ensure_operation_open("group pause", false)?;
        self.capability
            .proxy
            .pause(group, partitions)
            .await
            .map_err(|error| self.operation_error("group pause", error))
    }

    /// 业务作用：在受管 Ready 门禁内提交逐分区控制，并保留部分执行证据。
    ///
    /// 参数说明：
    /// - `group`：本源最终组名。
    /// - `actions`：非空显式目标与操作。
    ///
    /// 返回：独立报告；容器未开放、声明非法或组缺席时返回受管错误，不绕过生命周期。
    pub async fn control_partitions(
        &self,
        group: &str,
        actions: std::collections::BTreeMap<nafka::Tp, nafka::PartitionControl>,
    ) -> ApplicationResult<nafka::ControlReport> {
        self.ensure_operation_open("partition control", false)?;
        self.capability
            .proxy
            .control_partitions(group, actions)
            .await
            .map_err(|error| self.operation_error("partition control", error))
    }

    /// 业务作用：移除给定分区的业务暂停原因。
    ///
    /// 参数说明：
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    /// - `partitions`：需要恢复的 topic-partition 集合。
    ///
    /// 返回：owner 确认全部恢复时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// Application 不处于 Ready、group 不存在或底层恢复失败时返回错误。
    pub async fn resume(&self, group: &str, partitions: Vec<nafka::Tp>) -> ApplicationResult<()> {
        self.ensure_operation_open("group resume", false)?;
        self.capability
            .proxy
            .resume(group, partitions)
            .await
            .map_err(|error| self.operation_error("group resume", error))
    }

    /// 业务作用：暂停 group 当前 assignment 的全部分区。
    ///
    /// 参数说明：
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    ///
    /// 返回：owner 确认全量暂停时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// Application 不处于 Ready、group 不存在或底层暂停失败时返回错误。
    pub async fn pause_all(&self, group: &str) -> ApplicationResult<()> {
        self.ensure_operation_open("group pause all", false)?;
        self.capability
            .proxy
            .pause_all(group)
            .await
            .map_err(|error| self.operation_error("group pause all", error))
    }

    /// 业务作用：恢复 group 当前 assignment 的全部分区。
    ///
    /// 参数说明：
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    ///
    /// 返回：owner 确认全量恢复时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// Application 不处于 Ready、group 不存在或底层恢复失败时返回错误。
    pub async fn resume_all(&self, group: &str) -> ApplicationResult<()> {
        self.ensure_operation_open("group resume all", false)?;
        self.capability
            .proxy
            .resume_all(group)
            .await
            .map_err(|error| self.operation_error("group resume all", error))
    }

    /// 业务作用：替换 subscribe group 的动态 topic 集合。
    ///
    /// 参数说明：
    ///
    /// - `group`：启动时声明为 subscribe 模式的 resolved group id。
    /// - `topics`：新的非空、无空值且不重复 topic 集合。
    ///
    /// 返回：owner 确认新订阅生效时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// Application 不处于 Ready、topic 集非法或 group 模式不支持时返回错误。
    pub async fn subscribe_group(&self, group: &str, topics: Vec<String>) -> ApplicationResult<()> {
        self.ensure_operation_open("group subscribe", false)?;
        self.capability
            .proxy
            .subscribe_group(group, topics)
            .await
            .map_err(|error| self.operation_error("group subscribe", error))
    }

    /// 业务作用：取消 subscribe group 的当前订阅但保留 owner。
    ///
    /// 参数说明：
    ///
    /// - `group`：启动时声明为 subscribe 模式的 resolved group id。
    ///
    /// 返回：owner 确认取消订阅时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// Application 不处于 Ready、group 不存在或模式不支持时返回错误。
    pub async fn unsubscribe_group(&self, group: &str) -> ApplicationResult<()> {
        self.ensure_operation_open("group unsubscribe", false)?;
        self.capability
            .proxy
            .unsubscribe_group(group)
            .await
            .map_err(|error| self.operation_error("group unsubscribe", error))
    }

    /// 业务作用：查询 broker 当前可见的全部 topic 名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Starting 或 Ready 阶段返回排序后的 topic 名称副本。
    ///
    /// # 错误
    ///
    /// Application 已排空、metadata 请求失败或超时时返回错误。
    pub async fn list_topics(&self) -> ApplicationResult<Vec<String>> {
        self.ensure_operation_open("topic metadata query", true)?;
        self.capability
            .proxy
            .admin()
            .list_topics()
            .await
            .map_err(|error| self.operation_error("topic metadata query", error))
    }

    /// 业务作用：查询指定 topic 的可见分区编号。
    ///
    /// 参数说明：
    ///
    /// - `topic`：需要读取 metadata 的非空业务 topic 名称。
    ///
    /// 返回：Starting 或 Ready 阶段返回排序后的分区编号副本。
    ///
    /// # 错误
    ///
    /// Application 已排空、topic 非法或 metadata 请求失败时返回错误。
    pub async fn partitions_for(&self, topic: &str) -> ApplicationResult<Vec<i32>> {
        self.ensure_operation_open("topic partition metadata query", true)?;
        self.capability
            .proxy
            .admin()
            .partitions_for(topic)
            .await
            .map_err(|error| self.operation_error("topic partition metadata query", error))
    }

    /// 业务作用：在受管源上读取明确隔离级别的位置证据，不扩大写入或关闭权限。
    ///
    /// 参数说明：
    /// - `group`：可选组。
    /// - `tps`：明确分区。
    /// - `isolation`：读取隔离。
    /// - `timestamp_millis`：可选时间。
    /// - `timeout`：全部步骤共享预算。
    ///
    /// 返回：Starting/Ready 时提供逐字段观测；进入排空后拒绝新查询，已经接纳的实际查询仍受源关闭追踪。
    pub async fn observe_offsets(
        &self,
        group: Option<&str>,
        tps: Vec<nafka::Tp>,
        isolation: nafka::OffsetIsolation,
        timestamp_millis: Option<i64>,
        timeout: std::time::Duration,
    ) -> ApplicationResult<nafka::OffsetObservation> {
        self.ensure_operation_open("offset observation", true)?;
        self.capability
            .proxy
            .admin()
            .observe_offsets(group, tps, isolation, timestamp_millis, timeout)
            .await
            .map_err(|error| self.operation_error("offset observation", error))
    }

    /// 业务作用：在应用初始化或就绪阶段只读核对 topic 声明，不自动更改 broker 资源。
    ///
    /// 参数说明：
    /// - `spec`：分区、副本和配置子集。
    /// - `timeout`：整轮查询预算。
    ///
    /// 返回：匹配时提供实际描述；漂移或查询失败保留原因，应用排空后拒绝新核验。
    pub async fn verify_topic(
        &self,
        spec: nafka::TopicSpec,
        timeout: std::time::Duration,
    ) -> ApplicationResult<nafka::TopicDescription> {
        self.ensure_operation_open("topic contract verification", true)?;
        self.capability
            .proxy
            .admin()
            .verify_topic(spec, timeout)
            .await
            .map_err(|error| self.operation_error("topic contract verification", error))
    }

    /// 业务作用：显式重建一个允许人工恢复的 consumer group。
    ///
    /// 参数说明：
    ///
    /// - `group`：处于可恢复故障状态的 resolved group id。
    ///
    /// 返回：owner 接受并完成重建命令时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// Application 不处于 Ready、状态不允许重建或命令结果未知时返回错误。
    pub async fn restart_group(&self, group: &str) -> ApplicationResult<()> {
        self.ensure_operation_open("group restart", false)?;
        self.capability
            .proxy
            .restart_group(group)
            .await
            .map_err(|error| self.operation_error("group restart", error))
    }

    /// 业务作用：校验当前 Application 阶段是否允许创建本次操作。
    ///
    /// 参数说明：
    ///
    /// - `operation`：不含业务输入的稳定操作名称，用于错误归因。
    /// - `allow_starting`：producer 装配传 true；只有 consumer 已 Ready 才合法的控制面传 false。
    ///
    /// 返回：当前阶段允许操作时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// 阶段过早、已 Draining 或已终止时返回 Kafka 组件错误。
    fn ensure_operation_open(
        &self,
        operation: &'static str,
        allow_starting: bool,
    ) -> ApplicationResult<()> {
        let state = self.application_state.load();
        if state == ApplicationState::Ready
            || (allow_starting && state == ApplicationState::Starting)
        {
            return Ok(());
        }
        Err(unavailable_error(
            ComponentId::Kafka,
            state,
            format!("{operation} is unavailable while application state is {state:?}"),
        ))
    }

    /// 业务作用：把 nafka 数据面或控制面错误包装进统一 Application 错误域。
    ///
    /// 参数说明：
    ///
    /// - `operation`：不含 client/group/topic 输入的稳定操作名称。
    /// - `error`：nafka 返回的原始类型化错误；公开摘要不复制其可能包含的细节。
    ///
    /// 返回： component=kafka 且阶段随 Application 状态变化的错误，source 链保留原始分类。
    fn operation_error(
        &self,
        operation: &'static str,
        error: impl Into<anyhow::Error>,
    ) -> ApplicationError {
        let state = self.application_state.load();
        let phase = match state {
            ApplicationState::Starting => ApplicationPhase::Ready,
            ApplicationState::Ready => ApplicationPhase::Running,
            ApplicationState::Stopping => ApplicationPhase::Stopping,
            ApplicationState::Stopped | ApplicationState::Failed => ApplicationPhase::Stopped,
        };
        ApplicationError::with_source(
            ComponentId::Kafka,
            phase,
            format!("kafka {operation} failed"),
            error,
        )
    }
}

/// 日志组件对外开放的只读运行时能力句柄。
///
/// 日志订阅器是进程级设施，没有可克隆的实例客户端；该句柄只确认容器管理的订阅器是否已经建立，
/// 写日志仍使用门面公开的事件 API，配置重应用和文件输出关闭仍由容器独占。
#[cfg(feature = "log")]
#[derive(Clone)]
pub struct LogHandle {
    /// 日志组件发布的初始化事实，不拥有订阅器或文件关闭权。
    runtime: Arc<LogRuntimeState>,
    /// 应用生命周期的共享来源，与日志初始化标记独立读取。
    application_state: Arc<StateCell>,
}

#[cfg(feature = "log")]
impl LogHandle {
    /// 业务作用：从容器共享状态创建日志能力句柄。
    ///
    /// 参数说明：
    ///
    /// - `runtime`：日志组件独占写入、句柄只读的发布状态。
    /// - `application_state`：与 Application 共用的生命周期状态来源。
    ///
    /// 返回：共享观察句柄；不初始化日志订阅器，也不取得文件输出的关闭权。
    pub(crate) fn new(runtime: Arc<LogRuntimeState>, application_state: Arc<StateCell>) -> Self {
        Self {
            runtime,
            application_state,
        }
    }

    /// 业务作用：判断容器管理的日志订阅器是否已经完成早期初始化。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：读取日志组件发布的初始化标记；关闭动作撤销标记后为假。
    /// 不查询应用生命周期，也不确认待写日志已经刷盘。
    pub fn is_initialized(&self) -> bool {
        self.runtime.initialized.load(Ordering::Acquire)
    }

    /// 业务作用：返回与 Application 同源的组件生命周期状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前应用状态的映射；不读取日志初始化标记或文件输出队列。
    pub fn lifecycle(&self) -> ComponentLifecycleState {
        self.application_state.load().into()
    }
}

/// 日志组件发布初始化和关闭结果的内部状态。
#[cfg(feature = "log")]
pub(crate) struct LogRuntimeState {
    /// Bootstrap 发布、正常关闭确认后撤销的初始化标记；不记录每条日志的落盘结果。
    initialized: AtomicBool,
}

#[cfg(feature = "log")]
impl LogRuntimeState {
    /// 业务作用：创建尚未初始化的日志运行时状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：初始化标记为假的本地状态；不安装订阅器，Bootstrap 成功后由组件另行发布。
    pub(crate) const fn new() -> Self {
        Self {
            initialized: AtomicBool::new(false),
        }
    }

    /// 业务作用：发布日志订阅器已经初始化。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：以 Release 写入真，重复写入不改变结果；调用方必须先完成实际初始化。
    pub(crate) fn publish_initialized(&self) {
        self.initialized.store(true, Ordering::Release);
    }

    /// 业务作用：由日志清理动作撤销已发布的初始化标记。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：以 Release 写入假，不执行刷盘或关闭文件；此标记不能证明阻塞清理已经完成。
    pub(crate) fn close(&self) {
        self.initialized.store(false, Ordering::Release);
    }
}

/// 配置中心组件对外开放的只读拉取能力句柄。
///
/// 句柄只允许读取远端原文，不公开发布、删除、监听注册或关闭入口，避免业务绕过配置快照合并和统一清理。
#[cfg(feature = "nacos-config")]
#[derive(Clone)]
pub struct NacosConfigHandle {
    /// 只保存配置客户端的启用决定和弱引用，不延长底层连接的所有权。
    runtime: Arc<NacosConfigRuntimeState>,
    /// 生命周期观察与不可用错误的阶段来源，不是远端健康探测。
    application_state: Arc<StateCell>,
}

#[cfg(feature = "nacos-config")]
impl NacosConfigHandle {
    /// 业务作用：从容器共享状态创建配置中心能力句柄。
    ///
    /// 参数说明：
    ///
    /// - `runtime`：保存底层客户端弱引用和启用状态的共享单元。
    /// - `application_state`：与 Application 共用的生命周期状态来源。
    ///
    /// 返回：不持有底层客户端强引用的只读能力句柄；构造时不连接 Nacos，也不发布配置。
    pub(crate) fn new(
        runtime: Arc<NacosConfigRuntimeState>,
        application_state: Arc<StateCell>,
    ) -> Self {
        Self {
            runtime,
            application_state,
        }
    }

    /// 业务作用：返回配置中心是否已确定启用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尚未发布决定时为 None，禁用为 Some(false)，发布过客户端为 Some(true)。
    /// 不升级弱引用；即使客户端已释放，历史启用结果仍为真。
    pub fn is_enabled(&self) -> Option<bool> {
        self.runtime.client.get().map(|client| client.is_some())
    }

    /// 业务作用：使用容器已建立的底层连接拉取一份远端配置原文。
    ///
    /// 参数说明：
    ///
    /// - `data_id`：远端配置文档标识。
    /// - `group`：远端配置分组。
    ///
    /// 返回：底层客户端取得的配置原文；禁用、未发布或弱引用失效时拒绝，拉取失败保留来源错误。
    /// 不合并配置树，不触发组件热应用；请求期间暂时持有客户端强引用。
    pub async fn fetch(&self, data_id: &str, group: &str) -> ApplicationResult<String> {
        let client = self.client()?;
        client.fetch(data_id, group).await.map_err(|error| {
            ApplicationError::with_source(
                ComponentId::NacosConfig,
                ApplicationPhase::Running,
                "config center fetch failed",
                error,
            )
        })
    }

    /// 业务作用：使用连接时配置的默认分组拉取一份远端配置原文。
    ///
    /// 参数说明：
    ///
    /// - `data_id`：远端配置文档标识。
    ///
    /// 返回：客户端默认分组中的配置原文，不发布新的应用配置视图；客户端不可用或拉取失败时返回错误。
    pub async fn fetch_default_group(&self, data_id: &str) -> ApplicationResult<String> {
        let client = self.client()?;
        client.fetch_default_group(data_id).await.map_err(|error| {
            ApplicationError::with_source(
                ComponentId::NacosConfig,
                ApplicationPhase::Running,
                "config center default-group fetch failed",
                error,
            )
        })
    }

    /// 业务作用：返回与 Application 同源的组件生命周期状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前应用状态的映射；不升级客户端弱引用，也不探测 Nacos 或配置监听任务。
    pub fn lifecycle(&self) -> ComponentLifecycleState {
        self.application_state.load().into()
    }

    /// 业务作用：升级容器发布的底层客户端弱引用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：存在有效强引用时取得客户端 Arc；未发布、禁用或最后一个强引用已释放时拒绝。
    /// 应用状态仅决定错误阶段，此处不据它拒绝仍存活的客户端，也不承诺远端可用。
    fn client(&self) -> ApplicationResult<Arc<nanacos::NacosConfigClient>> {
        let state = self.application_state.load();
        // 尚未发布与明确禁用是不同结果，均不能临时创建旁路客户端绕过容器装配决定。
        let Some(client) = self.runtime.client.get() else {
            return Err(unavailable_error(
                ComponentId::NacosConfig,
                state,
                "config center capability is not published yet",
            ));
        };
        let Some(client) = client else {
            return Err(unavailable_error(
                ComponentId::NacosConfig,
                state,
                "config center is disabled by configuration",
            ));
        };
        client.upgrade().ok_or_else(|| {
            unavailable_error(
                ComponentId::NacosConfig,
                state,
                "config center client is no longer available",
            )
        })
    }
}

/// 配置中心组件发布客户端但不转移关闭所有权的内部状态。
#[cfg(feature = "nacos-config")]
pub(crate) struct NacosConfigRuntimeState {
    /// 未写入表示尚未决定，None 表示禁用，Some 表示发布过客户端弱引用；只能写入一次。
    client: OnceLock<Option<Weak<nanacos::NacosConfigClient>>>,
}

#[cfg(feature = "nacos-config")]
impl NacosConfigRuntimeState {
    /// 业务作用：创建尚未读取启用开关的配置中心状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尚无决定的 OnceLock；此后禁用与客户端发布共享同一个只能成功写入一次的槽位。
    pub(crate) const fn new() -> Self {
        Self {
            client: OnceLock::new(),
        }
    }

    /// 业务作用：发布配置中心已禁用，不创建底层连接。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次发布禁用决定为真；已有任何决定时为假并保留原值，不关闭已发布的客户端。
    pub(crate) fn publish_disabled(&self) -> bool {
        self.client.set(None).is_ok()
    }

    /// 业务作用：发布底层配置客户端的弱引用。
    ///
    /// 参数说明：
    ///
    /// - `client`：由组件和关闭动作持有强引用的底层客户端。
    ///
    /// 返回：首次写入弱引用为真；禁用或客户端已经发布则为假并保留原决定，不重连也不替换客户端。
    pub(crate) fn publish_client(&self, client: &Arc<nanacos::NacosConfigClient>) -> bool {
        self.client.set(Some(Arc::downgrade(client))).is_ok()
    }
}

/// 长连接组件对外开放的发送和只读运行状态句柄。
#[cfg(feature = "ws")]
#[derive(Clone)]
pub struct WsHandle {
    /// 发送器弱引用与历史监听地址的共享发布状态，不包含服务关闭权。
    runtime: Arc<WsRuntimeState>,
    /// 宿主生命周期来源，不能据此推断发送器强引用或会话是否已经释放。
    application_state: Arc<StateCell>,
}

#[cfg(feature = "ws")]
impl WsHandle {
    /// 业务作用：从容器共享状态创建长连接能力句柄。
    ///
    /// 参数说明：
    ///
    /// - `runtime`：组件发布发送器弱引用和真实监听地址的共享状态。
    /// - `application_state`：与 Application 共用的生命周期状态来源。
    ///
    /// 返回：不持有监听器或发送器强引用的观察句柄；构造时不绑定端口，也不建立会话。
    pub(crate) fn new(runtime: Arc<WsRuntimeState>, application_state: Arc<StateCell>) -> Self {
        Self {
            runtime,
            application_state,
        }
    }

    /// 业务作用：返回实际绑定的 TCP 地址。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：地址尚未发布时为 None，发布后为记录的绑定地址；停机不清空该记录，不证明端口仍在监听。
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.runtime.addrs.get().map(|addrs| addrs.0)
    }

    /// 业务作用：返回实际绑定的独立 WebSocket 地址。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：地址未发布或没有独立 WebSocket 监听器时为 None；发布后保留历史绑定地址，
    /// 不随关闭清空，也不表示共享 TCP 端口缺少 WebSocket 能力。
    pub fn websocket_addr(&self) -> Option<SocketAddr> {
        self.runtime.addrs.get().and_then(|addrs| addrs.1)
    }

    /// 业务作用：获取底层长连接库的共享广播发送器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：弱引用仍可升级时取得发送器 Arc；未发布或所有强引用已释放时返回阶段错误。
    /// 此入口不检查应用准入或活跃会话，外部持有 Arc 时可在宿主停机后继续取得同一发送器。
    pub fn sender(&self) -> ApplicationResult<Arc<naws::Sender>> {
        let state = self.application_state.load();
        // 只能使用宿主已经发布且仍存活的发送器，缺失时不能另建实例绕过原会话目录。
        self.runtime
            .sender
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| {
                unavailable_error(
                    ComponentId::Ws,
                    state,
                    "ws sender is not available; it is published during service binding and released during shutdown",
                )
            })
    }

    /// 业务作用：返回与 Application 同源的组件生命周期状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前应用状态的映射；不读取发送器的强引用数、会话数或监听器状态。
    pub fn lifecycle(&self) -> ComponentLifecycleState {
        self.application_state.load().into()
    }
}

/// 长连接组件一次发布、能力句柄只读访问的内部运行时状态。
#[cfg(feature = "ws")]
pub(crate) struct WsRuntimeState {
    /// 服务绑定时发布的发送器弱引用；该槽位不因旧对象释放而允许替换。
    sender: OnceLock<Weak<naws::Sender>>,
    /// 首次发布的 TCP 与可选独立 WebSocket 地址，关闭后仍保留历史记录。
    addrs: OnceLock<(SocketAddr, Option<SocketAddr>)>,
}

#[cfg(feature = "ws")]
impl WsRuntimeState {
    /// 业务作用：创建尚未发布发送器和监听地址的长连接状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：两个独立的空发布槽位；不创建服务。发送器和地址分别发布，读取它们不构成原子快照。
    pub(crate) const fn new() -> Self {
        Self {
            sender: OnceLock::new(),
            addrs: OnceLock::new(),
        }
    }

    /// 业务作用：发布底层发送器弱引用，不把服务资源所有权转给 Application。
    ///
    /// 参数说明：
    ///
    /// - `sender`：服务对象持有的共享广播发送器。
    ///
    /// 返回：首次记录弱引用为真；已发布时为假并保留原发送器，即使原弱引用已经失效也不替换。
    pub(crate) fn publish_sender(&self, sender: &Arc<naws::Sender>) -> bool {
        self.sender.set(Arc::downgrade(sender)).is_ok()
    }

    /// 业务作用：发布绑定完成后的真实监听地址。
    ///
    /// 参数说明：
    ///
    /// - `addr`：TCP 数据面真实地址。
    /// - `websocket`：可选的独立 WebSocket 数据面真实地址。
    ///
    /// 返回：首次记录地址对为真；重复发布为假并保留原值，不绑定端口、不核验监听器是否存活。
    pub(crate) fn publish_addrs(&self, addr: SocketAddr, websocket: Option<SocketAddr>) -> bool {
        self.addrs.set((addr, websocket)).is_ok()
    }
}

/// 服务发现组件对外开放的出站请求与注册状态句柄。
#[cfg(feature = "nacos-discovery")]
#[derive(Clone)]
pub struct NacosDiscoveryHandle {
    /// 发现会话的弱引用发布状态，不持有注册守卫或出站运行时。
    runtime: Arc<NacosDiscoveryRuntimeState>,
    /// 应用生命周期来源；独立于注册中心当前可见性。
    application_state: Arc<StateCell>,
}

/// 服务发现中由框架验证并选出的原生 gRPC endpoint。
#[cfg(all(feature = "nacos-discovery", feature = "grpc"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcDiscoveredEndpoint {
    /// 从健康实例记录取得的拨号主机或 IP，不复用 metadata 中的 TLS 身份作为网络地址。
    host: String,
    /// metadata 明确声明的非零 gRPC 监听端口。
    port: u16,
    /// 实例声明的传输安全模式；客户端仍需装配相应信任根与身份。
    tls_mode: GrpcDiscoveredTlsMode,
    /// 用于 SNI 和证书校验的身份，合法 metadata 缺省时采用实例 IP。
    authority: String,
}

/// 发现端点声明的 TLS 合同；调用方必须按该模式提供匹配的 client 身份与信任根。
#[cfg(all(feature = "nacos-discovery", feature = "grpc"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrpcDiscoveredTlsMode {
    /// 明文原生 gRPC，仅适用于部署明确允许的网络边界。
    Disabled,
    /// 服务端 TLS，客户端必须验证 authority。
    Server,
    /// 双向 TLS，客户端还必须提供受信身份。
    Mutual,
}

#[cfg(all(feature = "nacos-discovery", feature = "grpc"))]
impl GrpcDiscoveredEndpoint {
    /// 业务作用：返回 provider 给出的可拨号主机或 IP。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：经过 provider 健康过滤且非空的实例地址。
    pub fn host(&self) -> &str {
        &self.host
    }

    /// 业务作用：返回 gRPC metadata 中声明的独立端口，不复用 REST 主端口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：非零的实际 gRPC listener 端口。
    pub fn port(&self) -> u16 {
        self.port
    }

    /// 业务作用：返回 endpoint 的 TLS 模式，供 client 装配匹配的身份与信任根。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Disabled、Server 或 Mutual 中的一个封闭值。
    pub fn tls_mode(&self) -> GrpcDiscoveredTlsMode {
        self.tls_mode
    }

    /// 业务作用：返回 TLS SNI/证书校验使用的 authority；元数据未覆盖时使用注册 IP。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不含 scheme、端口和路径的 authority。
    pub fn authority(&self) -> &str {
        &self.authority
    }

    /// 业务作用：构造 generated client 可直接交给 `nasa::grpc::Endpoint` 的基础 URI。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按 TLS 模式选择 `http` 或 `https`，IPv6 地址自动加方括号。
    pub fn uri(&self) -> String {
        let scheme = if self.tls_mode == GrpcDiscoveredTlsMode::Disabled {
            "http"
        } else {
            "https"
        };
        let host = if self.host.parse::<std::net::Ipv6Addr>().is_ok() {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        format!("{scheme}://{host}:{}", self.port)
    }
}

#[cfg(feature = "nacos-discovery")]
impl NacosDiscoveryHandle {
    /// 业务作用：从容器共享状态创建服务发现能力句柄。
    ///
    /// 参数说明：
    ///
    /// - `runtime`：保存发现会话弱引用的共享状态。
    /// - `application_state`：与 Application 共用的生命周期状态来源。
    ///
    /// 返回：只引用发布状态的能力句柄；不建立发现会话，不注册实例，也不取得关闭权。
    pub(crate) fn new(
        runtime: Arc<NacosDiscoveryRuntimeState>,
        application_state: Arc<StateCell>,
    ) -> Self {
        Self {
            runtime,
            application_state,
        }
    }

    /// 业务作用：获取底层带负载均衡能力的共享 HTTP 客户端。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：在会话锁内克隆已安装的 HTTP 客户端；会话未发布、弱引用失效或出站运行时已移除时拒绝。
    /// 返回成功不保证下一次请求仍有可用实例，客户端 Arc 也不转移宿主的关闭责任。
    pub async fn client(
        &self,
    ) -> ApplicationResult<Arc<rest_discovery_nacos::RestDiscoveryClient>> {
        let session = self.session()?;
        let client = session.lock().await.rest_client();
        client.ok_or_else(|| {
            unavailable_error(
                ComponentId::NacosDiscovery,
                self.application_state.load(),
                "outbound discovery client is no longer available",
            )
        })
    }

    /// 业务作用：读取发现会话是否仍持有本实例的注册守卫。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：会话锁内的本地登记状态；纯消费者或已注销为假，会话不可用时失败。
    /// 不向注册中心复查可见性、租约或健康状态，不能作为远端当前在线证明。
    pub async fn is_registered(&self) -> ApplicationResult<bool> {
        let session = self.session()?;
        let registered = session.lock().await.is_registered();
        Ok(registered)
    }

    /// 业务作用：查询当前配置是否要求注册本实例。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：会话冻结配置中发现与注册两个开关同时开启时为真；会话不可用时失败。
    /// 查询不执行注册，真值也不代表已经向远端发布成功。
    pub async fn wants_registration(&self) -> ApplicationResult<bool> {
        let session = self.session()?;
        let wants_registration = session.lock().await.wants_registration();
        Ok(wants_registration)
    }

    /// 业务作用：向受管协议适配器提供同一次发现查询中的完整实例记录，保持端点和元数据绑定。
    ///
    /// 参数说明：
    /// - `service`：受信配置指定的注册服务名。
    ///
    /// 返回：发现中心的健康实例集合；会话已关闭或查询失败时返回错误，不回退到历史地址。
    #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
    pub(crate) async fn saga_instances(
        &self,
        service: &str,
    ) -> ApplicationResult<Vec<rest_discovery_nacos::Instance>> {
        let session = self.session()?;
        let instances = session
            .lock()
            .await
            .discover_instances(service)
            .await
            .map_err(|error| {
                ApplicationError::with_source(
                    ComponentId::NacosDiscovery,
                    ApplicationPhase::Running,
                    "cannot resolve Saga service instances",
                    error,
                )
            })?;
        Ok(instances)
    }

    /// 业务作用：从注册中心选择一条声明原生 gRPC 协议且 endpoint 合同完整的健康实例。
    ///
    /// 参数说明：
    /// - `service`: 注册中心内的服务名，不是 protobuf service full name。
    ///
    /// 返回：至少存在一条合法 gRPC endpoint 时返回其地址、端口、TLS 与 authority；查询失败或没有
    /// 合格实例时返回服务发现错误，绝不把 REST 主端口猜成 gRPC 端口。
    #[cfg(feature = "grpc")]
    pub async fn grpc_endpoint(&self, service: &str) -> ApplicationResult<GrpcDiscoveredEndpoint> {
        let session = self.session()?;
        let instances = session
            .lock()
            .await
            .discover_instances(service)
            .await
            .map_err(|error| {
                ApplicationError::with_source(
                    ComponentId::NacosDiscovery,
                    ApplicationPhase::Running,
                    "cannot resolve a gRPC service endpoint",
                    error,
                )
            })?;
        instances
            .into_iter()
            .find_map(parse_grpc_endpoint)
            .ok_or_else(|| {
                ApplicationError::new(
                    ComponentId::NacosDiscovery,
                    ApplicationPhase::Running,
                    "no healthy instance publishes a valid gRPC endpoint",
                )
            })
    }

    /// 业务作用：返回与 Application 同源的组件生命周期状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前应用状态的映射；不探测注册中心，不读取本地注册守卫，也不延长会话生命周期。
    pub fn lifecycle(&self) -> ComponentLifecycleState {
        self.application_state.load().into()
    }

    /// 业务作用：升级组件发布的发现会话弱引用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：存在强引用时取得共享会话；未发布或已释放时拒绝，并按应用状态归类错误。
    /// 此处不验证会话内部客户端是否关闭；调用期间的 Arc 会暂时延长会话对象存活时间。
    fn session(
        &self,
    ) -> ApplicationResult<Arc<tokio::sync::Mutex<rest_discovery_nacos::DiscoverySession>>> {
        // 升级失败表示此入口已经没有可用会话，不自动重建发现运行时或重新注册实例。
        self.runtime
            .session
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| {
                unavailable_error(
                    ComponentId::NacosDiscovery,
                    self.application_state.load(),
                    "discovery runtime is not available; it is published after provider setup and released during shutdown",
                )
            })
    }
}

/// 业务作用：把 provider 的固定 metadata 映射为 typed gRPC endpoint，并拒绝不完整或开放式值域。
///
/// 参数说明：
/// - `instance`: 已通过 provider 健康过滤的服务实例。
///
/// 返回：协议、端口、TLS 模式与 authority 均合法时返回 typed endpoint，否则跳过该实例。
#[cfg(all(feature = "nacos-discovery", feature = "grpc"))]
fn parse_grpc_endpoint(instance: rest_discovery_nacos::Instance) -> Option<GrpcDiscoveredEndpoint> {
    if instance.metadata.get("nasa.grpc.protocol")?.as_str() != "grpc" {
        return None;
    }
    let port = instance.metadata.get("nasa.grpc.port")?.parse().ok()?;
    if port == 0 {
        return None;
    }
    let tls_mode = match instance.metadata.get("nasa.grpc.tls_mode")?.as_str() {
        "disabled" => GrpcDiscoveredTlsMode::Disabled,
        "server" => GrpcDiscoveredTlsMode::Server,
        "mutual" => GrpcDiscoveredTlsMode::Mutual,
        _ => return None,
    };
    let authority = instance
        .metadata
        .get("nasa.grpc.authority")
        .map(String::as_str)
        .filter(|value| valid_grpc_authority(value))
        .unwrap_or(&instance.ip)
        .to_owned();
    if !valid_grpc_authority(&authority) {
        return None;
    }
    Some(GrpcDiscoveredEndpoint {
        host: instance.ip,
        port,
        tls_mode,
        authority,
    })
}

/// 业务作用：限制发现 authority 为单一 DNS/IP 身份，阻止 metadata 注入 scheme、端口或路径。
///
/// 参数说明：
/// - `value`: provider metadata 或实例 IP 中取得的候选 authority。
///
/// 返回：长度有界、无首尾空白且不含 URI 分隔符时为 `true`。
#[cfg(all(feature = "nacos-discovery", feature = "grpc"))]
fn valid_grpc_authority(value: &str) -> bool {
    if value.parse::<std::net::IpAddr>().is_ok() {
        return true;
    }
    !value.is_empty()
        && value.len() <= 253
        && value == value.trim()
        && !value
            .bytes()
            .any(|byte| matches!(byte, b'/' | b':' | b'?' | b'#' | b'@'))
}

/// 服务发现组件发布会话但不转移关闭所有权的内部状态。
#[cfg(feature = "nacos-discovery")]
pub(crate) struct NacosDiscoveryRuntimeState {
    /// 一次发布的会话弱引用；关闭动作持有强引用并负责注销和释放出站运行时。
    session: OnceLock<Weak<tokio::sync::Mutex<rest_discovery_nacos::DiscoverySession>>>,
}

#[cfg(feature = "nacos-discovery")]
impl NacosDiscoveryRuntimeState {
    /// 业务作用：创建尚未连接发现 provider 的运行时状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：空的会话发布槽位；不建立网络连接，后续只能成功登记一个会话弱引用。
    pub(crate) const fn new() -> Self {
        Self {
            session: OnceLock::new(),
        }
    }

    /// 业务作用：发布组件持有的发现会话弱引用。
    ///
    /// 参数说明：
    ///
    /// - `session`：由组件清理动作持有强引用的发现会话。
    ///
    /// 返回：首次保存弱引用为真；重复发布为假并保留原会话。只公开本地访问入口，不执行远端注册。
    pub(crate) fn publish_session(
        &self,
        session: &Arc<tokio::sync::Mutex<rest_discovery_nacos::DiscoverySession>>,
    ) -> bool {
        self.session.set(Arc::downgrade(session)).is_ok()
    }
}

/// 调度组件对外开放的只读底层运行时句柄。
#[cfg(feature = "scheduling")]
#[derive(Clone)]
pub struct SchedulingHandle {
    /// 调度运行标记与可选选主对象的发布状态，不包含停止调度器的权限。
    runtime: Arc<SchedulingRuntimeState>,
    /// 宿主生命周期来源；与底层运行标记和选主状态分别读取。
    application_state: Arc<StateCell>,
}

#[cfg(feature = "scheduling")]
impl SchedulingHandle {
    /// 业务作用：从容器共享状态创建调度能力句柄。
    ///
    /// 参数说明：
    ///
    /// - `runtime`：记录组件是否完成底层调度器启动的共享状态。
    /// - `application_state`：与 Application 共用的生命周期状态来源。
    ///
    /// 返回：共享观察句柄；不启动调度器、不参加选主，也不转移停机责任。
    pub(crate) fn new(
        runtime: Arc<SchedulingRuntimeState>,
        application_state: Arc<StateCell>,
    ) -> Self {
        Self {
            runtime,
            application_state,
        }
    }

    /// 业务作用：获取底层调度库的只读运行时句柄。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：组件发布的 running 标记为真时取得调度句柄，否则返回阶段错误。
    /// 仅检查本次读取的标记，不预留后续操作资格，也不代表所有调度任务健康。
    pub fn runtime(&self) -> ApplicationResult<nasched::SchedulerHandle> {
        // 未确认底层启动或已经撤销运行标记时拒绝导出句柄，避免观察入口变成隐式启动入口。
        if self.runtime.running.load(Ordering::Acquire) {
            return Ok(nasched::scheduler_handle());
        }
        Err(unavailable_error(
            ComponentId::Scheduling,
            self.application_state.load(),
            "scheduled task runtime is not available",
        ))
    }

    /// 业务作用：查询集群调度模式下当前节点是否持有 leader 身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：选主弱引用尚未发布或已失效时为 None；对象仍存活则返回其当前本地领导权判断，
    /// 尚未取得领导权通常为 Some(false)。此查询不续租，也不授予后续任务执行权。
    #[cfg(feature = "scheduling-cluster")]
    pub fn is_leader(&self) -> Option<bool> {
        self.runtime
            .leader
            .get()
            .and_then(Weak::upgrade)
            .map(|leader| leader.is_leader())
    }

    /// 业务作用：返回与 Application 同源的组件生命周期状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前应用状态的映射；不读取 running 标记或领导权，不据此证明调度器已完成排干。
    pub fn lifecycle(&self) -> ComponentLifecycleState {
        self.application_state.load().into()
    }
}

/// 调度组件发布启动结果和可选选主句柄的内部状态。
#[cfg(feature = "scheduling")]
pub(crate) struct SchedulingRuntimeState {
    /// 底层启动成功后置位，清理收尾时撤销；假值不证明调度任务已全部退出。
    running: AtomicBool,
    #[cfg(feature = "scheduling-cluster")]
    /// 集群选主对象的弱引用，供观察领导权使用，不延长租约持有者存活时间。
    leader: OnceLock<Weak<nadis::leader::Leader>>,
}

#[cfg(feature = "scheduling")]
impl SchedulingRuntimeState {
    /// 业务作用：创建尚未启动底层调度器的状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：运行标记为假的本地状态；集群能力开启时选主槽位为空，不启动调度或续租任务。
    pub(crate) const fn new() -> Self {
        Self {
            running: AtomicBool::new(false),
            #[cfg(feature = "scheduling-cluster")]
            leader: OnceLock::new(),
        }
    }

    /// 业务作用：发布底层调度器已经完整启动。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：以 Release 将运行标记置真，使运行时句柄可被取得；调用方须先完成底层启动。
    /// 不代替整个应用的 Ready 发布，也不表示已经取得集群领导权。
    pub(crate) fn publish_running(&self) {
        self.running.store(true, Ordering::Release);
    }

    /// 业务作用：在调度清理收尾时撤销后续获取运行时句柄的资格。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：以 Release 将运行标记置假；不调用底层关闭函数，也不撤销已取得的句柄。
    /// 调度清理失败时也会撤销该标记，不能用假值代替资源已排干的证明。
    pub(crate) fn close(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// 业务作用：发布集群模式使用的只读选主弱引用。
    ///
    /// 参数说明：
    ///
    /// - `leader`：由调度清理动作持有并负责退位的选主对象。
    ///
    /// 返回：首次保存弱引用为真；重复发布为假并保留原值，不发起选举，也不延长选主对象的存活时间。
    #[cfg(feature = "scheduling-cluster")]
    pub(crate) fn publish_leader(&self, leader: &Arc<nadis::leader::Leader>) -> bool {
        self.leader.set(Arc::downgrade(leader)).is_ok()
    }
}
