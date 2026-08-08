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

/// 组件能力相对于统一应用状态机的只读生命周期状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentLifecycleState {
    /// 应用仍在启动，组件的底层对象可能尚未发布。
    Starting,
    /// 全部就绪动作已经完成，组件可以承接正常业务访问。
    Ready,
    /// 应用正在摘流并按逆序释放组件资源。
    Draining,
    /// 正常清理已经完成，底层对象不再可用。
    Closed,
    /// 启动、运行或清理发生主故障且生命周期已经收敛。
    Failed,
}

impl ComponentLifecycleState {
    /// 业务作用：返回适合管理端和指标标签使用的稳定名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回值不包含故障详情或业务输入。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Draining => "draining",
            Self::Closed => "closed",
            Self::Failed => "failed",
        }
    }

    /// 业务作用：判断组件是否处于统一应用定义的正常可用阶段。
    ///
    /// # 参数
    ///
    /// 本方法无参数；只有 `Ready` 返回 `true`。
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::Ready)
    }
}

impl From<ApplicationState> for ComponentLifecycleState {
    /// 业务作用：把应用状态机读数映射为组件能力生命周期状态。
    ///
    /// # 参数
    ///
    /// - `state`：从 Application 共享状态单元读取的当前状态。
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
/// # 参数
///
/// - `component`：能力所属的组件身份。
/// - `state`：用于选择错误阶段的统一应用状态。
/// - `message`：不包含配置秘密或业务负载的稳定说明。
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
    /// 当前 client 的全部 ReadyRule 是否同时满足。
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
    /// 动态就绪聚合项；更新顺序为先写快照、后发布该原子结论。
    contributor: crate::readiness::ReadinessContributor,
    /// 供管理面读取的最近一次脱敏快照。
    readiness: std::sync::RwLock<KafkaReadinessSnapshot>,
    /// Start 注入 proxy、UserHook 安装真实 sink 的内部时序桥。
    metrics: Arc<crate::kafka::KafkaMetricsBridge>,
}

#[cfg(feature = "kafka")]
impl KafkaClientCapability {
    /// 业务作用：创建初始未就绪的 Kafka client 能力根。
    ///
    /// # 参数
    ///
    /// - `client_name`：通过 KafkaConfig 校验的稳定 client name。
    /// - `proxy`：Start 已完成本地构造、尚未启动 consumer 的原始运行时。
    /// - `contributor`：Application readiness registry 为本 client 分配的独占贡献句柄。
    /// - `metrics`：Start 时注入 proxy 的内部可替换指标桥。
    ///
    /// # 返回
    ///
    /// 返回只能由 KafkaRuntimeState 和生命周期 action 持有的共享能力根。
    pub(crate) fn new(
        client_name: Arc<str>,
        proxy: nafka::KafkaProxy,
        contributor: crate::readiness::ReadinessContributor,
        metrics: Arc<crate::kafka::KafkaMetricsBridge>,
    ) -> Self {
        Self {
            readiness: std::sync::RwLock::new(KafkaReadinessSnapshot {
                client_name: client_name.to_string(),
                ready: false,
                groups: Vec::new(),
            }),
            client_name,
            proxy,
            contributor,
            metrics,
        }
    }

    /// 业务作用：返回稳定 client name。
    ///
    /// # 返回
    ///
    /// 借用与 capability 共同存活，不创建新字符串。
    pub(crate) fn client_name(&self) -> &str {
        &self.client_name
    }

    /// 业务作用：原子发布 client 最新动态就绪结论和 group 快照。
    ///
    /// # 参数
    ///
    /// - `ready`：全部受管 group 当前是否满足各自 ReadyRule。
    /// - `groups`：按 resolved group id 稳定排序的最新健康快照。
    ///
    /// # 返回
    ///
    /// 本方法无返回值；快照先写入，随后 contributor 以 Release 语义发布聚合结论。
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
        self.contributor.set_ready(ready);
    }

    /// 业务作用：返回最近一次完整发布的脱敏就绪快照。
    ///
    /// # 返回
    ///
    /// 返回拥有型副本，调用方不能借此修改组件内部状态。
    pub(crate) fn readiness(&self) -> KafkaReadinessSnapshot {
        self.readiness
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// 业务作用：尝试在 UserHook 为本 client 安装真实指标 sink。
    ///
    /// # 参数
    ///
    /// - `sink`：业务提供的无阻塞共享指标出口。
    ///
    /// # 返回
    ///
    /// 首次安装且 bridge 尚未封口时返回 true；重复安装或 Ready 已封口返回 false。
    pub(crate) fn install_metrics(&self, sink: Arc<dyn nafka::MetricsSink>) -> bool {
        self.metrics.install(sink)
    }

    /// 业务作用：在 Ready 取走全部 UserHook 定制后封口指标安装入口。
    ///
    /// # 返回
    ///
    /// 本方法无返回值；封口后既有 sink 保持有效，后续安装稳定失败。
    pub(crate) fn seal_metrics(&self) {
        self.metrics.seal();
    }
}

/// Kafka 组件对业务开放的受控发布、健康和运行控制句柄。
///
/// 句柄不公开原始 `KafkaProxy`、consumer registry、admin 写操作或 shutdown；即使业务长期
/// 持有它，容器 action 仍能独立完成两段停机。
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
    /// 业务作用：从组件私有能力根创建业务句柄。
    ///
    /// # 参数
    ///
    /// - `capability`：Start 已发布的目标 client 能力根。
    /// - `application_state`：与所属 Application 同源的状态单元。
    ///
    /// # 返回
    ///
    /// 返回不拥有任何 shutdown action 的轻量共享句柄。
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
    /// # 返回
    ///
    /// 借用与句柄共同存活，不包含 broker 或安全配置。
    pub fn client_name(&self) -> &str {
        self.capability.client_name()
    }

    /// 业务作用：返回 Kafka 能力相对于统一 Application 的当前生命周期。
    ///
    /// # 返回
    ///
    /// Starting/Ready/Draining/Closed/Failed 之一，不暴露 nafka 内部状态机。
    pub fn lifecycle(&self) -> ComponentLifecycleState {
        ComponentLifecycleState::from(self.application_state.load())
    }

    /// 业务作用：返回最近一次 Ready 门禁或运行期 monitor 发布的健康快照。
    ///
    /// # 返回
    ///
    /// 返回拥有型脱敏副本；producer-only client 的 groups 为空。
    pub fn readiness(&self) -> KafkaReadinessSnapshot {
        self.capability.readiness()
    }

    /// 业务作用：获取一个无关闭权的 producer lane。
    ///
    /// # 参数
    ///
    /// - `name`：配置中冻结的 lane 名；默认 lane 使用 `default`。
    ///
    /// # 返回
    ///
    /// Starting 或 Ready 阶段返回共享 lane；克隆不会新建底层 producer。
    ///
    /// # 错误
    ///
    /// Application 已进入 Draining/终态或 lane 不存在时返回 Kafka 组件错误。
    pub fn producer_lane(&self, name: &str) -> ApplicationResult<nafka::ProducerLane> {
        self.ensure_operation_open("producer lane access", true)?;
        self.capability
            .proxy
            .producer_lane(name)
            .map_err(|error| self.operation_error("producer lane access", error))
    }

    /// 业务作用：获取受管 client 的 admin 句柄(topic metadata 查询、按需建 topic 等)。
    ///
    /// 只读探测(list/exists/partitions)与显式 `create_if_absent` 都经它;不交出 client 关闭权。业务在
    /// UserHook 取得后即可用,真实 broker 往返(如建 topic)需在 broker 连接后(Ready)执行。
    ///
    /// # 返回
    ///
    /// Starting 或 Ready 阶段返回 admin 句柄。
    ///
    /// # 错误
    ///
    /// Application 已进入 Draining/终态时返回 Kafka 组件错误。
    pub fn admin(&self) -> ApplicationResult<nafka::KafkaAdmin> {
        self.ensure_operation_open("admin access", true)?;
        Ok(self.capability.proxy.admin())
    }

    /// 业务作用：查询一个 resolved group 的当前健康快照。
    ///
    /// # 参数
    ///
    /// - `group`：Ready 阶段解析出的最终 group id。
    ///
    /// # 返回
    ///
    /// 返回 nafka 自有的脱敏 GroupHealth 副本。
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

    /// 业务作用：查询一个 resolved group 的当前 assignment。
    ///
    /// # 参数
    ///
    /// - `group`：Ready 阶段解析出的最终 group id。
    ///
    /// # 返回
    ///
    /// 返回排序语义由 nafka 控制面的分区列表副本。
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
    /// # 参数
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    /// - `partitions`：待查询的 topic-partition 集合；所有权交给控制命令。
    ///
    /// # 返回
    ///
    /// 返回每个分区到可选下一消费 offset 的有序映射。
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
    /// # 参数
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    /// - `partition`：需要重新定位的 topic-partition。
    /// - `offset`：新的非负消费位置。
    ///
    /// # 返回
    ///
    /// owner 确认 seek 已完成时返回 `Ok(())`。
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
    /// # 参数
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    /// - `partitions`：需要重新定位的 topic-partition 集合。
    ///
    /// # 返回
    ///
    /// 全部分区完成定位时返回 `Ok(())`。
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
    /// # 参数
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    /// - `partitions`：需要重新定位的 topic-partition 集合。
    ///
    /// # 返回
    ///
    /// 全部分区完成定位时返回 `Ok(())`。
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
    /// # 参数
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    /// - `timestamps`：分区到目标毫秒时间戳的有序映射。
    ///
    /// # 返回
    ///
    /// 全部查询和定位完成时返回 `Ok(())`。
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
    /// # 参数
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    /// - `partitions`：需要恢复的 topic-partition 集合。
    ///
    /// # 返回
    ///
    /// 全部分区恢复完成时返回 `Ok(())`。
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
    /// # 参数
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    /// - `partitions`：需要暂停的 topic-partition 集合。
    ///
    /// # 返回
    ///
    /// owner 确认全部暂停时返回 `Ok(())`。
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

    /// 业务作用：移除给定分区的业务暂停原因。
    ///
    /// # 参数
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    /// - `partitions`：需要恢复的 topic-partition 集合。
    ///
    /// # 返回
    ///
    /// owner 确认全部恢复时返回 `Ok(())`。
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
    /// # 参数
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    ///
    /// # 返回
    ///
    /// owner 确认全量暂停时返回 `Ok(())`。
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
    /// # 参数
    ///
    /// - `group`：已经由本 client 启动的 resolved group id。
    ///
    /// # 返回
    ///
    /// owner 确认全量恢复时返回 `Ok(())`。
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
    /// # 参数
    ///
    /// - `group`：启动时声明为 subscribe 模式的 resolved group id。
    /// - `topics`：新的非空、无空值且不重复 topic 集合。
    ///
    /// # 返回
    ///
    /// owner 确认新订阅生效时返回 `Ok(())`。
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
    /// # 参数
    ///
    /// - `group`：启动时声明为 subscribe 模式的 resolved group id。
    ///
    /// # 返回
    ///
    /// owner 确认取消订阅时返回 `Ok(())`。
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
    /// # 返回
    ///
    /// Starting 或 Ready 阶段返回排序后的 topic 名称副本。
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
    /// # 参数
    ///
    /// - `topic`：需要读取 metadata 的非空业务 topic 名称。
    ///
    /// # 返回
    ///
    /// Starting 或 Ready 阶段返回排序后的分区编号副本。
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

    /// 业务作用：显式重建一个允许人工恢复的 consumer group。
    ///
    /// # 参数
    ///
    /// - `group`：处于可恢复故障状态的 resolved group id。
    ///
    /// # 返回
    ///
    /// owner 接受并完成重建命令时返回 `Ok(())`。
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
    /// # 参数
    ///
    /// - `operation`：不含业务输入的稳定操作名称，用于错误归因。
    /// - `allow_starting`：producer 装配传 true；只有 consumer 已 Ready 才合法的控制面传 false。
    ///
    /// # 返回
    ///
    /// 当前阶段允许操作时返回 `Ok(())`。
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
    /// # 参数
    ///
    /// - `operation`：不含 client/group/topic 输入的稳定操作名称。
    /// - `error`：nafka 返回的原始类型化错误；公开摘要不复制其可能包含的细节。
    ///
    /// # 返回
    ///
    /// 返回 component=kafka 且阶段随 Application 状态变化的错误，source 链保留原始分类。
    fn operation_error(
        &self,
        operation: &'static str,
        error: nafka::NafkaError,
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
    runtime: Arc<LogRuntimeState>,
    application_state: Arc<StateCell>,
}

#[cfg(feature = "log")]
impl LogHandle {
    /// 业务作用：从容器共享状态创建日志能力句柄。
    ///
    /// # 参数
    ///
    /// - `runtime`：日志组件独占写入、句柄只读的发布状态。
    /// - `application_state`：与 Application 共用的生命周期状态来源。
    pub(crate) fn new(runtime: Arc<LogRuntimeState>, application_state: Arc<StateCell>) -> Self {
        Self {
            runtime,
            application_state,
        }
    }

    /// 业务作用：判断容器管理的日志订阅器是否已经完成早期初始化。
    ///
    /// # 参数
    ///
    /// 本方法无参数；进入清理终态后返回 `false`。
    pub fn is_initialized(&self) -> bool {
        self.runtime.initialized.load(Ordering::Acquire)
    }

    /// 业务作用：返回与 Application 同源的组件生命周期状态。
    ///
    /// # 参数
    ///
    /// 本方法无参数；句柄不会维护第二份就绪标志。
    pub fn lifecycle(&self) -> ComponentLifecycleState {
        self.application_state.load().into()
    }
}

/// 日志组件发布初始化和关闭结果的内部状态。
#[cfg(feature = "log")]
pub(crate) struct LogRuntimeState {
    initialized: AtomicBool,
}

#[cfg(feature = "log")]
impl LogRuntimeState {
    /// 业务作用：创建尚未初始化的日志运行时状态。
    ///
    /// # 参数
    ///
    /// 本函数无参数；Bootstrap 成功后由日志组件置位。
    pub(crate) const fn new() -> Self {
        Self {
            initialized: AtomicBool::new(false),
        }
    }

    /// 业务作用：发布日志订阅器已经初始化。
    ///
    /// # 参数
    ///
    /// 本方法无参数；重复发布保持幂等。
    pub(crate) fn publish_initialized(&self) {
        self.initialized.store(true, Ordering::Release);
    }

    /// 业务作用：在日志清理动作完成后撤销运行时可用标志。
    ///
    /// # 参数
    ///
    /// 本方法无参数；它不负责实际刷盘，只反映清理结果。
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
    runtime: Arc<NacosConfigRuntimeState>,
    application_state: Arc<StateCell>,
}

#[cfg(feature = "nacos-config")]
impl NacosConfigHandle {
    /// 业务作用：从容器共享状态创建配置中心能力句柄。
    ///
    /// # 参数
    ///
    /// - `runtime`：保存底层客户端弱引用和启用状态的共享单元。
    /// - `application_state`：与 Application 共用的生命周期状态来源。
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
    /// # 参数
    ///
    /// 本方法无参数；Bootstrap 尚未读取配置时返回 `None`。
    pub fn is_enabled(&self) -> Option<bool> {
        self.runtime.client.get().map(|client| client.is_some())
    }

    /// 业务作用：使用容器已建立的底层连接拉取一份远端配置原文。
    ///
    /// # 参数
    ///
    /// - `data_id`：远端配置文档标识。
    /// - `group`：远端配置分组。
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
    /// # 参数
    ///
    /// - `data_id`：远端配置文档标识。
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
    /// # 参数
    ///
    /// 本方法无参数；句柄不会延长底层配置客户端生命周期。
    pub fn lifecycle(&self) -> ComponentLifecycleState {
        self.application_state.load().into()
    }

    /// 业务作用：升级容器发布的底层客户端弱引用。
    ///
    /// # 参数
    ///
    /// 本方法无参数；禁用、未发布或清理完成分别返回明确错误。
    fn client(&self) -> ApplicationResult<Arc<nanacos::NacosConfigClient>> {
        let state = self.application_state.load();
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
    client: OnceLock<Option<Weak<nanacos::NacosConfigClient>>>,
}

#[cfg(feature = "nacos-config")]
impl NacosConfigRuntimeState {
    /// 业务作用：创建尚未读取启用开关的配置中心状态。
    ///
    /// # 参数
    ///
    /// 本函数无参数；Bootstrap 只允许发布一次最终结果。
    pub(crate) const fn new() -> Self {
        Self {
            client: OnceLock::new(),
        }
    }

    /// 业务作用：发布配置中心已禁用，不创建底层连接。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回 `false` 表示组件重复发布。
    pub(crate) fn publish_disabled(&self) -> bool {
        self.client.set(None).is_ok()
    }

    /// 业务作用：发布底层配置客户端的弱引用。
    ///
    /// # 参数
    ///
    /// - `client`：由组件和关闭动作持有强引用的底层客户端。
    pub(crate) fn publish_client(&self, client: &Arc<nanacos::NacosConfigClient>) -> bool {
        self.client.set(Some(Arc::downgrade(client))).is_ok()
    }
}

/// 长连接组件对外开放的发送和只读运行状态句柄。
#[cfg(feature = "ws")]
#[derive(Clone)]
pub struct WsHandle {
    runtime: Arc<WsRuntimeState>,
    application_state: Arc<StateCell>,
}

#[cfg(feature = "ws")]
impl WsHandle {
    /// 业务作用：从容器共享状态创建长连接能力句柄。
    ///
    /// # 参数
    ///
    /// - `runtime`：组件发布发送器弱引用和真实监听地址的共享状态。
    /// - `application_state`：与 Application 共用的生命周期状态来源。
    pub(crate) fn new(runtime: Arc<WsRuntimeState>, application_state: Arc<StateCell>) -> Self {
        Self {
            runtime,
            application_state,
        }
    }

    /// 业务作用：返回实际绑定的 TCP 地址。
    ///
    /// # 参数
    ///
    /// 本方法无参数；绑定完成前返回 `None`。
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.runtime.addrs.get().map(|addrs| addrs.0)
    }

    /// 业务作用：返回实际绑定的独立 WebSocket 地址。
    ///
    /// # 参数
    ///
    /// 本方法无参数；未配置独立端口或绑定未完成时返回 `None`。
    pub fn websocket_addr(&self) -> Option<SocketAddr> {
        self.runtime.addrs.get().and_then(|addrs| addrs.1)
    }

    /// 业务作用：获取底层长连接库的共享广播发送器。
    ///
    /// # 参数
    ///
    /// 本方法无参数；发送器尚未发布或服务已经清理时返回明确阶段错误。
    pub fn sender(&self) -> ApplicationResult<Arc<naws::Sender>> {
        let state = self.application_state.load();
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
    /// # 参数
    ///
    /// 本方法无参数；状态不依赖发送器是否被外部暂时克隆。
    pub fn lifecycle(&self) -> ComponentLifecycleState {
        self.application_state.load().into()
    }
}

/// 长连接组件一次发布、能力句柄只读访问的内部运行时状态。
#[cfg(feature = "ws")]
pub(crate) struct WsRuntimeState {
    sender: OnceLock<Weak<naws::Sender>>,
    addrs: OnceLock<(SocketAddr, Option<SocketAddr>)>,
}

#[cfg(feature = "ws")]
impl WsRuntimeState {
    /// 业务作用：创建尚未发布发送器和监听地址的长连接状态。
    ///
    /// # 参数
    ///
    /// 本函数无参数；两个字段由 Ready 阶段依次发布。
    pub(crate) const fn new() -> Self {
        Self {
            sender: OnceLock::new(),
            addrs: OnceLock::new(),
        }
    }

    /// 业务作用：发布底层发送器弱引用，不把服务资源所有权转给 Application。
    ///
    /// # 参数
    ///
    /// - `sender`：服务对象持有的共享广播发送器。
    pub(crate) fn publish_sender(&self, sender: &Arc<naws::Sender>) -> bool {
        self.sender.set(Arc::downgrade(sender)).is_ok()
    }

    /// 业务作用：发布绑定完成后的真实监听地址。
    ///
    /// # 参数
    ///
    /// - `addr`：TCP 数据面真实地址。
    /// - `websocket`：可选的独立 WebSocket 数据面真实地址。
    pub(crate) fn publish_addrs(&self, addr: SocketAddr, websocket: Option<SocketAddr>) -> bool {
        self.addrs.set((addr, websocket)).is_ok()
    }
}

/// 服务发现组件对外开放的出站请求与注册状态句柄。
#[cfg(feature = "nacos-discovery")]
#[derive(Clone)]
pub struct NacosDiscoveryHandle {
    runtime: Arc<NacosDiscoveryRuntimeState>,
    application_state: Arc<StateCell>,
}

#[cfg(feature = "nacos-discovery")]
impl NacosDiscoveryHandle {
    /// 业务作用：从容器共享状态创建服务发现能力句柄。
    ///
    /// # 参数
    ///
    /// - `runtime`：保存发现会话弱引用的共享状态。
    /// - `application_state`：与 Application 共用的生命周期状态来源。
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
    /// # 参数
    ///
    /// 本方法无参数；Start 尚未完成或发现运行时已经关闭时返回错误。
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

    /// 业务作用：查询当前实例是否已经向注册中心发布。
    ///
    /// # 参数
    ///
    /// 本方法无参数；纯消费者模式稳定返回 `false`。
    pub async fn is_registered(&self) -> ApplicationResult<bool> {
        let session = self.session()?;
        let registered = session.lock().await.is_registered();
        Ok(registered)
    }

    /// 业务作用：查询当前配置是否要求注册本实例。
    ///
    /// # 参数
    ///
    /// 本方法无参数；该值来自组件实际使用的冻结配置。
    pub async fn wants_registration(&self) -> ApplicationResult<bool> {
        let session = self.session()?;
        let wants_registration = session.lock().await.wants_registration();
        Ok(wants_registration)
    }

    /// 业务作用：返回与 Application 同源的组件生命周期状态。
    ///
    /// # 参数
    ///
    /// 本方法无参数；它不根据注册中心网络状态推测应用就绪性。
    pub fn lifecycle(&self) -> ComponentLifecycleState {
        self.application_state.load().into()
    }

    /// 业务作用：升级组件发布的发现会话弱引用。
    ///
    /// # 参数
    ///
    /// 本方法无参数；句柄自身不会延长会话和后台任务生命周期。
    fn session(
        &self,
    ) -> ApplicationResult<Arc<tokio::sync::Mutex<rest_discovery_nacos::DiscoverySession>>> {
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

/// 服务发现组件发布会话但不转移关闭所有权的内部状态。
#[cfg(feature = "nacos-discovery")]
pub(crate) struct NacosDiscoveryRuntimeState {
    session: OnceLock<Weak<tokio::sync::Mutex<rest_discovery_nacos::DiscoverySession>>>,
}

#[cfg(feature = "nacos-discovery")]
impl NacosDiscoveryRuntimeState {
    /// 业务作用：创建尚未连接发现 provider 的运行时状态。
    ///
    /// # 参数
    ///
    /// 本函数无参数；Start 成功后只发布一次。
    pub(crate) const fn new() -> Self {
        Self {
            session: OnceLock::new(),
        }
    }

    /// 业务作用：发布组件持有的发现会话弱引用。
    ///
    /// # 参数
    ///
    /// - `session`：由组件清理动作持有强引用的发现会话。
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
    runtime: Arc<SchedulingRuntimeState>,
    application_state: Arc<StateCell>,
}

#[cfg(feature = "scheduling")]
impl SchedulingHandle {
    /// 业务作用：从容器共享状态创建调度能力句柄。
    ///
    /// # 参数
    ///
    /// - `runtime`：记录组件是否完成底层调度器启动的共享状态。
    /// - `application_state`：与 Application 共用的生命周期状态来源。
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
    /// # 参数
    ///
    /// 本方法无参数；组件未启动或已经关闭时返回阶段错误。
    pub fn runtime(&self) -> ApplicationResult<nasched::SchedulerHandle> {
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
    /// # 参数
    ///
    /// 本方法无参数；本地模式、尚未选举或已关闭时返回 `None`。
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
    /// # 参数
    ///
    /// 本方法无参数；底层运行标志只决定 `runtime()` 能否取得，不另造生命周期。
    pub fn lifecycle(&self) -> ComponentLifecycleState {
        self.application_state.load().into()
    }
}

/// 调度组件发布启动结果和可选选主句柄的内部状态。
#[cfg(feature = "scheduling")]
pub(crate) struct SchedulingRuntimeState {
    running: AtomicBool,
    #[cfg(feature = "scheduling-cluster")]
    leader: OnceLock<Weak<nadis::leader::Leader>>,
}

#[cfg(feature = "scheduling")]
impl SchedulingRuntimeState {
    /// 业务作用：创建尚未启动底层调度器的状态。
    ///
    /// # 参数
    ///
    /// 本函数无参数；Ready 成功后才发布运行标志。
    pub(crate) const fn new() -> Self {
        Self {
            running: AtomicBool::new(false),
            #[cfg(feature = "scheduling-cluster")]
            leader: OnceLock::new(),
        }
    }

    /// 业务作用：发布底层调度器已经完整启动。
    ///
    /// # 参数
    ///
    /// 本方法无参数；调用必须发生在底层启动函数成功之后。
    pub(crate) fn publish_running(&self) {
        self.running.store(true, Ordering::Release);
    }

    /// 业务作用：在调度清理动作完成后撤销运行标志。
    ///
    /// # 参数
    ///
    /// 本方法无参数；它不直接调用底层关闭函数。
    pub(crate) fn close(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// 业务作用：发布集群模式使用的只读选主弱引用。
    ///
    /// # 参数
    ///
    /// - `leader`：由调度清理动作持有并负责退位的选主对象。
    #[cfg(feature = "scheduling-cluster")]
    pub(crate) fn publish_leader(&self, leader: &Arc<nadis::leader::Leader>) -> bool {
        self.leader.set(Arc::downgrade(leader)).is_ok()
    }
}
