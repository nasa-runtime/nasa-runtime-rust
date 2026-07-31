use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, OnceLock, RwLock},
    time::{Duration, Instant},
};

use futures::{stream::FuturesUnordered, StreamExt};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    application::KafkaCustomization, capabilities::KafkaClientCapability, Application,
    ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase, ApplicationResult,
    ApplicationState, ComponentId, ReadyContext, ShutdownAction, ShutdownContext, StartContext,
};

/// Kafka Ready 归因错误相对 Application 全局 deadline 预留的固定诊断余量。
///
/// 余量只用于让带 client/group/topic 的组件错误先于 Runner 通用超时完成，不会扩大全局预算。
const READY_DIAGNOSTIC_RESERVE: Duration = Duration::from_millis(50);

/// 受管 client 是否启动 consumer registry。
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ConsumerMode {
    /// 自动属性项与 UserHook 定制共同组成一次性 registry。
    #[default]
    Collected,
    /// 只使用 producer/admin，Ready 通过 metadata 探测。
    Disabled,
}

/// 单个 consumer group 的动态就绪规则。
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ReadyRule {
    /// 完成真实 join 即可就绪，允许竞争组健康 standby 暂时没有分区。
    #[default]
    Joined,
    /// 当前 assignment 至少包含给定数量的分区。
    Assigned {
        /// 最小分区数，必须大于零。
        min_partitions: usize,
    },
    /// 当前 assignment 必须覆盖全部指定 topic。
    AssignedTopics {
        /// 非空、无空值且不重复的必需 topic 集合。
        topics: Vec<String>,
    },
}

impl ReadyRule {
    /// 校验规则不会形成永远无法满足的 Ready 条件。
    ///
    /// # 返回
    ///
    /// 参数合法时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// 最小分区为零，或 topic 集合为空、含空值/重复项时返回稳定配置摘要。
    fn validate(&self) -> Result<(), &'static str> {
        match self {
            Self::Joined => Ok(()),
            Self::Assigned { min_partitions: 0 } => {
                Err("assigned readiness requires min_partitions greater than zero")
            }
            Self::Assigned { .. } => Ok(()),
            Self::AssignedTopics { topics } => {
                let mut unique = BTreeSet::new();
                if topics.is_empty()
                    || topics.iter().any(|topic| topic.trim().is_empty())
                    || topics.iter().any(|topic| !unique.insert(topic))
                {
                    Err("assigned_topics readiness requires non-empty unique topics")
                } else {
                    Ok(())
                }
            }
        }
    }

    /// 投影为 nafka 启动期 broker Ready 要求。
    ///
    /// # 返回
    ///
    /// 返回拥有型规则，供多个 group 共享同一绝对 deadline 并发等待。
    fn requirement(&self) -> nafka::ReadyRequirement {
        match self {
            Self::Joined => nafka::ReadyRequirement::Joined,
            Self::Assigned { min_partitions } => nafka::ReadyRequirement::Assigned {
                min_partitions: *min_partitions,
            },
            Self::AssignedTopics { topics } => {
                nafka::ReadyRequirement::AssignedTopics(topics.clone())
            }
        }
    }

    /// 判断运行期健康快照当前是否仍满足本规则。
    ///
    /// # 参数
    ///
    /// - `health`：同一 resolved group 的最新脱敏健康快照。
    ///
    /// # 返回
    ///
    /// 仅 Running/Retrying 且 assignment 事实满足规则时返回 true；软降级和 rebalance 返回 false。
    fn is_satisfied(&self, health: &nafka::GroupHealth) -> bool {
        if !matches!(
            health.state,
            nafka::GroupState::Running | nafka::GroupState::Retrying
        ) || health.ready_assignment_epoch.is_none()
        {
            return false;
        }
        match self {
            Self::Joined => true,
            Self::Assigned { min_partitions } => health.assignment.len() >= *min_partitions,
            Self::AssignedTopics { topics } => topics.iter().all(|required| {
                health
                    .assignment
                    .iter()
                    .any(|assigned| assigned.topic == *required)
            }),
        }
    }
}

/// 一个 client 的 consumer 与 producer-only Ready 规则。
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct KafkaReadinessOptions {
    /// 未被 group map 覆盖时使用的规则。
    default: ReadyRule,
    /// resolved group id 到专属规则的稳定映射。
    groups: BTreeMap<String, ReadyRule>,
    /// producer-only client 可选的指定 topic metadata 探测目标。
    producer_probe_topic: Option<String>,
}

/// napp 从 Kafka 配置中剥离的容器编排字段。
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct KafkaContainerOptions {
    /// 是否自动启动 consumer registry。
    consumers: ConsumerMode,
    /// 运行期 group 健康轮询周期，单位毫秒，合法范围 100..=10_000。
    monitor_interval_ms: u64,
    /// broker 启动门禁与动态健康规则。
    readiness: KafkaReadinessOptions,
}

impl Default for KafkaContainerOptions {
    /// 返回 consumer 自动收集、500ms 健康轮询和 Joined Ready 的安全默认值。
    ///
    /// # 返回
    ///
    /// 返回只影响 Application 编排、不改变 nafka 数据面默认值的配置。
    fn default() -> Self {
        Self {
            consumers: ConsumerMode::Collected,
            monitor_interval_ms: 500,
            readiness: KafkaReadinessOptions::default(),
        }
    }
}

impl KafkaContainerOptions {
    /// 校验容器专属字段及其交叉约束。
    ///
    /// # 返回
    ///
    /// 全部字段合法且与 consumer mode 一致时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// 监控周期越界、规则非法、group key 为空或 producer probe 与 consumer 模式冲突时返回摘要。
    fn validate(&self) -> Result<(), &'static str> {
        if !(100..=10_000).contains(&self.monitor_interval_ms) {
            return Err("container.monitor_interval_ms must be within 100..=10000");
        }
        self.readiness.default.validate()?;
        for (group, rule) in &self.readiness.groups {
            if group.trim().is_empty() {
                return Err("container.readiness.groups cannot contain an empty group id");
            }
            rule.validate()?;
        }
        if let Some(topic) = self.readiness.producer_probe_topic.as_deref() {
            if topic.trim().is_empty() {
                return Err("container.readiness.producer_probe_topic cannot be empty");
            }
            if self.consumers != ConsumerMode::Disabled {
                return Err("producer_probe_topic requires consumers=disabled");
            }
        }
        if self.consumers == ConsumerMode::Disabled && !self.readiness.groups.is_empty() {
            return Err("consumers=disabled cannot declare group readiness overrides");
        }
        Ok(())
    }
}

/// 解析完成后供 Start 使用的单一 client 内部表征。
#[derive(Clone)]
struct KafkaClientConfig {
    /// 已剥离 container 且通过 nafka 校验的数据面配置。
    config: nafka::KafkaConfig,
    /// Application 专属编排选项。
    container: KafkaContainerOptions,
}

/// Start 已构造并由 KafkaComponent 独占推进的单个 client 计划。
struct KafkaClientPlan {
    /// consumer、monitor 与 producer probe 的编排选项。
    container: KafkaContainerOptions,
    /// 容器私有的原始运行时句柄。
    proxy: nafka::KafkaProxy,
    /// 业务只能经 KafkaHandle 访问的受控能力根。
    capability: Arc<KafkaClientCapability>,
    /// Ready 后冻结的 resolved group 与规则列表。
    groups: Vec<(String, ReadyRule)>,
}

/// 指标桥内部可变状态；业务私有 sink 只允许 UserHook 安装一次，Ready 后永久封口。
struct KafkaMetricsBridgeState {
    /// 业务经 `install` 追加的私有 sink;`None` 时指标仍恒进框架默认 sink(统一 hub),不丢弃。
    sink: Option<Arc<dyn nafka::MetricsSink>>,
    /// Ready 取走定制后为 true，阻止晚到安装被静默忽略。
    sealed: bool,
}

/// Start 与 UserHook 之间传递指标 sink 的 crate 私有时序桥。
pub(crate) struct KafkaMetricsBridge {
    /// 框架默认 sink:恒接收 nafka 上报并转发到进程级统一 `MetricHub`。
    ///
    /// 与业务 `install` 的可选 sink 是 fan-out 关系:业务无需再手工装 sink 也能让指标进 hub;
    /// 若业务另装私有 sink(如自有埋点系统),两者同时收到。
    default_sink: Arc<dyn nafka::MetricsSink>,
    /// 读多写一次的业务 sink 状态；回调只短暂持读锁并先克隆 Arc 再调用业务实现。
    state: RwLock<KafkaMetricsBridgeState>,
}

impl KafkaMetricsBridge {
    /// 创建以 `default_sink` 为框架默认出口的桥。
    ///
    /// # 参数
    ///
    /// - `default_sink`:框架装配的统一 hub 适配器,恒接收 nafka 上报。
    ///
    /// # 返回
    ///
    /// 新桥立即把指标转发到 `default_sink`;业务可再经 `install` 追加私有 sink。
    fn new(default_sink: Arc<dyn nafka::MetricsSink>) -> Self {
        Self {
            default_sink,
            state: RwLock::new(KafkaMetricsBridgeState {
                sink: None,
                sealed: false,
            }),
        }
    }

    /// 尝试安装本 client 唯一的真实指标 sink。
    ///
    /// # 参数
    ///
    /// - `sink`：业务提供的无阻塞、不可 panic 指标出口。
    ///
    /// # 返回
    ///
    /// 首次安装且尚未封口时返回 true；重复或晚到安装返回 false。
    pub(crate) fn install(&self, sink: Arc<dyn nafka::MetricsSink>) -> bool {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.sealed || state.sink.is_some() {
            return false;
        }
        state.sink = Some(sink);
        true
    }

    /// 永久关闭指标安装入口，保留已安装 sink 继续接收运行期指标。
    ///
    /// # 返回
    ///
    /// 本方法无返回值；重复封口为幂等操作。
    pub(crate) fn seal(&self) {
        self.state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sealed = true;
    }

    /// 克隆当前 sink，使业务回调不在 bridge 锁内执行。
    ///
    /// # 返回
    ///
    /// 未安装时返回 None；已安装时返回共享 sink 的 Arc 副本。
    fn sink(&self) -> Option<Arc<dyn nafka::MetricsSink>> {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sink
            .clone()
    }
}

impl nafka::MetricsSink for KafkaMetricsBridge {
    /// 把 counter 转发给 UserHook 安装的 sink；未安装时安全丢弃。
    ///
    /// # 参数
    ///
    /// - `name`：nafka 固定指标名。
    /// - `delta`：本次单调增量。
    /// - `labels`：nafka 提供的低基数借用标签。
    fn counter(&self, name: &'static str, delta: u64, labels: nafka::MetricLabels<'_>) {
        self.default_sink.counter(name, delta, labels);
        if let Some(sink) = self.sink() {
            sink.counter(name, delta, labels);
        }
    }

    /// 把 gauge 转发给 UserHook 安装的 sink；未安装时安全丢弃。
    ///
    /// # 参数
    ///
    /// - `name`：nafka 固定指标名。
    /// - `value`：当前瞬时值。
    /// - `labels`：nafka 提供的低基数借用标签。
    fn gauge(&self, name: &'static str, value: i64, labels: nafka::MetricLabels<'_>) {
        self.default_sink.gauge(name, value, labels);
        if let Some(sink) = self.sink() {
            sink.gauge(name, value, labels);
        }
    }
}

/// ApplicationInner 持有的 Kafka client 发布与 UserHook 定制状态。
pub(crate) struct KafkaRuntimeState {
    /// Start 完整成功后一次性发布的有序 client 能力表。
    clients: OnceLock<BTreeMap<String, Arc<KafkaClientCapability>>>,
    /// UserHook 可追加、Ready 一次性取走并封口的 consumer 定制队列。
    customizations: Mutex<Option<BTreeMap<String, Vec<KafkaCustomization>>>>,
}

impl KafkaRuntimeState {
    /// 创建尚未发布 client 且定制入口开放的运行时状态。
    ///
    /// # 返回
    ///
    /// 返回每个 Application 独占的一次性状态容器。
    pub(crate) fn new() -> Self {
        Self {
            clients: OnceLock::new(),
            customizations: Mutex::new(Some(BTreeMap::new())),
        }
    }

    /// 一次性发布完整有序 client 能力表。
    ///
    /// # 参数
    ///
    /// - `clients`：Start 已为每个条目激活 final shutdown action 的完整表。
    ///
    /// # 返回
    ///
    /// 首次发布返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// 表为空或重复发布时返回 Kafka Start 错误。
    pub(crate) fn publish_clients(
        &self,
        clients: BTreeMap<String, Arc<KafkaClientCapability>>,
    ) -> ApplicationResult<()> {
        if clients.is_empty() {
            return Err(kafka_error(
                ApplicationPhase::Start,
                "kafka client table cannot be empty",
            ));
        }
        self.clients
            .set(clients)
            .map_err(|_| kafka_error(ApplicationPhase::Start, "kafka clients already published"))
    }

    /// 按稳定名称克隆一个私有能力根。
    ///
    /// # 参数
    ///
    /// - `name`：单 client 的 client_name 或多 client map key。
    ///
    /// # 返回
    ///
    /// Start 已发布且名称存在时返回 Arc 副本，否则返回 None。
    pub(crate) fn client(&self, name: &str) -> Option<Arc<KafkaClientCapability>> {
        self.clients
            .get()
            .and_then(|clients| clients.get(name))
            .cloned()
    }

    /// 在 UserHook 为目标 client 追加一次 consumer builder 定制。
    ///
    /// # 参数
    ///
    /// - `name`：已经发布的目标 client name。
    /// - `customize`：拥有业务依赖的一次性 builder 转换闭包。
    ///
    /// # 返回
    ///
    /// 定制进入目标 client 有序队列时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// 名称不存在或 Ready 已取走队列时返回阶段错误。
    pub(crate) fn push_customization(
        &self,
        name: &str,
        customize: KafkaCustomization,
    ) -> ApplicationResult<()> {
        if self.client(name).is_none() {
            return Err(kafka_error(
                ApplicationPhase::UserHook,
                format!("kafka client `{name}` is not configured"),
            ));
        }
        let mut slot = self
            .customizations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(customizations) = slot.as_mut() else {
            return Err(kafka_error(
                ApplicationPhase::Ready,
                "kafka consumer configuration is already sealed",
            ));
        };
        customizations
            .entry(name.to_owned())
            .or_default()
            .push(customize);
        Ok(())
    }

    /// 为目标 client 安装一次真实指标 sink。
    ///
    /// # 参数
    ///
    /// - `name`：已经发布的目标 client name。
    /// - `sink`：业务提供的无阻塞共享指标出口。
    ///
    /// # 返回
    ///
    /// 首次安装成功时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// client 不存在、重复安装或 Ready 已封口时返回错误。
    pub(crate) fn install_metrics(
        &self,
        name: &str,
        sink: Arc<dyn nafka::MetricsSink>,
    ) -> ApplicationResult<()> {
        let capability = self.client(name).ok_or_else(|| {
            kafka_error(
                ApplicationPhase::UserHook,
                format!("kafka client `{name}` is not configured"),
            )
        })?;
        if capability.install_metrics(sink) {
            Ok(())
        } else {
            Err(kafka_error(
                ApplicationPhase::UserHook,
                format!("kafka metrics for client `{name}` are already installed or sealed"),
            ))
        }
    }

    /// 取走全部 consumer 定制并永久关闭 UserHook 配置入口。
    ///
    /// # 返回
    ///
    /// 首次调用返回按 client name 排序的完整队列；重复调用返回空表。
    pub(crate) fn take_customizations(&self) -> BTreeMap<String, Vec<KafkaCustomization>> {
        self.customizations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_default()
    }
}

/// `#[application("kafka")]` 对应的三阶段生命周期组件。
pub(crate) struct KafkaComponent {
    /// Start 构造、Ready 启动并由 monitor 只读观察的有序 client 表。
    clients: BTreeMap<String, KafkaClientPlan>,
    /// Ready 成功后交给 Runner 的唯一关键健康任务。
    critical_task: Option<ApplicationFuture<'static>>,
}

impl KafkaComponent {
    /// 创建尚未产生任何 Kafka 副作用的组件。
    ///
    /// # 返回
    ///
    /// 返回可由 Runner 依次推进 Start 和 Ready 的空组件。
    pub(crate) fn new() -> Self {
        Self {
            clients: BTreeMap::new(),
            critical_task: None,
        }
    }
}

impl ApplicationComponent for KafkaComponent {
    /// 返回稳定 Kafka 组件身份。
    ///
    /// # 返回
    ///
    /// 始终返回 `ComponentId::Kafka`。
    fn id(&self) -> ComponentId {
        ComponentId::Kafka
    }

    /// 使用最终初始配置构造全部本地 client，并为每个成功 client 立即激活 final action。
    ///
    /// # 参数
    ///
    /// - `context`：提供最终配置、共享 Application 和 Start 层 action 栈。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let configured = parse_kafka_clients(
                context.application().config().value(),
                ApplicationPhase::Start,
            )?;
            let application = context.application().clone();
            let runtime = application.kafka_runtime();
            let mut capabilities = BTreeMap::new();

            // 把 nafka 域接到进程级统一 hub。descriptor 一次性注册(幂等,
            // register 对完全一致的 descriptor 幂等);hub 适配器作为每个桥的框架默认 sink,取代
            // 业务手工 install。冲突审计失败视为启动期配置错误(同名 descriptor 语义不一致)。
            let hub = application.metrics_hub();
            crate::metrics::register_nafka_descriptors(&hub).map_err(|conflict| {
                kafka_error(
                    ApplicationPhase::Start,
                    format!(
                        "nafka metric descriptor `{}` conflicts with an existing registration",
                        conflict.name
                    ),
                )
            })?;
            let hub_sink: Arc<dyn nafka::MetricsSink> = Arc::new(
                crate::metrics::NafkaMetricSinkAdapter::new(Arc::clone(&hub)),
            );

            for (name, client) in configured {
                let bridge = Arc::new(KafkaMetricsBridge::new(Arc::clone(&hub_sink)));
                let metrics: Arc<dyn nafka::MetricsSink> = bridge.clone();
                #[cfg(feature = "telemetry")]
                let span_recorder = application.span_recorder();
                #[cfg(not(feature = "telemetry"))]
                let span_recorder = None;
                let proxy = nafka::KafkaProxy::connect_with_observability(
                    client.config,
                    metrics,
                    span_recorder,
                )
                .map_err(|error| {
                    kafka_source_error(
                        ApplicationPhase::Start,
                        format!("kafka client `{name}` local construction failed"),
                        error,
                    )
                })?;

                // connect 成功后先压 final action，再做任何可能失败的发布；否则后续 readiness
                // 注册或 OnceLock 发布失败会只靠 Drop，丢失 producer flush。
                context.activate(Box::new(KafkaFinalShutdown {
                    client_name: name.clone(),
                    proxy: proxy.clone(),
                }));
                let contributor = application.register_readiness(
                    ComponentId::Kafka,
                    Arc::<str>::from(format!("kafka:{name}")),
                    // kafka 由自身健康 monitor 通过 set_ready 周期发布,沿用关键、立即生效策略;
                    // group Ready/断连由 monitor 翻转,不用 registry stale 兜底。
                    crate::readiness::ReadinessPolicy::critical_immediate(),
                )?;
                let capability = Arc::new(KafkaClientCapability::new(
                    Arc::from(name.as_str()),
                    proxy.clone(),
                    contributor,
                    bridge,
                ));
                capabilities.insert(name.clone(), Arc::clone(&capability));
                self.clients.insert(
                    name,
                    KafkaClientPlan {
                        container: client.container,
                        proxy,
                        capability,
                        groups: Vec::new(),
                    },
                );
            }
            runtime.publish_clients(capabilities)?;
            Ok(())
        })
    }

    /// 在 UserHook 封口后启动 consumer、等待真实 broker Ready 并创建运行期健康 monitor。
    ///
    /// # 参数
    ///
    /// - `context`：提供 UserHook 后的 Application、共享启动 deadline 和 Ready 层 action 栈。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            validate_collected_targets(&self.clients)?;
            let application = context.application().clone();
            let deadline = kafka_ready_deadline(context.deadline())?;

            // Phase 1:在启动任何 consumer 之前,对**所有** client 执行只读 broker metadata 探针
            // (consumer client 也探,不再只探 producer-only)。只读、无 owner 副作用,失败无需回滚;
            // 全部失败按 client 名稳定排序后主错误报第一个。metadata 成功**不**冒充 consumer Ready——
            // consumer 仍须在 Phase 3 等待真实 group join/assignment。
            probe_all_clients_metadata(&self.clients, deadline).await?;

            let runtime = application.kafka_runtime();
            let mut customizations = runtime.take_customizations();
            let consumer_action_clients = Arc::new(Mutex::new(Vec::new()));
            let mut consumer_action_active = false;

            // Phase 2:metadata 探针通过后再启动 consumer(建立 owner 副作用),按现有回滚纪律压栈排空 action。
            for (name, plan) in &mut self.clients {
                plan.capability.seal_metrics();
                match plan.container.consumers {
                    ConsumerMode::Collected => {
                        let has_static = nafka::COLLECTED_CONSUMERS
                            .iter()
                            .any(|entry| entry.client == name);
                        let mut builder = plan.proxy.consumers();
                        if has_static {
                            builder = builder.with_collected_for(name).map_err(|error| {
                                kafka_source_error(
                                    ApplicationPhase::Ready,
                                    format!(
                                        "kafka client `{name}` static consumer collection failed"
                                    ),
                                    error,
                                )
                            })?;
                        }
                        for customize in customizations.remove(name).unwrap_or_default() {
                            builder = customize(builder).map_err(|error| {
                                kafka_source_error(
                                    ApplicationPhase::Ready,
                                    format!("kafka client `{name}` consumer customization failed"),
                                    error,
                                )
                            })?;
                        }
                        let consumer = builder.start().await.map_err(|error| {
                            kafka_source_error(
                                ApplicationPhase::Ready,
                                format!("kafka client `{name}` consumer start failed"),
                                error,
                            )
                        })?;
                        let resolved_groups = consumer.groups().to_vec();
                        // start 成功已经产生 owner 副作用，必须先把 client 加入共享排空
                        // action，再做任何可能失败的 ReadyRule 解析；否则拼错 override 会让
                        // 已启动 owner 越过 Ready 回滚层，只能拖到 final shutdown 才被发现。
                        consumer_action_clients
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .push((
                                name.clone(),
                                plan.proxy.clone(),
                                Arc::clone(&plan.capability),
                            ));
                        if !consumer_action_active {
                            context.activate(Box::new(KafkaConsumersShutdown {
                                clients: Arc::clone(&consumer_action_clients),
                            }));
                            consumer_action_active = true;
                        }
                        plan.groups = resolve_group_rules(name, &resolved_groups, &plan.container)?;
                    }
                    ConsumerMode::Disabled => {
                        if customizations.contains_key(name) {
                            return Err(kafka_error(
                                ApplicationPhase::Ready,
                                format!(
                                    "kafka client `{name}` has consumer customizations but consumers are disabled"
                                ),
                            ));
                        }
                    }
                }
            }
            if let Some((name, _)) = customizations.into_iter().next() {
                return Err(kafka_error(
                    ApplicationPhase::Ready,
                    format!("kafka client `{name}` is not configured"),
                ));
            }

            // Phase 3:consumer client 等待真实 group join/assignment 满足 ReadyRequirement;producer-only
            // client 的就绪已由 Phase 1 metadata 探针确认,此处不再重复探测。group 超时构建含每 group 快照
            // (state/assignment/ready_epoch/last_error)的富诊断,无需 librdkafka 旁路日志即可定位。
            let ready_budget_ms = deadline
                .saturating_duration_since(Instant::now())
                .as_millis();
            let mut waits: FuturesUnordered<
                ApplicationFuture<'static, (String, Vec<nafka::GroupHealth>)>,
            > = FuturesUnordered::new();
            for (name, plan) in &self.clients {
                let name = name.clone();
                let proxy = plan.proxy.clone();
                let groups = plan.groups.clone();
                waits.push(Box::pin(async move {
                    if groups.is_empty() {
                        // producer-only:Phase 1 已确认 broker 可达,直接就绪,不重复探测。
                        return Ok((name, Vec::new()));
                    }
                    let requirements = groups
                        .iter()
                        .map(|(group, rule)| (group.clone(), rule.requirement()))
                        .collect();
                    proxy
                        .await_groups_ready(requirements, deadline)
                        .await
                        .map_err(|error| {
                            group_ready_error(&name, &groups, ready_budget_ms, error)
                        })?;
                    let mut health = Vec::with_capacity(groups.len());
                    for (group, _) in &groups {
                        health.push(proxy.group_health(group).await.map_err(|error| {
                            kafka_source_error(
                                ApplicationPhase::Ready,
                                format!("kafka client `{name}` health snapshot failed"),
                                error,
                            )
                        })?);
                    }
                    Ok((name, health))
                }));
            }

            while let Some(result) = waits.next().await {
                let (name, health) = result?;
                if let Some(plan) = self.clients.get(&name) {
                    plan.capability.publish_readiness(true, health);
                }
            }

            let monitor_clients = self
                .clients
                .iter()
                .map(|(name, plan)| KafkaMonitorClient {
                    client_name: name.clone(),
                    proxy: plan.proxy.clone(),
                    capability: Arc::clone(&plan.capability),
                    monitor_interval: Duration::from_millis(plan.container.monitor_interval_ms),
                    producer_probe_topic: plan.container.readiness.producer_probe_topic.clone(),
                    groups: plan.groups.clone(),
                })
                .collect();
            self.critical_task = Some(Box::pin(run_kafka_monitor(application, monitor_clients)));
            Ok(())
        })
    }

    /// 取出唯一 Kafka 健康 monitor，交由 Runner 按关键任务监督。
    ///
    /// # 返回
    ///
    /// Ready 成功后首次调用返回任务；重复调用返回 None。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("kafka-health-monitor", task))
    }
}

/// Ready action 共享的已启动 consumer client 列表。
type ConsumerShutdownClients =
    Arc<Mutex<Vec<(String, nafka::KafkaProxy, Arc<KafkaClientCapability>)>>>;

/// Ready 层先于 UserTasks 执行的多 client consumer 排空 action。
struct KafkaConsumersShutdown {
    /// 每个 consumer start 成功后立即追加的稳定 client 列表。
    clients: ConsumerShutdownClients,
}

impl ShutdownAction for KafkaConsumersShutdown {
    /// 返回稳定 action 名称。
    ///
    /// # 返回
    ///
    /// 名称不包含 client 或配置值。
    fn label(&self) -> &'static str {
        "kafka-consumers"
    }

    /// 在共享全局 deadline 内并发排空全部已启动 consumer。
    ///
    /// # 参数
    ///
    /// - `context`：Runner 提供的首次停机原因和绝对截止时刻。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        let clients = self
            .clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Box::pin(async move {
            let results =
                futures::future::join_all(clients.iter().map(|(name, proxy, capability)| {
                    let name = name.clone();
                    let proxy = proxy.clone();
                    let capability = Arc::clone(capability);
                    async move {
                        let result = proxy.stop_consumers_until(context.deadline()).await;
                        capability.publish_readiness(false, Vec::new());
                        (name, result)
                    }
                }))
                .await;
            let mut failures = results
                .into_iter()
                .filter_map(|(name, result)| result.err().map(|error| (name, error)))
                .collect::<Vec<_>>();
            failures.sort_by(|left, right| left.0.cmp(&right.0));
            if let Some((_, source)) = failures.into_iter().next() {
                Err(kafka_source_error(
                    ApplicationPhase::Stopping,
                    "one or more kafka consumer clients did not drain",
                    source,
                ))
            } else {
                Ok(())
            }
        })
    }
}

/// Start 层在 UserTasks 和业务资源之后执行的单 client 最终关闭 action。
struct KafkaFinalShutdown {
    /// 只用于稳定错误归因的 client name。
    client_name: String,
    /// 拥有最终 producer/admin 关闭权的原始运行时句柄。
    proxy: nafka::KafkaProxy,
}

impl ShutdownAction for KafkaFinalShutdown {
    /// 返回稳定 action 名称。
    ///
    /// # 返回
    ///
    /// 名称不包含 client 或配置值。
    fn label(&self) -> &'static str {
        "kafka-final"
    }

    /// 在全局剩余预算内完成本 client 的 lane/admin 最终关闭。
    ///
    /// # 参数
    ///
    /// - `context`：Runner 提供的首次停机原因和绝对截止时刻。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            self.proxy
                .shutdown_until(context.deadline())
                .await
                .map_err(|error| {
                    kafka_source_error(
                        ApplicationPhase::Stopping,
                        format!("kafka client `{}` final shutdown failed", self.client_name),
                        error,
                    )
                })
        })
    }
}

/// 运行期健康 monitor 所需的单 client 不可变输入。
struct KafkaMonitorClient {
    /// 稳定 client name，只用于低基数错误归因。
    client_name: String,
    /// 只读健康查询使用的原始运行时句柄。
    proxy: nafka::KafkaProxy,
    /// 发布脱敏快照和全局 readiness 的受控能力根。
    capability: Arc<KafkaClientCapability>,
    /// 本 client 配置的轮询周期。
    monitor_interval: Duration,
    /// producer-only client 的可选指定 metadata 探测 topic。
    producer_probe_topic: Option<String>,
    /// Ready 冻结的 resolved group 与规则列表；producer-only 为空。
    groups: Vec<(String, ReadyRule)>,
}

/// 按全部 client 的最小周期刷新动态 readiness，并把硬故障升级为关键任务失败。
///
/// # 参数
///
/// - `application`：用于区分运行期意外退出与 Runner 已先发布的主动 Stopping。
/// - `clients`：Ready 冻结的全部 client、group 与规则。
///
/// # 返回
///
/// Application 进入 Stopping 时正常返回；Crashed/意外 Stopped/健康查询失败时返回 Kafka 错误。
async fn run_kafka_monitor(
    application: Application,
    clients: Vec<KafkaMonitorClient>,
) -> ApplicationResult<()> {
    let idle_interval = clients
        .iter()
        .map(|client| client.monitor_interval)
        .min()
        .unwrap_or(Duration::from_millis(500));
    let mut next_checks = vec![tokio::time::Instant::now(); clients.len()];
    loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                for client in &clients {
                    client.capability.publish_readiness(false, Vec::new());
                }
                return Ok(());
            }
            ApplicationState::Starting => {
                tokio::time::sleep(idle_interval).await;
                continue;
            }
            ApplicationState::Ready => {}
        }

        let now = tokio::time::Instant::now();
        for (index, client) in clients.iter().enumerate() {
            if next_checks[index] > now {
                continue;
            }
            if client.groups.is_empty() {
                // producer-only client 没有 GroupHealth 可读，必须继续用只读 metadata
                // 刷新动态 readiness；否则 broker 在初次 Ready 后失联会永久保留假就绪。
                let deadline = Instant::now() + client.monitor_interval;
                let ready = probe_client_metadata(
                    &client.client_name,
                    &client.proxy,
                    client.producer_probe_topic.as_deref(),
                    deadline,
                )
                .await
                .is_ok();
                client.capability.publish_readiness(ready, Vec::new());
                next_checks[index] = tokio::time::Instant::now() + client.monitor_interval;
                continue;
            }
            let mut all_ready = true;
            let mut snapshots = Vec::with_capacity(client.groups.len());
            for (group, rule) in &client.groups {
                let health = match client.proxy.group_health(group).await {
                    Ok(health) => health,
                    Err(error) => {
                        client
                            .capability
                            .publish_readiness(false, snapshots.clone());
                        // Runner 会先发布 Stopping 再执行 consumer action。查询与该状态切换
                        // 竞争时按主动排空正常退出，不能把预期的 group 消失升级成运行期崩溃。
                        if application.state() != ApplicationState::Ready {
                            return Ok(());
                        }
                        return Err(kafka_source_error(
                            ApplicationPhase::Running,
                            format!(
                                "kafka client `{}` group health query failed",
                                client.client_name
                            ),
                            error,
                        ));
                    }
                };
                let hard_failure = matches!(
                    health.state,
                    nafka::GroupState::Crashed
                        | nafka::GroupState::Stopping
                        | nafka::GroupState::Stopped
                );
                all_ready &= rule.is_satisfied(&health);
                snapshots.push(health);
                if hard_failure {
                    client
                        .capability
                        .publish_readiness(false, snapshots.clone());
                    if application.state() != ApplicationState::Ready {
                        return Ok(());
                    }
                    return Err(kafka_error(
                        ApplicationPhase::Running,
                        format!(
                            "kafka client `{}` has an unexpectedly stopped consumer group",
                            client.client_name
                        ),
                    ));
                }
            }
            client.capability.publish_readiness(all_ready, snapshots);
            next_checks[index] = tokio::time::Instant::now() + client.monitor_interval;
        }
        let next_check = next_checks
            .iter()
            .copied()
            .min()
            .unwrap_or_else(|| tokio::time::Instant::now() + idle_interval);
        tokio::time::sleep_until(next_check).await;
    }
}

/// 在产生连接副作用前校验声明中的 `kafka`/`kafkas` 配置。
///
/// # 参数
///
/// - `tree`：已经合并、插值但尚未发布的完整配置候选树。
/// - `phase`：启动或热刷新本轮校验所属阶段。
///
/// # 返回
///
/// 配置可归一成非空有序 client 表时返回 `Ok(())`。
///
/// # 错误
///
/// 根冲突、未知字段、client 身份或 nafka/container 不变量非法时返回 Kafka 组件错误。
pub(crate) fn validate_kafka_sections(
    tree: &Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    parse_kafka_clients(tree, phase).map(|_| ())
}

/// 把单/多 client 配置根归一成唯一有序内部表征。
///
/// # 参数
///
/// - `tree`：包含 `kafka` 或 `kafkas` 的完整配置树。
/// - `phase`：错误归因使用的生命周期阶段。
///
/// # 返回
///
/// 返回按最终 client name 排序的非空配置表。
///
/// # 错误
///
/// 两根同时存在、两根都缺失、根形状非法或任一 client 解析失败时返回错误，不发布半份结果。
fn parse_kafka_clients(
    tree: &Value,
    phase: ApplicationPhase,
) -> ApplicationResult<BTreeMap<String, KafkaClientConfig>> {
    match (tree.get("kafka"), tree.get("kafkas")) {
        (Some(_), Some(_)) => Err(kafka_error(
            phase,
            "config roots `kafka` and `kafkas` are mutually exclusive",
        )),
        (None, None) => Err(kafka_error(
            phase,
            "kafka component requires config root `kafka` or `kafkas`",
        )),
        (Some(value), None) => {
            let client = parse_kafka_client(value, None, phase)?;
            let mut clients = BTreeMap::new();
            clients.insert(client.config.client_name.clone(), client);
            Ok(clients)
        }
        (None, Some(value)) => {
            let entries = value.as_object().ok_or_else(|| {
                kafka_error(phase, "config root `kafkas` must be a non-empty object")
            })?;
            if entries.is_empty() {
                return Err(kafka_error(
                    phase,
                    "config root `kafkas` must be a non-empty object",
                ));
            }
            let mut clients = BTreeMap::new();
            for (name, value) in entries {
                if name.trim().is_empty() {
                    return Err(kafka_error(phase, "kafkas client name cannot be empty"));
                }
                let client = parse_kafka_client(value, Some(name), phase)?;
                clients.insert(name.clone(), client);
            }
            Ok(clients)
        }
    }
}

/// 解析一个严格受管 client 节点并剥离 container 字段。
///
/// # 参数
///
/// - `value`：单 client 的对象节点。
/// - `authoritative_name`：多 client map key；存在时覆盖缺省 client_name 并拒绝显式不一致。
/// - `phase`：错误归因使用的生命周期阶段。
///
/// # 返回
///
/// 返回通过 KafkaConfig::validate 和容器规则校验的拥有型配置。
///
/// # 错误
///
/// 节点不是对象、包含未知顶层键、反序列化失败或任一不变量非法时返回错误。
fn parse_kafka_client(
    value: &Value,
    authoritative_name: Option<&str>,
    phase: ApplicationPhase,
) -> ApplicationResult<KafkaClientConfig> {
    let mut fields = value
        .as_object()
        .cloned()
        .ok_or_else(|| kafka_error(phase, "each kafka client config must be an object"))?;
    for field in fields.keys() {
        if field != "container" && !nafka::KAFKA_CONFIG_FIELDS.contains(&field.as_str()) {
            return Err(kafka_error(
                phase,
                format!("unknown managed kafka client field `{field}`"),
            ));
        }
    }

    let container = match fields.remove("container") {
        Some(value) => serde_json::from_value(value).map_err(|error| {
            kafka_source_error(phase, "cannot deserialize kafka container options", error)
        })?,
        None => KafkaContainerOptions::default(),
    };
    if let Some(name) = authoritative_name {
        match fields.get("client_name") {
            Some(Value::String(configured)) if configured == name => {}
            Some(Value::String(_)) => {
                return Err(kafka_error(
                    phase,
                    format!("kafkas client `{name}` has a mismatched client_name"),
                ));
            }
            Some(_) => {
                return Err(kafka_error(
                    phase,
                    format!("kafkas client `{name}` has a non-string client_name"),
                ));
            }
            None => {
                fields.insert("client_name".into(), Value::String(name.to_owned()));
            }
        }
    }

    let config: nafka::KafkaConfig =
        serde_json::from_value(Value::Object(fields)).map_err(|error| {
            kafka_source_error(phase, "cannot deserialize managed kafka config", error)
        })?;
    config.validate().map_err(|error| {
        kafka_source_error(phase, "managed kafka config validation failed", error)
    })?;
    container
        .validate()
        .map_err(|message| kafka_error(phase, message))?;
    Ok(KafkaClientConfig { config, container })
}

/// 校验所有静态属性 consumer 都指向已配置且允许消费的 client。
///
/// # 参数
///
/// - `clients`：Start 已构造的完整受管 client 表。
///
/// # 返回
///
/// 每个收集项都能被唯一 client 接管时返回 `Ok(())`。
///
/// # 错误
///
/// 收集项指向未知或 producer-only client 时返回 Ready 错误，避免静默漏消费。
fn validate_collected_targets(
    clients: &BTreeMap<String, KafkaClientPlan>,
) -> ApplicationResult<()> {
    for collected in nafka::COLLECTED_CONSUMERS {
        let Some(client) = clients.get(collected.client) else {
            return Err(kafka_error(
                ApplicationPhase::Ready,
                format!(
                    "collected kafka consumer `{}` targets an unconfigured client",
                    collected.id
                ),
            ));
        };
        if client.container.consumers == ConsumerMode::Disabled {
            return Err(kafka_error(
                ApplicationPhase::Ready,
                format!(
                    "collected kafka consumer `{}` targets a producer-only client",
                    collected.id
                ),
            ));
        }
    }
    Ok(())
}

/// 为实际启动的 resolved group 冻结运行期 ReadyRule 列表。
///
/// # 参数
///
/// - `client_name`：只用于稳定错误归因的 client name。
/// - `groups`：ConsumerRuntime 返回的有序 resolved group id 列表。
/// - `container`：本 client 已校验的默认规则和 group override。
///
/// # 返回
///
/// 返回与实际 group 一一对应的有序规则列表。
///
/// # 错误
///
/// 配置存在未命中任何实际 group 的 override 时返回 Ready 错误，避免拼写错误静默失效。
fn resolve_group_rules(
    client_name: &str,
    groups: &[String],
    container: &KafkaContainerOptions,
) -> ApplicationResult<Vec<(String, ReadyRule)>> {
    let actual: BTreeSet<&str> = groups.iter().map(String::as_str).collect();
    if let Some(unknown) = container
        .readiness
        .groups
        .keys()
        .find(|group| !actual.contains(group.as_str()))
    {
        return Err(kafka_error(
            ApplicationPhase::Ready,
            format!(
                "kafka client `{client_name}` readiness override targets an unknown resolved group `{unknown}`"
            ),
        ));
    }
    Ok(groups
        .iter()
        .map(|group| {
            let rule = container
                .readiness
                .groups
                .get(group)
                .cloned()
                .unwrap_or_else(|| container.readiness.default.clone());
            (group.clone(), rule)
        })
        .collect())
}

/// 计算严格早于 Application 全局时刻的 Kafka Ready 子 deadline。
///
/// # 参数
///
/// - `application_deadline`：Runner 为完整启动流程建立的绝对截止时刻。
///
/// # 返回
///
/// 剩余时间足够时返回扣除固定诊断余量后的绝对时刻。
///
/// # 错误
///
/// 全局剩余预算已经不足诊断余量时立即返回 Kafka Ready 超时归因。
fn kafka_ready_deadline(application_deadline: Instant) -> ApplicationResult<Instant> {
    let now = Instant::now();
    let remaining = application_deadline.saturating_duration_since(now);
    if remaining <= READY_DIAGNOSTIC_RESERVE {
        return Err(kafka_error(
            ApplicationPhase::Ready,
            "kafka readiness has no remaining startup budget",
        ));
    }
    Ok(application_deadline - READY_DIAGNOSTIC_RESERVE)
}

/// 在共享子 deadline 内验证 producer-only client 的 broker metadata。
///
/// # 参数
///
/// - `client_name`：只用于稳定错误归因的 client name。
/// - `proxy`：Start 已完成本地构造的目标 client。
/// - `topic`：可选指定 topic；存在时必须可见且至少有一个分区。
/// - `deadline`：全部 client 共用、严格早于 Application 全局时刻的绝对 deadline。
///
/// # 返回
///
/// metadata 请求成功且满足指定 topic 条件时返回 `Ok(())`。
///
/// # 错误
///
/// 请求失败、topic 不存在/无分区或 deadline 到期时返回带 client 归因的 Ready 错误。
/// 规范化 Kafka bootstrap 列表为安全展示形式:按**原顺序**去空白拼接 `host:port` 列表。
/// Kafka bootstrap 不含 userinfo/SASL 凭据(凭据在独立配置项),故整串可安全进日志与错误;空则占位符。
///
/// # 参数
///
/// - `bootstrap`:配置里的原始 `bootstrap_servers`(逗号分隔 host:port)。
fn safe_bootstrap_endpoint(bootstrap: &str) -> String {
    let joined = bootstrap
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(",");
    if joined.is_empty() {
        "<unknown-endpoint>".to_owned()
    } else {
        joined
    }
}

/// 对单个 client 执行**只读** broker metadata 探针:配了 `producer_probe_topic` 则验证该 topic
/// 可见且至少一个分区,否则请求 cluster metadata。只读——不建 topic、不改配置、不提交 offset。
/// 失败错误带 client 名、安全 bootstrap endpoint、`metadata` 阶段与底层 broker 根因。
///
/// # 参数
///
/// - `client_name`:逻辑 client 名。
/// - `proxy`:该 client 的 KafkaProxy(取 admin 连接与 bootstrap endpoint)。
/// - `topic`:producer-only client 可选的指定探测 topic;consumer client 为 None,走 cluster metadata。
/// - `deadline`:本次探针的绝对上限(严格早于 Application 启动 deadline)。
async fn probe_client_metadata(
    client_name: &str,
    proxy: &nafka::KafkaProxy,
    topic: Option<&str>,
    deadline: Instant,
) -> ApplicationResult<()> {
    let endpoint = safe_bootstrap_endpoint(&proxy.config().bootstrap_servers);
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(kafka_error(
            ApplicationPhase::Ready,
            format!("kafka client `{client_name}` timed out during metadata on {endpoint} (no remaining startup budget)"),
        ));
    }
    let admin = proxy.admin();
    let result = if let Some(topic) = topic {
        tokio::time::timeout(remaining, admin.partitions_for(topic))
            .await
            .map_err(|_| {
                kafka_error(
                    ApplicationPhase::Ready,
                    format!("kafka client `{client_name}` timed out during metadata on {endpoint} (topic `{topic}` after {}ms)", remaining.as_millis()),
                )
            })?
            .and_then(|partitions| {
                if partitions.is_empty() {
                    Err(nafka::NafkaError::Broker(format!(
                        "topic `{topic}` metadata contains no partitions"
                    )))
                } else {
                    Ok(())
                }
            })
    } else {
        tokio::time::timeout(remaining, admin.list_topics())
            .await
            .map_err(|_| {
                kafka_error(
                    ApplicationPhase::Ready,
                    format!("kafka client `{client_name}` timed out during metadata on {endpoint} (cluster metadata after {}ms)", remaining.as_millis()),
                )
            })?
            .map(|_| ())
    };
    result.map_err(|error| {
        kafka_source_error(
            ApplicationPhase::Ready,
            format!("kafka client `{client_name}` probe failed during metadata on {endpoint}"),
            error,
        )
    })
}

/// 在启动任何 consumer 之前,对**所有** client 并发执行只读 metadata 探针。
///
/// 探针只读、无 owner 副作用,失败时无需回滚(尚未启动任何 consumer)。全部失败按 client 名**稳定排序**,
/// 主错误只输出第一个(名序最小)以避免多源覆盖首因,其余以 `warn` 记录(仍稳定有序)。
///
/// # 参数
///
/// - `clients`:Start 已构造的全部 client 计划。
/// - `deadline`:所有探针共享、严格早于 Application 启动 deadline 的子 deadline。
async fn probe_all_clients_metadata(
    clients: &BTreeMap<String, KafkaClientPlan>,
    deadline: Instant,
) -> ApplicationResult<()> {
    let mut probes: FuturesUnordered<ApplicationFuture<'static, (String, ApplicationResult<()>)>> =
        FuturesUnordered::new();
    for (name, plan) in clients {
        let name = name.clone();
        let proxy = plan.proxy.clone();
        let topic = plan.container.readiness.producer_probe_topic.clone();
        probes.push(Box::pin(async move {
            let result = probe_client_metadata(&name, &proxy, topic.as_deref(), deadline).await;
            Ok((name, result))
        }));
    }
    let mut failures: Vec<(String, ApplicationError)> = Vec::new();
    while let Some(item) = probes.next().await {
        // 外层 ApplicationResult 恒为 Ok(future 内不返错);`?` 只为满足 ApplicationFuture 别名的类型。
        let (name, result) = item?;
        if let Err(error) = result {
            failures.push((name, error));
        }
    }
    if failures.is_empty() {
        return Ok(());
    }
    // 稳定按 client 名排序,不由 FuturesUnordered 完成顺序决定。
    failures.sort_by(|a, b| a.0.cmp(&b.0));
    for (name, error) in failures.iter().skip(1) {
        tracing::warn!(
            client = %name,
            "additional kafka client metadata probe failed: {}",
            error.message()
        );
    }
    Err(failures.into_iter().next().expect("failures non-empty").1)
}

/// 把 group ready 等待错误转成富诊断。
///
/// `GroupReadyTimeout` 展开每个未就绪 group 的紧凑快照:要求的 `ReadyRequirement`、等待预算、`GroupState`、
/// assignment 分区数、`ready_assignment_epoch` 是否存在、脱敏 `last_error`——无需 librdkafka 旁路日志即可定位。
/// 其它错误按通用摘要 + 类型化 source。
///
/// # 参数
///
/// - `client_name`:逻辑 client 名。
/// - `groups`:该 client 的 (group, 规则) 列表,用于把 group id 映射回其 `ReadyRequirement`。
/// - `budget_ms`:group ready 等待的大致预算毫秒(用于诊断 "before ~Xms")。
/// - `error`:`await_groups_ready` 返回的底层错误。
fn group_ready_error(
    client_name: &str,
    groups: &[(String, ReadyRule)],
    budget_ms: u128,
    error: nafka::NafkaError,
) -> ApplicationError {
    if let nafka::NafkaError::GroupReadyTimeout { last_health, .. } = &error {
        let mut lines: Vec<String> = last_health
            .iter()
            .map(|health| {
                let requirement = groups
                    .iter()
                    .find(|(group, _)| group == &health.group)
                    .map(|(_, rule)| describe_requirement(&rule.requirement()))
                    .unwrap_or_else(|| "<unknown>".to_owned());
                format!(
                    "group `{}` did not satisfy `{}` before ~{}ms: state={:?} assignment={} ready_epoch={} last_error={}",
                    health.group,
                    requirement,
                    budget_ms,
                    health.state,
                    health.assignment.len(),
                    if health.ready_assignment_epoch.is_some() {
                        "present"
                    } else {
                        "none"
                    },
                    health
                        .last_error
                        .as_deref()
                        .map(|reason| format!("\"{reason}\""))
                        .unwrap_or_else(|| "none".to_owned()),
                )
            })
            .collect();
        // group id 领起每行,排序即按 group 名稳定。
        lines.sort();
        return kafka_error(
            ApplicationPhase::Ready,
            format!(
                "kafka client `{client_name}` group readiness timed out: {}",
                lines.join("; ")
            ),
        );
    }
    kafka_source_error(
        ApplicationPhase::Ready,
        format!("kafka client `{client_name}` group readiness failed"),
        error,
    )
}

/// 把 `ReadyRequirement` 转成诊断用短名。
///
/// # 参数
///
/// - `requirement`:group 的就绪要求。
fn describe_requirement(requirement: &nafka::ReadyRequirement) -> String {
    match requirement {
        nafka::ReadyRequirement::Joined => "joined".to_owned(),
        nafka::ReadyRequirement::Assigned { min_partitions } => {
            format!("assigned>={min_partitions}")
        }
        nafka::ReadyRequirement::AssignedTopics(_) => "assigned_topics".to_owned(),
    }
}

/// 创建没有底层 source 的 Kafka 组件错误。
///
/// # 参数
///
/// - `phase`：错误被观察到的 Application 生命周期阶段。
/// - `message`：不包含 broker、凭据或业务负载的稳定摘要。
///
/// # 返回
///
/// 返回 component 固定为 Kafka 的统一错误。
fn kafka_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Kafka, phase, message)
}

/// 创建保留类型化底层 source 的 Kafka 组件错误。
///
/// # 参数
///
/// - `phase`：错误被观察到的 Application 生命周期阶段。
/// - `message`：不复制底层配置值或消息正文的稳定摘要。
/// - `source`：serde 或 nafka 返回的类型化根因。
///
/// # 返回
///
/// 返回 component 固定为 Kafka 且保留 source 链的统一错误。
fn kafka_source_error(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Kafka, phase, message, source)
}
