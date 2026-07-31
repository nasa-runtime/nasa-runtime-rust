//! socket Kafka control/data plane 的统一装配与生命周期。

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use nafka::{
    BorrowedKafkaRecord, ConsumerRuntime, Delivery, GroupSpec, InvalidRecordReason, KafkaConfig,
    KafkaProxy, NafkaError, PassthroughConsumer, PassthroughDisposition, PassthroughFailure,
    ProducerLane, ProducerLaneStats, ReadyRequirement,
};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::{
    GroupMode, HeaderPassthroughAdapter, KafkaClusterDataPublisher, PassthroughConfig, SenderSlot,
    SourceFence, SourceIdentityPolicy, TopicContractLimits, TopicManagement, WsKafkaPublisher,
    WsKafkaTopicContract,
};
use crate::cluster::{Notifier, OnMessage, PublishOutcome, ReadyFuture, StartError};
use crate::proto::{ClusterEvent, Mode, WireCodec};

/// control consumer 使用的固定业务事件。
pub const NAWS_CONTROL_EVENT: &str = "NAWS_CONTROL";

/// 固定协议、key 与框架 header 的默认保守预算。
pub const DEFAULT_PROTOCOL_HEADER_BUDGET_BYTES: usize = 4096;

/// 连续无法读取 TopicContract 的次数上限；确定漂移不经过该预算。
const CONTRACT_AUDIT_FAILURE_LIMIT: u32 = 3;

/// socket Kafka runtime 的冻结装配配置。
#[derive(Clone, Debug)]
pub struct WsKafkaRuntimeConfig {
    /// 当前稳定逻辑节点 ID。
    pub local_node: String,
    /// control plane topic。
    pub control_topic: String,
    /// data plane topic。
    pub data_topic: String,
    /// control 物理 producer lane 名。
    pub control_lane: String,
    /// data 物理 producer lane 名。
    pub data_lane: String,
    /// DLT 物理 producer lane 名；可等于 data，不能等于 control。
    pub dlt_lane: String,
    /// 当前节点独占的 control group id。
    pub control_group: String,
    /// data header、group、容量与交付策略。
    pub passthrough: PassthroughConfig,
    /// 首次 group assignment 的总等待毫秒数。
    pub ready_timeout_ms: u64,
    /// 完整 presence 快照周期，供 control retention 核验。
    pub full_presence_interval_ms: u64,
    /// 固定协议、key 与框架 header 的保守容量预算。
    pub protocol_header_budget_bytes: usize,
    /// 运行期 topic 漂移复核周期；零表示不启用后台复核。
    pub contract_check_interval_ms: u64,
    /// Kafka native queue 与 DLT backlog 的进程级内存预算。
    pub kafka_memory_budget_bytes: usize,
}

impl WsKafkaRuntimeConfig {
    /// 返回适合生产继续细化的安全基线。
    pub fn new(local_node: impl Into<String>) -> Self {
        let local_node = local_node.into();
        Self {
            control_group: format!("naws-control-{local_node}"),
            local_node,
            control_topic: "naws.cluster.control".into(),
            data_topic: "naws.cluster.data".into(),
            control_lane: "control".into(),
            data_lane: "data".into(),
            dlt_lane: "data".into(),
            passthrough: PassthroughConfig {
                source_identity_policy: SourceIdentityPolicy::Required,
                ..PassthroughConfig::default()
            },
            ready_timeout_ms: 30_000,
            full_presence_interval_ms: 10_000,
            protocol_header_budget_bytes: DEFAULT_PROTOCOL_HEADER_BUDGET_BYTES,
            contract_check_interval_ms: 30_000,
            kafka_memory_budget_bytes: 256 * 1024 * 1024,
        }
    }
}

/// runtime 当前生命周期与最近契约错误的只读快照。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WsKafkaRuntimeHealth {
    /// 稳定生命周期名称。
    pub state: &'static str,
    /// 最近一次 topic 契约漂移错误。
    pub last_contract_error: Option<String>,
}

/// socket Kafka control/data/DLT 三平面的物理 producer 快照。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WsKafkaPlaneStats {
    /// control lane 快照。
    pub control: ProducerLaneStats,
    /// data lane 快照。
    pub data: ProducerLaneStats,
    /// DLT lane 快照；与 data 共用物理 lane 时两者内容相同。
    pub dlt: ProducerLaneStats,
}

/// runtime 内部生命周期。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum RuntimeState {
    /// 已完成本地装配。
    Created = 0,
    /// 正在核验 topic、冻结 registry 和等待 assignment。
    Starting = 1,
    /// control/data group 均已就绪。
    Running = 2,
    /// 正在停止 consumer 和 producer。
    ShuttingDown = 3,
    /// 已完全停止。
    Stopped = 4,
    /// 启动或运行期契约检查失败。
    Failed = 5,
}

/// 共享 runtime 状态。
struct RuntimeInner {
    /// 通用 Kafka runtime。
    kafka: KafkaProxy,
    /// socket 装配配置。
    config: WsKafkaRuntimeConfig,
    /// topic 冻结契约。
    contract: WsKafkaTopicContract,
    /// 固定 control producer lane。
    control_lane: ProducerLane,
    /// 固定 data producer lane。
    data_lane: ProducerLane,
    /// 固定 DLT producer lane。
    dlt_lane: ProducerLane,
    /// 固定 data publisher。
    data_publisher: WsKafkaPublisher,
    /// 固定 cluster data publisher。
    cluster_data_publisher: Arc<KafkaClusterDataPublisher>,
    /// Server build 后、Kafka start 前注入的 Sender 白名单。
    senders: OnceLock<SenderSlot>,
    /// Server build 后注入的窄化来源围栏。
    source_fence: OnceLock<Arc<dyn SourceFence>>,
    /// Notifier.start 注入的 control 接收回调。
    on_control: OnceLock<OnMessage>,
    /// 已启动消费者句柄。
    consumers: OnceLock<ConsumerRuntime>,
    /// runtime 是否已经包含 control consumer。
    started_with_control: std::sync::atomic::AtomicBool,
    /// 全 runtime 唯一 Notifier.start 门禁。
    notifier_started: std::sync::atomic::AtomicBool,
    /// 串行化一次性启动。
    start_lock: tokio::sync::Mutex<()>,
    /// 生命周期。
    state: AtomicU8,
    /// 后台漂移检查和异步 shutdown 任务。
    tasks: TaskTracker,
    /// 停止后台任务。
    cancel: CancellationToken,
    /// 最近 topic 契约错误。
    last_contract_error: Mutex<Option<String>>,
    /// 只有完整 ready 后才允许 control/data cluster ingress。
    ingress_ready: Arc<std::sync::atomic::AtomicBool>,
}

/// control/data 两平面的共享 Kafka runtime。
#[derive(Clone)]
pub struct WsKafkaRuntime {
    /// 共享内部状态。
    inner: Arc<RuntimeInner>,
}

impl WsKafkaRuntime {
    /// 校验通用配置、构造 KafkaProxy 并冻结 socket runtime。
    ///
    /// # 参数
    ///
    /// - `kafka_config`: nafka 完整配置。
    /// - `config`: socket control/data 装配配置。
    /// - `contract`: topic 冻结契约。
    ///
    /// # 错误
    ///
    /// 任一配置、lane 或 topic 静态契约非法时返回错误；不会连接 broker。
    pub fn connect(
        kafka_config: KafkaConfig,
        config: WsKafkaRuntimeConfig,
        contract: WsKafkaTopicContract,
    ) -> nafka::Result<Self> {
        let kafka = KafkaProxy::connect(kafka_config)?;
        Self::new(kafka, config, contract)
    }

    /// 以已经构造的 KafkaProxy 装配 socket runtime。
    ///
    /// # 参数
    ///
    /// - `kafka`: 共享通用 Kafka runtime。
    /// - `config`: socket control/data 装配配置。
    /// - `contract`: topic 冻结契约。
    ///
    /// # 错误
    ///
    /// lane、group、topic 或容量静态契约不满足时返回错误。
    pub fn new(
        kafka: KafkaProxy,
        config: WsKafkaRuntimeConfig,
        contract: WsKafkaTopicContract,
    ) -> nafka::Result<Self> {
        validate_runtime_config(&kafka, &config, &contract)?;
        let limits = contract_limits(&kafka, &config);
        contract.validate(limits)?;
        let control_lane = kafka.producer_lane(&config.control_lane)?;
        let data_lane = kafka.producer_lane(&config.data_lane)?;
        let dlt_lane = kafka.producer_lane(&config.dlt_lane)?;
        let ingress_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let data_publisher = WsKafkaPublisher::new(data_lane.clone(), config.passthrough.clone())
            .map_err(NafkaError::Config)?;
        let cluster_data_publisher = Arc::new(
            KafkaClusterDataPublisher::new_gated(
                data_lane.clone(),
                config.data_topic.clone(),
                config.passthrough.clone(),
                Arc::clone(&ingress_ready),
                config.local_node.clone(),
            )
            .map_err(NafkaError::Config)?,
        );
        Ok(Self {
            inner: Arc::new(RuntimeInner {
                kafka,
                config,
                contract,
                control_lane,
                data_lane,
                dlt_lane,
                data_publisher,
                cluster_data_publisher,
                senders: OnceLock::new(),
                source_fence: OnceLock::new(),
                on_control: OnceLock::new(),
                consumers: OnceLock::new(),
                started_with_control: std::sync::atomic::AtomicBool::new(false),
                notifier_started: std::sync::atomic::AtomicBool::new(false),
                start_lock: tokio::sync::Mutex::new(()),
                state: AtomicU8::new(RuntimeState::Created as u8),
                tasks: TaskTracker::new(),
                cancel: CancellationToken::new(),
                last_contract_error: Mutex::new(None),
                ingress_ready,
            }),
        })
    }

    /// 返回实现 control plane Notifier 的轻量句柄。
    pub fn notifier(&self) -> KafkaNotifier {
        KafkaNotifier {
            runtime: self.clone(),
        }
    }

    /// 返回固定 data lane/topic 的 cluster data publisher。
    pub fn cluster_data_publisher(&self) -> Arc<KafkaClusterDataPublisher> {
        Arc::clone(&self.inner.cluster_data_publisher)
    }

    /// runtime ready 后返回固定 data lane 的业务 raw publisher。
    ///
    /// # 错误
    ///
    /// consumer 尚未完成初始 assignment 或 runtime 已关闭时返回生命周期错误。
    pub fn publisher(&self) -> nafka::Result<WsKafkaPublisher> {
        if self.state() != RuntimeState::Running {
            return Err(NafkaError::Lifecycle(
                "data publisher 只能在 runtime ready 后取得".into(),
            ));
        }
        Ok(self.inner.data_publisher.clone())
    }

    /// 返回 control consumer group 的健康快照。
    ///
    /// # 错误
    ///
    /// runtime 尚未启动 control group 时返回 group 不存在错误。
    pub async fn control_group_health(&self) -> nafka::Result<nafka::GroupHealth> {
        self.inner
            .kafka
            .group_health(&self.inner.config.control_group)
            .await
    }

    /// 返回 data consumer group 的健康快照。
    ///
    /// # 错误
    ///
    /// runtime 尚未安装 consumer，或解析后的 group 表中不存在 data group 时返回错误。
    pub async fn data_group_health(&self) -> nafka::Result<nafka::GroupHealth> {
        let consumers = self
            .inner
            .consumers
            .get()
            .ok_or_else(|| NafkaError::Lifecycle("Kafka consumer runtime 尚未启动".into()))?;
        let include_control = self.inner.started_with_control.load(Ordering::Acquire);
        let group = consumers
            .groups()
            .iter()
            .find(|group| !include_control || *group != &self.inner.config.control_group)
            .ok_or_else(|| NafkaError::Registry("Kafka data group 不存在".into()))?;
        self.inner.kafka.group_health(group).await
    }

    /// 返回 control/data/DLT 三平面的物理 producer 容量与累计结果。
    pub fn plane_stats(&self) -> WsKafkaPlaneStats {
        WsKafkaPlaneStats {
            control: self.inner.control_lane.stats(),
            data: self.inner.data_lane.stats(),
            dlt: self.inner.dlt_lane.stats(),
        }
    }

    /// 在 Server build 后、listener bind 前一次性注入本地 Sender 与来源围栏。
    ///
    /// # 参数
    ///
    /// - `senders`: 预注册 `sink_id → Sender` 白名单。
    /// - `source_fence`: 只暴露 incarnation 判定的窄接口。
    ///
    /// # 错误
    ///
    /// runtime 已启动、缺少 required fence 或重复绑定时返回生命周期错误。
    pub fn bind_data_plane(
        &self,
        senders: SenderSlot,
        source_fence: Option<Arc<dyn SourceFence>>,
    ) -> nafka::Result<()> {
        if self.state() != RuntimeState::Created {
            return Err(NafkaError::Lifecycle(
                "data plane 只能在 runtime 启动前绑定".into(),
            ));
        }
        if self.inner.config.passthrough.source_identity_policy != SourceIdentityPolicy::Forbidden
            && source_fence.is_none()
        {
            return Err(NafkaError::Config(
                "允许来源身份时必须注入 SourceFence".into(),
            ));
        }
        self.inner
            .senders
            .set(senders)
            .map_err(|_| NafkaError::Lifecycle("SenderSlot 已绑定".into()))?;
        if let Some(fence) = source_fence {
            self.inner
                .source_fence
                .set(fence)
                .map_err(|_| NafkaError::Lifecycle("SourceFence 已绑定".into()))?;
        }
        Ok(())
    }

    /// 显式启动 data-only 模式，不伪造 control Notifier。
    ///
    /// # 错误
    ///
    /// Sender/Fence 未绑定、topic 契约失败、registry 启动失败或 assignment 超时返回错误。
    pub async fn start_data_only(&self) -> nafka::Result<()> {
        self.start(false).await
    }

    /// 关闭 ingress、停止 consumer、排空 producer，并等待后台检查任务退出。
    ///
    /// # 错误
    ///
    /// nafka 共享 shutdown deadline 内仍有 group 或 lane 未排空时返回错误。
    pub async fn shutdown(&self) -> nafka::Result<()> {
        let current = self.state();
        if current == RuntimeState::Stopped {
            return Ok(());
        }
        self.inner
            .state
            .store(RuntimeState::ShuttingDown as u8, Ordering::Release);
        self.inner.ingress_ready.store(false, Ordering::Release);
        self.inner.cancel.cancel();
        self.inner.tasks.close();
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(self.inner.kafka.config().behavior.shutdown_timeout_ms);
        let (kafka_result, task_result) = tokio::join!(
            tokio::time::timeout_at(deadline, self.inner.kafka.shutdown()),
            tokio::time::timeout_at(deadline, self.inner.tasks.wait()),
        );
        let result = match kafka_result {
            Ok(result) => result,
            Err(_) => Err(NafkaError::ShutdownTimeout {
                groups: Vec::new(),
                producer_lanes: vec!["ws-kafka-runtime".into()],
            }),
        };
        let result = if result.is_ok() && task_result.is_err() {
            Err(NafkaError::ShutdownTimeout {
                groups: vec!["ws-kafka-background".into()],
                producer_lanes: Vec::new(),
            })
        } else {
            result
        };
        if result.is_ok() {
            self.inner
                .state
                .store(RuntimeState::Stopped as u8, Ordering::Release);
        }
        result
    }

    /// 返回稳定 runtime 健康快照。
    pub fn health(&self) -> WsKafkaRuntimeHealth {
        WsKafkaRuntimeHealth {
            state: state_name(self.state()),
            last_contract_error: self
                .inner
                .last_contract_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        }
    }

    /// 安装 control 回调并启动完整 runtime。
    async fn start_control(&self, on_message: OnMessage) -> nafka::Result<()> {
        self.inner
            .on_control
            .set(on_message)
            .map_err(|_| NafkaError::Lifecycle("control callback 已安装".into()))?;
        self.start(true).await
    }

    /// 核验 topic、冻结 registry 并等待精确 topic assignment。
    async fn start(&self, include_control: bool) -> nafka::Result<()> {
        let _guard = self.inner.start_lock.lock().await;
        match self.state() {
            RuntimeState::Running => {
                if include_control && !self.inner.started_with_control.load(Ordering::Acquire) {
                    return Err(NafkaError::Lifecycle(
                        "data-only runtime 不能在启动后追加 control consumer".into(),
                    ));
                }
                return Ok(());
            }
            RuntimeState::Created => {}
            state => {
                return Err(NafkaError::Lifecycle(format!(
                    "runtime 无法从 {} 启动",
                    state_name(state)
                )))
            }
        }
        self.inner
            .state
            .store(RuntimeState::Starting as u8, Ordering::Release);
        let result = self.start_inner(include_control).await;
        match result {
            Ok(()) => {
                self.inner
                    .started_with_control
                    .store(include_control, Ordering::Release);
                self.inner
                    .state
                    .store(RuntimeState::Running as u8, Ordering::Release);
                self.inner.ingress_ready.store(true, Ordering::Release);
                self.spawn_contract_monitor();
                Ok(())
            }
            Err(error) => {
                self.inner.ingress_ready.store(false, Ordering::Release);
                let _ = self.inner.kafka.shutdown().await;
                self.inner
                    .state
                    .store(RuntimeState::Failed as u8, Ordering::Release);
                Err(error)
            }
        }
    }

    /// 执行一次启动事务。
    async fn start_inner(&self, include_control: bool) -> nafka::Result<()> {
        let senders = self
            .inner
            .senders
            .get()
            .cloned()
            .ok_or_else(|| NafkaError::Lifecycle("SenderSlot 尚未绑定".into()))?;
        let source_fence = self.inner.source_fence.get().cloned();
        if include_control && self.inner.on_control.get().is_none() {
            return Err(NafkaError::Lifecycle("control callback 尚未安装".into()));
        }
        warn_frame_budget(&self.inner.kafka, senders.min_max_frame());
        self.inner
            .contract
            .enforce(
                &self.inner.kafka.admin(),
                contract_limits(&self.inner.kafka, &self.inner.config),
            )
            .await?;
        let data = HeaderPassthroughAdapter::new(
            self.inner.config.data_topic.clone(),
            self.inner.config.local_node.clone(),
            self.inner.config.passthrough.clone(),
            senders,
            source_fence,
        )
        .map_err(NafkaError::Config)?;
        let mut registry = self.inner.kafka.consumers();
        if include_control {
            registry = registry.register_passthrough(ControlConsumer {
                topic: self.inner.config.control_topic.clone(),
                group: self.inner.config.control_group.clone(),
                on_message: self
                    .inner
                    .on_control
                    .get()
                    .cloned()
                    .ok_or_else(|| NafkaError::Lifecycle("control callback 尚未安装".into()))?,
            })?;
        }
        let runtime = registry.register_passthrough(data)?.start().await?;
        let groups = runtime.groups();
        let deadline = Instant::now() + Duration::from_millis(self.inner.config.ready_timeout_ms);
        let mut requirements = Vec::new();
        if include_control {
            if groups.len() != 2
                || !groups
                    .iter()
                    .any(|group| group == &self.inner.config.control_group)
            {
                return Err(NafkaError::Registry(
                    "control/data 必须解析为两个独立 consumer group".into(),
                ));
            }
            requirements.push((
                self.inner.config.control_group.clone(),
                ReadyRequirement::AssignedTopics(vec![self.inner.config.control_topic.clone()]),
            ));
            let data_group = groups
                .iter()
                .find(|group| *group != &self.inner.config.control_group)
                .expect("长度和 control group 已校验")
                .clone();
            requirements.push((
                data_group,
                ReadyRequirement::AssignedTopics(vec![self.inner.config.data_topic.clone()]),
            ));
        } else {
            if groups.len() != 1 {
                return Err(NafkaError::Registry(
                    "data-only runtime 必须只解析出一个 consumer group".into(),
                ));
            }
            requirements.push((
                groups[0].clone(),
                ReadyRequirement::AssignedTopics(vec![self.inner.config.data_topic.clone()]),
            ));
        }
        self.inner
            .kafka
            .await_groups_ready(requirements, deadline)
            .await?;
        self.inner
            .consumers
            .set(runtime)
            .map_err(|_| NafkaError::Lifecycle("consumer runtime 已安装".into()))?;
        Ok(())
    }

    /// 启动运行期 topic 漂移复核；发现漂移立即关闭 Kafka ingress。
    fn spawn_contract_monitor(&self) {
        let interval_ms = self.inner.config.contract_check_interval_ms;
        if interval_ms == 0 {
            return;
        }
        let runtime = self.clone();
        self.inner.tasks.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
            let mut consecutive_observation_failures = 0_u32;
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = runtime.inner.cancel.cancelled() => break,
                    _ = interval.tick() => {
                        let result = runtime.inner.contract.audit(
                            &runtime.inner.kafka.admin(),
                            contract_limits(&runtime.inner.kafka, &runtime.inner.config),
                        ).await;
                        match result {
                            Ok(()) => {
                                consecutive_observation_failures = 0;
                                *runtime.inner.last_contract_error.lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                            }
                            Err(error) => {
                                let stop = contract_audit_failure_requires_stop(
                                    &error,
                                    &mut consecutive_observation_failures,
                                );
                                *runtime.inner.last_contract_error.lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                    Some(error.to_string());
                                if !stop {
                                    tracing::warn!(
                                        error = %error,
                                        consecutive = consecutive_observation_failures,
                                        limit = CONTRACT_AUDIT_FAILURE_LIMIT,
                                        "socket Kafka topic 契约短时无法观测，将继续复核"
                                    );
                                    continue;
                                }
                                runtime.inner.state.store(RuntimeState::Failed as u8, Ordering::Release);
                                runtime.inner.ingress_ready.store(false, Ordering::Release);
                                tracing::error!(error = %error, "socket Kafka topic 契约漂移或连续无法观测，正在停止 ingress");
                                let _ = runtime.inner.kafka.shutdown().await;
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    /// 由同步 Notifier.shutdown 发起受跟踪的异步关闭任务。
    fn spawn_shutdown(&self) {
        if matches!(
            self.state(),
            RuntimeState::ShuttingDown | RuntimeState::Stopped
        ) {
            return;
        }
        let runtime = self.clone();
        self.inner
            .state
            .store(RuntimeState::ShuttingDown as u8, Ordering::Release);
        self.inner.ingress_ready.store(false, Ordering::Release);
        self.inner.cancel.cancel();
        self.inner.tasks.spawn(async move {
            if runtime.inner.kafka.shutdown().await.is_ok() {
                runtime
                    .inner
                    .state
                    .store(RuntimeState::Stopped as u8, Ordering::Release);
            }
        });
    }

    /// 读取内部生命周期。
    fn state(&self) -> RuntimeState {
        match self.inner.state.load(Ordering::Acquire) {
            0 => RuntimeState::Created,
            1 => RuntimeState::Starting,
            2 => RuntimeState::Running,
            3 => RuntimeState::ShuttingDown,
            4 => RuntimeState::Stopped,
            _ => RuntimeState::Failed,
        }
    }
}

/// 判断一次运行期契约复核失败是否必须立即停 ingress。
///
/// 已成功观测到的确定契约不一致使用 `TopicContract`，立即失败；broker/网络/权限等
/// 观测错误允许少量连续重试，避免一次短时 AllBrokersDown 被误判成永久分区漂移。
fn contract_audit_failure_requires_stop(error: &NafkaError, consecutive: &mut u32) -> bool {
    if matches!(error, NafkaError::TopicContract(_)) {
        *consecutive = CONTRACT_AUDIT_FAILURE_LIMIT;
        return true;
    }
    *consecutive = consecutive.saturating_add(1);
    *consecutive >= CONTRACT_AUDIT_FAILURE_LIMIT
}

/// 实现 control plane 的 Kafka Notifier。
#[derive(Clone)]
pub struct KafkaNotifier {
    /// 共享 socket Kafka runtime。
    runtime: WsKafkaRuntime,
}

impl KafkaNotifier {
    /// 发布 control payload 并等待 broker delivery report。
    ///
    /// 该入口用于需要明确 broker 确认的启动控制消息和验收；常规 presence 仍使用 Notifier::publish 的
    /// 有界 fire 语义。
    ///
    /// # 参数
    ///
    /// - `payload`: 已编码 ClusterEvent bytes。
    ///
    /// # 错误
    ///
    /// 生命周期、producer 准入、broker delivery 或超时失败时返回 nafka 错误。
    pub async fn publish_confirmed(&self, payload: &[u8]) -> nafka::Result<Delivery> {
        if self.runtime.state() != RuntimeState::Running {
            return Err(NafkaError::Lifecycle(
                "control runtime 尚未 ready 或已关闭".into(),
            ));
        }
        self.runtime
            .inner
            .control_lane
            .publish_raw(&self.runtime.inner.config.control_topic, payload)
            .event(NAWS_CONTROL_EVENT)
            .key(self.runtime.inner.config.local_node.clone())
            .send()
            .await
    }
}

impl Notifier for KafkaNotifier {
    /// 安装 Cluster control callback，启动两组 consumer 并等待 broker assignment。
    fn start(&self, on_message: OnMessage) -> ReadyFuture {
        if self
            .runtime
            .inner
            .notifier_started
            .swap(true, Ordering::AcqRel)
        {
            return Box::pin(async { Err(StartError::AlreadyStarted) });
        }
        let runtime = self.runtime.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.runtime.inner.tasks.spawn(async move {
            let result = runtime
                .start_control(on_message)
                .await
                .map_err(|error| StartError::NotReady(error.to_string()));
            let _ = sender.send(result);
        });
        Box::pin(async move {
            receiver.await.unwrap_or_else(|_| {
                Err(StartError::NotReady(
                    "Kafka runtime 启动任务在报告结果前退出".into(),
                ))
            })
        })
    }

    /// 把 ClusterEvent bytes 发布到固定 control topic/lane，并以 source node 作 key。
    fn publish(&self, payload: Bytes) -> PublishOutcome {
        if self.runtime.state() != RuntimeState::Running {
            return PublishOutcome::Closed;
        }
        let result = self
            .runtime
            .inner
            .control_lane
            .publish_raw(&self.runtime.inner.config.control_topic, payload.as_ref())
            .event(NAWS_CONTROL_EVENT)
            .key(self.runtime.inner.config.local_node.clone())
            .fire();
        match result {
            Ok(()) => PublishOutcome::Queued,
            Err(NafkaError::ProducerQueueFull { .. }) => PublishOutcome::Full,
            Err(_) => PublishOutcome::Closed,
        }
    }

    /// 发起异步 runtime shutdown；真正 join 由 task tracker 完成。
    fn shutdown(&self) {
        self.runtime.spawn_shutdown();
    }

    /// 返回 runtime 后台任务跟踪器。
    fn task_tracker(&self) -> Option<&TaskTracker> {
        Some(&self.runtime.inner.tasks)
    }
}

/// control topic 的同步借用消费者；payload 只在进入 Cluster 回调前复制一次。
struct ControlConsumer {
    /// 固定 control topic。
    topic: String,
    /// 当前节点独占 group。
    group: String,
    /// Cluster control 回调。
    on_message: OnMessage,
}

impl PassthroughConsumer for ControlConsumer {
    /// 返回固定 control topic。
    fn topics(&self) -> Vec<String> {
        vec![self.topic.clone()]
    }

    /// 返回固定 control 事件。
    fn event(&self) -> String {
        NAWS_CONTROL_EVENT.into()
    }

    /// 返回当前节点独占 control group。
    fn group(&self) -> GroupSpec {
        GroupSpec::Named(self.group.clone())
    }

    /// 复制一次 control payload 后同步交给 Cluster。
    fn consume_borrowed(
        &self,
        record: BorrowedKafkaRecord<'_>,
    ) -> Result<PassthroughDisposition, PassthroughFailure> {
        let payload = record.payload.ok_or_else(|| {
            PassthroughFailure::invalid(
                InvalidRecordReason::MissingPayload,
                "control topic 不允许 tombstone",
            )
        })?;
        ClusterEvent::decode(Mode::BitpackTlv, payload).map_err(|_| {
            PassthroughFailure::invalid(
                InvalidRecordReason::MalformedRoute,
                "control payload 不是合法 BITPACK_TLV ClusterEvent",
            )
        })?;
        (self.on_message)(Bytes::copy_from_slice(payload));
        Ok(PassthroughDisposition::Handled { queued: 1 })
    }
}

/// 校验 socket runtime 的 lane、group、topic 与底层配置一致性。
/// 帧信封开销预算：`Message` TLV 的 tag/长度域与定长头，帧长 = payload + 该预算。
///
/// 不含路由 header 预算——那部分已经计入 `behavior.max_record_bytes`
/// （Kafka 记录 = payload + key + headers），再加一次即重复计入。
const FRAME_ENVELOPE_BUDGET: usize = 4 * 1024;

/// 检查"能装进单帧的最大记录"这一跨侧约束，不一致时**告警**（不拒绝启动）。
///
/// 记录经 `Message` TLV 信封编码后才成为 frame，帧长 ≈ payload + 信封 + 定长头。
/// 若允许的最大记录加信封超过 `max_frame`，一条**合法**的大记录会在编码期
/// `FrameTooLarge` → `Halt`（不可重试、不进 DLT），等于外部可停掉数据面。
///
/// **为什么是告警不是拒绝**：`behavior.max_record_bytes` 与 `DEFAULT_MAX_FRAME` 的默认值都是
/// 16MiB，而 TopicContract 又要求 `max_message_bytes >= max_record_bytes + header 预算`。
/// 三者叠加后，任何"硬拒绝"形式的校验都会让**默认配置无法启动**（实测下界 16797696 >
/// 上界 16760832，合法区间为空）。默认值保持不变、放大走 `ServerConfig.max_frame`，
/// 因此这里只负责给出确定信号，取舍留给业务侧。
///
/// # 参数
///
/// - `kafka`: 已冻结配置的运行时；用 `behavior.max_record_bytes` 作为 payload 上界。
/// - `min_max_frame`: 全部目标 Sender 中最小的单帧上限。
fn warn_frame_budget(kafka: &KafkaProxy, min_max_frame: usize) {
    let max_record = kafka.config().behavior.max_record_bytes;
    // 只加信封开销：header 预算已经计入 max_record_bytes（记录 = payload+key+headers），
    // 再加一次就是重复计入，正是"区间为空"的根因。
    let required = max_record.saturating_add(FRAME_ENVELOPE_BUDGET);
    if required > min_max_frame {
        tracing::warn!(
            max_record_bytes = max_record,
            envelope_budget = FRAME_ENVELOPE_BUDGET,
            min_max_frame,
            "单条记录可能装不进 socket 单帧：贴近上限的合法记录会在编码期 FrameTooLarge 并 \
             Halt 数据面(不可重试、不进 DLT)。要消除该风险二选一：\
             调小 behavior.max_record_bytes，或调大 ServerConfig.max_frame(默认 16MiB)"
        );
    }
}

/// 交叉校验 socket Kafka topic、lane、group、DLT 与 wire frame 预算的不变量。
fn validate_runtime_config(
    kafka: &KafkaProxy,
    config: &WsKafkaRuntimeConfig,
    contract: &WsKafkaTopicContract,
) -> nafka::Result<()> {
    for (name, value) in [
        ("local_node", &config.local_node),
        ("control_topic", &config.control_topic),
        ("data_topic", &config.data_topic),
        ("control_lane", &config.control_lane),
        ("data_lane", &config.data_lane),
        ("dlt_lane", &config.dlt_lane),
        ("control_group", &config.control_group),
    ] {
        if value.trim().is_empty() || value.chars().any(char::is_control) {
            return Err(NafkaError::Config(format!(
                "WsKafkaRuntimeConfig.{name} 不能为空或包含控制字符"
            )));
        }
    }
    if config.control_topic == config.data_topic {
        return Err(NafkaError::Config("control 与 data topic 必须不同".into()));
    }
    if config.control_lane == config.data_lane || config.control_lane == config.dlt_lane {
        return Err(NafkaError::Config(
            "control lane 必须与 data/DLT lane 物理隔离".into(),
        ));
    }
    if kafka.config().behavior.dlt_producer_lane != config.dlt_lane {
        return Err(NafkaError::Config(
            "WsKafkaRuntimeConfig.dlt_lane 必须等于 behavior.dlt_producer_lane".into(),
        ));
    }
    if config.ready_timeout_ms == 0
        || config.full_presence_interval_ms == 0
        || config.protocol_header_budget_bytes == 0
        || config.kafka_memory_budget_bytes == 0
    {
        return Err(NafkaError::Config(
            "ready、presence 与 protocol header 预算必须大于零".into(),
        ));
    }
    if kafka.config().consumer.auto_offset_reset != "latest" {
        return Err(NafkaError::Config(
            "socket ephemeral control/data group 必须使用 auto_offset_reset=latest".into(),
        ));
    }
    config.passthrough.validate().map_err(NafkaError::Config)?;
    if config.passthrough.source_identity_policy != SourceIdentityPolicy::Required {
        return Err(NafkaError::Config(
            "cluster relay data route 必须使用 SourceIdentityPolicy::Required".into(),
        ));
    }
    if matches!(
        &config.passthrough.group_mode,
        GroupMode::Durable { group } if group == &config.control_group
    ) {
        return Err(NafkaError::Config(
            "control 与 data consumer group 必须隔离".into(),
        ));
    }
    if contract.control.name != config.control_topic || contract.data.name != config.data_topic {
        return Err(NafkaError::TopicContract(
            "runtime control/data topic 必须与 TopicContract 一致".into(),
        ));
    }
    let max_topic_message = std::iter::once(&contract.control)
        .chain(std::iter::once(&contract.data))
        .chain(contract.dead_letters.iter())
        .map(|spec| spec.max_message_bytes)
        .max()
        .unwrap_or(0);
    if kafka.config().consumer.max_partition_fetch_bytes < max_topic_message {
        return Err(NafkaError::Config(format!(
            "consumer.max_partition_fetch_bytes({}) 小于 TopicContract 最大消息({max_topic_message})",
            kafka.config().consumer.max_partition_fetch_bytes
        )));
    }
    validate_dead_letter_topics(kafka, config, contract)?;
    validate_memory_budget(kafka, config)?;
    if matches!(contract.management, TopicManagement::ValidateOnly) {
        validate_production_lane(
            kafka.config(),
            &config.control_lane,
            contract.control.max_message_bytes,
        )?;
        let dlt_message_max = contract
            .dead_letters
            .iter()
            .map(|spec| spec.max_message_bytes)
            .max()
            .unwrap_or(0);
        validate_production_lane(
            kafka.config(),
            &config.data_lane,
            contract
                .data
                .max_message_bytes
                .max(if config.dlt_lane == config.data_lane {
                    dlt_message_max
                } else {
                    0
                }),
        )?;
        if config.dlt_lane != config.data_lane {
            validate_production_lane(kafka.config(), &config.dlt_lane, dlt_message_max)?;
        }
    }
    kafka.producer_lane(&config.control_lane)?;
    kafka.producer_lane(&config.data_lane)?;
    kafka.producer_lane(&config.dlt_lane)?;
    Ok(())
}

/// 要求 TopicContract 覆盖 control/data 实际可能产生的 DLT topic。
fn validate_dead_letter_topics(
    kafka: &KafkaProxy,
    config: &WsKafkaRuntimeConfig,
    contract: &WsKafkaTopicContract,
) -> nafka::Result<()> {
    let suffix = &kafka.config().behavior.dead_letter_topic_suffix;
    if suffix.is_empty() {
        return Ok(());
    }
    let names: std::collections::BTreeSet<&str> = contract
        .dead_letters
        .iter()
        .map(|spec| spec.name.as_str())
        .collect();
    for source in [&config.control_topic, &config.data_topic] {
        let expected = format!("{source}{suffix}");
        if !names.contains(expected.as_str()) {
            return Err(NafkaError::TopicContract(format!(
                "TopicContract 缺少 runtime DLT topic `{expected}`"
            )));
        }
    }
    Ok(())
}

/// 核算各物理 native queue 与 DLT backlog 的显式内存上限。
fn validate_memory_budget(kafka: &KafkaProxy, config: &WsKafkaRuntimeConfig) -> nafka::Result<()> {
    let mut names = std::collections::BTreeSet::new();
    names.insert(config.control_lane.as_str());
    names.insert(config.data_lane.as_str());
    names.insert(config.dlt_lane.as_str());
    let mut total = kafka.config().behavior.dlt_queue_max_bytes;
    for name in names {
        let queue_kbytes = effective_queue_kbytes(kafka.config(), name);
        total = total
            .checked_add(queue_kbytes.checked_mul(1024).ok_or_else(|| {
                NafkaError::Config(format!("producer lane `{name}` queue 字节预算溢出"))
            })?)
            .ok_or_else(|| NafkaError::Config("Kafka 内存预算求和溢出".into()))?;
    }
    if total > config.kafka_memory_budget_bytes {
        return Err(NafkaError::Config(format!(
            "Kafka queue+DLT 预算({total}) 超过 kafka_memory_budget_bytes({})",
            config.kafka_memory_budget_bytes
        )));
    }
    Ok(())
}

/// 返回一个 lane 生效的 native queue KiB 上限。
fn effective_queue_kbytes(config: &KafkaConfig, lane: &str) -> usize {
    if lane == "default" {
        return config.producer.queue_buffering_max_kbytes;
    }
    config
        .producer
        .lanes
        .get(lane)
        .and_then(|item| item.queue_buffering_max_kbytes)
        .unwrap_or(config.producer.queue_buffering_max_kbytes)
}

/// 校验生产 lane 的 durability 与显式容量覆盖。
fn validate_production_lane(
    config: &KafkaConfig,
    lane: &str,
    required_message_bytes: usize,
) -> nafka::Result<()> {
    let (acks, idempotence, explicit_capacity) = if lane == "default" {
        (
            config.producer.acks.as_str(),
            config.producer.enable_idempotence,
            false,
        )
    } else {
        let item = config
            .producer
            .lanes
            .get(lane)
            .ok_or_else(|| NafkaError::Config(format!("生产 producer lane `{lane}` 未配置")))?;
        (
            item.acks.as_deref().unwrap_or(&config.producer.acks),
            item.enable_idempotence
                .unwrap_or(config.producer.enable_idempotence),
            item.queue_buffering_max_messages.is_some()
                && item.queue_buffering_max_kbytes.is_some()
                && item.fire_observer_capacity.is_some(),
        )
    };
    if acks != "all" || !idempotence {
        return Err(NafkaError::Config(format!(
            "生产 producer lane `{lane}` 必须 acks=all 且 enable_idempotence=true"
        )));
    }
    if !explicit_capacity {
        return Err(NafkaError::Config(format!(
            "生产 producer lane `{lane}` 必须显式配置 native queue 与 observer 容量"
        )));
    }
    let message_max = effective_native_property(config, lane, "message.max.bytes")
        .ok_or_else(|| {
            NafkaError::Config(format!(
                "生产 producer lane `{lane}` 必须显式配置 message.max.bytes"
            ))
        })?
        .parse::<usize>()
        .map_err(|_| {
            NafkaError::Config(format!(
                "生产 producer lane `{lane}` message.max.bytes 不是有效 usize"
            ))
        })?;
    if message_max < required_message_bytes {
        return Err(NafkaError::Config(format!(
            "生产 producer lane `{lane}` message.max.bytes({message_max}) 小于 topic 契约({required_message_bytes})"
        )));
    }
    Ok(())
}

/// 按 common→producer→lane 覆盖顺序读取一个 producer 原生属性。
fn effective_native_property<'a>(
    config: &'a KafkaConfig,
    lane: &str,
    key: &str,
) -> Option<&'a str> {
    config
        .producer
        .lanes
        .get(lane)
        .and_then(|item| item.properties.get(key))
        .or_else(|| config.producer.properties.get(key))
        .or_else(|| config.properties.get(key))
        .map(String::as_str)
}

/// 从 nafka 与 socket 配置生成 topic 容量核验上下文。
fn contract_limits(kafka: &KafkaProxy, config: &WsKafkaRuntimeConfig) -> TopicContractLimits {
    TopicContractLimits {
        full_presence_interval_ms: config.full_presence_interval_ms,
        max_record_bytes: kafka.config().behavior.max_record_bytes,
        max_route_header_bytes: config.passthrough.max_route_header_bytes,
        protocol_header_budget_bytes: config.protocol_header_budget_bytes,
        max_assignment_partitions: kafka.config().consumer.max_assignment_partitions,
    }
}

/// 返回稳定生命周期名称。
fn state_name(state: RuntimeState) -> &'static str {
    match state {
        RuntimeState::Created => "Created",
        RuntimeState::Starting => "Starting",
        RuntimeState::Running => "Running",
        RuntimeState::ShuttingDown => "ShuttingDown",
        RuntimeState::Stopped => "Stopped",
        RuntimeState::Failed => "Failed",
    }
}
