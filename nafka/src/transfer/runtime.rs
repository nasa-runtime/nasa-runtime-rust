//! 单目标 at-least-once 转发；目的确认先于来源持久进度。

use super::{TransferError, TransferPayloadPolicy, TransferPlan, TransferResult, TransferTrace};
use crate::replay::runtime::ReplayParticipant;
use crate::{
    CheckpointStore, KafkaProxy, KafkaProxyInner, NafkaError, ProducerLane, ReplayError,
    ReplayHandle, ReplayIdentityProvider, ReplayProcessor, ReplayRecord, ReplayResult,
    ReplayStatus, Tp,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

/// 显式 Registry 能力绑定；宿主必须在实际转发退出后才能关闭这些客户端。
#[derive(Clone)]
pub struct TransferRegistries {
    /// 稳定来源 Registry 名，必须与计划完全一致。
    pub source_name: String,
    /// 稳定目标 Registry 名，必须与计划完全一致。
    pub destination_name: String,
    /// 来源客户端，具有独立认证与缓存；转发不拥有它的关闭权。
    #[cfg(feature = "schema-registry")]
    pub source: Arc<dyn crate::SchemaRegistryClient>,
    /// 目标客户端；转发只查询，不执行自动注册或演进。
    #[cfg(feature = "schema-registry")]
    pub destination: Arc<dyn crate::SchemaRegistryClient>,
}

/// 目标实际确认与来源持久进度分开呈现，确认数量不能替代 checkpoint。
#[derive(Clone, Debug)]
pub struct TransferStatus {
    /// 来源执行、所有权和持久进度。
    pub replay: ReplayStatus,
    /// 首个目标处理失败，不包含消息正文或凭据。
    pub failure: Option<TransferError>,
    /// 本实例目标收到成功回执的数量；可领先于持久进度。
    pub delivered_records: u64,
}
impl TransferStatus {
    /// 业务作用：区分任务取消请求与所有实际资源退出。
    /// 参数说明：无。返回：实际 worker、native 交付和 lease 全部退出才为 true。
    pub fn drained(&self) -> bool {
        self.replay.drained()
    }
}

/// 同时受来源和目标停机约束的转发控制句柄。
#[derive(Clone)]
pub struct TransferHandle {
    replay: ReplayHandle,
    processor: Arc<Processor>,
}
impl TransferHandle {
    /// 业务作用：暂停后续转发准入，保留已经接纳的实际交付。
    /// 参数说明：无。返回：包含来源进度与目标确认数的即时快照。
    pub fn pause(&self) -> TransferStatus {
        self.replay.pause();
        self.status()
    }
    /// 业务作用：恢复未取消或失败的转发任务。
    /// 参数说明：无。返回：终态与停止任务不会重新开放。
    pub fn resume(&self) -> TransferStatus {
        self.replay.resume();
        self.status()
    }
    /// 业务作用：关闭下一条转发准入，不撤销或遗弃在途 native 交付。
    /// 参数说明：无。返回：停止状态，不能据此断言来源 lease 已释放。
    pub fn cancel(&self) -> TransferStatus {
        self.replay.cancel();
        self.status()
    }
    /// 业务作用：无需网络读取两端执行和持久责任。
    /// 参数说明：无。返回：脱敏即时快照，目标确认与持久进度可能不同。
    pub fn status(&self) -> TransferStatus {
        TransferStatus {
            replay: self.replay.status(),
            failure: *self
                .processor
                .failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            delivered_records: self.processor.delivered.load(Ordering::Acquire),
        }
    }
    /// 业务作用：等待实际转发退出，取消等待和超时均不改变后台责任。
    /// 参数说明：timeout 为本次预算，零只查询。返回：实际退出为 true，否则继续保留双方登记与 lease。
    pub async fn await_drained(&self, timeout: Duration) -> bool {
        self.replay.await_drained(timeout).await
    }
}

impl KafkaProxy {
    /// 业务作用：按固定合同将有限来源范围原文转发到单一目标，目标成功后才推进来源 checkpoint。
    /// 参数说明：plan 为共同计划，destination 为固定目标源，store 为独占持久后端，source_identity/destination_identity 为当前代次目录，registries 为 Confluent 策略的显式绑定。
    /// 返回：双方受管的控制句柄；不可靠 lane、错误绑定或已关闭来源在副作用前拒绝。
    pub fn transfer(
        &self,
        plan: TransferPlan,
        destination: &KafkaProxy,
        store: Arc<dyn CheckpointStore>,
        source_identity: Arc<dyn ReplayIdentityProvider>,
        destination_identity: Arc<dyn ReplayIdentityProvider>,
        registries: Option<TransferRegistries>,
    ) -> TransferResult<TransferHandle> {
        plan.validate()?;
        if plan.replay.source_id != self.config().client_name
            || plan.target.source_id != destination.config().client_name
        {
            return Err(TransferError::ContractMismatch);
        }
        let delivery = Delivery::new(
            plan.target.clone(),
            plan.payload_policy.clone(),
            destination,
            destination_identity,
            registries,
        )?;
        let processor = Arc::new(Processor {
            routes: plan
                .routes
                .iter()
                .map(|r| (r.source.clone(), r.destination.clone()))
                .collect(),
            plan: plan.clone(),
            delivery,
            failure: Mutex::new(None),
            delivered: AtomicU64::new(0),
        });
        // 先占目标责任，来源 coordinator 必须等到句柄绑定才开放；任一端并发停机都不能漏掉任务。
        let dependency = destination.inner.transfers.reserve(
            &destination.inner,
            format!("{}::{}", plan.replay.source_id, plan.replay.task_id),
            Arc::clone(&processor.delivery.inspection),
        )?;
        let replay = match self.replay_participating(
            plan.replay,
            store,
            source_identity,
            processor.clone(),
            Some(dependency.clone()),
        ) {
            Ok(handle) => handle,
            Err(error) => {
                dependency.finished();
                return Err(replay_error(error));
            }
        };
        dependency.bind(replay.clone());
        Ok(TransferHandle { replay, processor })
    }
}

struct Processor {
    plan: TransferPlan,
    delivery: Delivery,
    routes: BTreeMap<Tp, Tp>,
    failure: Mutex<Option<TransferError>>,
    delivered: AtomicU64,
}
impl ReplayProcessor for Processor {
    /// 业务作用：将固定目标成功合同绑定到来源持久任务。
    /// 参数说明：无。返回：与计划中目的、策略和映射相同的共同指纹。
    fn fingerprint(&self) -> String {
        self.plan.replay.pipeline_fingerprint.clone()
    }
    /// 业务作用：同步完成一条真实转发，失败不允许推进来源进度。
    /// 参数说明：record 为本次取得来源责任的原文。返回：只有真实目标确认才成功，失败独立保留目标类别。
    fn process(&self, record: ReplayRecord) -> ReplayResult<()> {
        match crate::replay::runtime::isolate(|| self.forward(record))
            .unwrap_or(Err(TransferError::ObservationFailed))
        {
            Ok(()) => Ok(()),
            Err(error) => {
                self.failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get_or_insert(error);
                Err(ReplayError::ProcessingFailed)
            }
        }
    }
}
impl Processor {
    /// 业务作用：复验来源路径、schema 与目的资源，并持有同步责任直到 native 实际回执。
    /// 参数说明：record 为原始来源。返回：目标交付成功后增加确认数；不自行写来源 checkpoint。
    fn forward(&self, record: ReplayRecord) -> TransferResult<()> {
        let destination = self
            .routes
            .get(&Tp {
                topic: record.topic.clone(),
                partition: record.partition,
            })
            .ok_or(TransferError::ContractMismatch)?;
        if record.cluster_id != self.plan.replay.cluster_id {
            return Err(TransferError::ContractMismatch);
        }
        let headers = TransferTrace::forward(&self.plan, &record, destination)?;
        self.delivery.send(&record, destination, &headers, true)?;
        self.delivered.fetch_add(1, Ordering::Release);
        Ok(())
    }
}

/// 一项有限任务共用一个只读目标客户端；实际查询串行，实时证据不跨消息复用。
#[derive(Default)]
pub(crate) struct TargetInspection {
    consumer: Mutex<Option<rdkafka::consumer::BaseConsumer>>,
}
impl TargetInspection {
    /// 业务作用：在同一总期限内独占探测连接并重新取得目标完整证据。
    /// 参数说明：config 为目标冻结配置，topics 为批准主题，deadline 包含等待连接使用权与全部查询。
    /// 返回：实时物理身份、分区和LSO快照；超时、权限或不完整证据均拒绝本次发送。
    fn observe(
        &self,
        config: &crate::KafkaConfig,
        topics: &std::collections::BTreeSet<String>,
        deadline: Instant,
    ) -> TransferResult<crate::ReplayObservation> {
        let mut slot = loop {
            remaining(deadline)?;
            match self.consumer.try_lock() {
                Ok(slot) => break slot,
                Err(std::sync::TryLockError::Poisoned(error)) => break error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => {
                    std::thread::sleep(remaining(deadline)?.min(Duration::from_millis(1)));
                }
            }
        };
        // 只在任务第一次使用时建连；后续仍查询真实broker，不能以连接复用绕过资源复验。
        if slot.is_none() {
            *slot =
                Some(crate::replay::inspection_consumer(config, deadline).map_err(replay_error)?);
        }
        crate::replay::inspect_consumer(
            slot.as_ref().expect("探测客户端已创建"),
            topics,
            None,
            deadline,
        )
        .map_err(replay_error)
    }

    /// 业务作用：在全部任务worker退出后销毁探测资源，与外部控制句柄寿命解耦。
    /// 参数说明：无。
    /// 返回：原生析构实际返回后才结束；重复关闭不创建或恢复连接。
    pub(crate) fn close(&self) {
        let client = self
            .consumer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(client);
    }
}

/// 共享目标交付责任，不持有来源checkpoint写权限。
pub(crate) struct Delivery {
    target: super::TransferTarget,
    policy: TransferPayloadPolicy,
    destination: Weak<KafkaProxyInner>,
    lane: ProducerLane,
    identity: Arc<dyn ReplayIdentityProvider>,
    runtime: tokio::runtime::Handle,
    /// 只复用连接，不缓存物理身份或范围证据；任务实际退出前持有独立关闭责任。
    pub(crate) inspection: Arc<TargetInspection>,
    #[cfg(feature = "schema-registry")]
    registries: Option<TransferRegistries>,
}
impl Delivery {
    /// 业务作用：固定可靠发布能力，在开放任务前拒绝目标或schema错绑。
    /// 参数说明：target/policy为目标合同，destination为实际来源，identity为代次目录，registries为显式schema能力。
    /// 返回：尚未发送的目标交付器；不可靠lane或缺失能力立即拒绝。
    pub(crate) fn new(
        target: super::TransferTarget,
        policy: TransferPayloadPolicy,
        destination: &KafkaProxy,
        identity: Arc<dyn ReplayIdentityProvider>,
        registries: Option<TransferRegistries>,
    ) -> TransferResult<Self> {
        if target.source_id != destination.config().client_name {
            return Err(TransferError::ContractMismatch);
        }
        let effective =
            crate::rd::config::effective_producer_lane(destination.config(), &target.lane)
                .map_err(|_| TransferError::ContractMismatch)?;
        if !matches!(effective.acks.as_str(), "all" | "-1") {
            return Err(TransferError::UnconfirmedLane);
        }
        if let TransferPayloadPolicy::Confluent {
            source_registry,
            destination_registry,
            ..
        } = &policy
        {
            if !cfg!(feature = "schema-registry")
                || registries.as_ref().is_none_or(|bindings| {
                    bindings.source_name != *source_registry
                        || bindings.destination_name != *destination_registry
                })
            {
                return Err(TransferError::ContractMismatch);
            }
        }
        let lane = destination
            .producer_lanes()
            .get(&target.lane)
            .ok_or(TransferError::ContractMismatch)?
            .clone();
        Ok(Self {
            target,
            policy,
            destination: Arc::downgrade(&destination.inner),
            lane,
            identity,
            runtime: destination.inner.runtime.clone(),
            inspection: Arc::new(TargetInspection::default()),
            #[cfg(feature = "schema-registry")]
            registries,
        })
    }
    /// 业务作用：复验目标并等待实际交付，原文隔离不执行schema解释。
    /// 参数说明：record为输入，destination为批准分区，headers为完整证据，map_payload选择schema映射。
    /// 返回：只有native真实成功才返回；不写来源checkpoint，取消等待不解除责任。
    pub(crate) fn send(
        &self,
        record: &ReplayRecord,
        destination: &Tp,
        headers: &crate::KafkaHeaders,
        map_payload: bool,
    ) -> TransferResult<()> {
        if !self.target.partitions.contains(destination) {
            return Err(TransferError::ContractMismatch);
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        let payload = if map_payload {
            self.payload(record.payload.as_deref(), deadline)?
        } else {
            record.payload.clone()
        };
        let owner = self.destination.upgrade().ok_or(TransferError::Closed)?;
        if owner.managed_admin_closed.load(Ordering::Acquire) {
            return Err(TransferError::Closed);
        }
        let topics = self.target.topic_generations.keys().cloned().collect();
        // 紧邻发送重新取得完整目的证据；启动时的旧 metadata 不能授权当前消息的去向。
        let current = self.inspection.observe(&owner.config, &topics, deadline)?;
        let generations = self
            .identity
            .generations(
                &current.cluster_id,
                &self.target.topic_generations.keys().cloned().collect(),
                remaining(deadline)?,
            )
            .map_err(replay_error)?;
        remaining(deadline)?;
        self.target.validate_current(&current, &generations)?;
        // 此 worker 不会被任务取消打断；native 入队后不附加超时，保留双方登记与 lease 到实际终态。
        self.runtime
            .block_on(
                self.lane
                    .send_transfer(crate::rd::producer::OutboundRecord {
                        topic: &destination.topic,
                        partition: Some(destination.partition),
                        key: record.key.as_deref(),
                        payload: payload.as_deref(),
                        timestamp: (record.timestamp >= 0).then_some(record.timestamp),
                        headers,
                    }),
            )
            .map_err(|error| match error {
                NafkaError::Authorization => TransferError::Authorization,
                NafkaError::OutcomeUnknown { .. } => TransferError::OutcomeUnknown,
                NafkaError::Lifecycle(_) => TransferError::Closed,
                NafkaError::ProducerQueueFull { .. } => TransferError::Capacity,
                _ => TransferError::DeliveryFailed,
            })?;
        Ok(())
    }

    /// 业务作用：原文模式保持字节，明确 Confluent 模式只替换批准且双端 schema 一致的 ID。
    /// 参数说明：payload 为可空来源值，deadline 为 schema 与目的复验共用截止时间。
    /// 返回：独立目标正文，tombstone 保持 None；不自动注册或推断 schema。
    fn payload(
        &self,
        payload: Option<&[u8]>,
        deadline: Instant,
    ) -> TransferResult<Option<Vec<u8>>> {
        let Some(payload) = payload else {
            return Ok(None);
        };
        if matches!(self.policy, TransferPayloadPolicy::Opaque) {
            return Ok(Some(payload.to_vec()));
        }
        self.confluent(payload, deadline).map(Some)
    }

    /// 业务作用：在未启用 Registry 能力的构建中拒绝解释 Confluent 原文。
    /// 参数说明：payload/deadline 为已登记转发的输入与预算。返回：明确合同拒绝，不执行网络或发送。
    #[cfg(not(feature = "schema-registry"))]
    fn confluent(&self, _payload: &[u8], _deadline: Instant) -> TransferResult<Vec<u8>> {
        Err(TransferError::ContractMismatch)
    }

    /// 业务作用：用两个明确 Registry 的相同 schema 正文证明批准 ID 映射，保留 envelope 剩余字节。
    /// 参数说明：payload 为 Confluent 原文，deadline 为本条总验证预算。返回：只替换四字节 ID 的正文或稳定拒绝。
    #[cfg(feature = "schema-registry")]
    fn confluent(&self, payload: &[u8], deadline: Instant) -> TransferResult<Vec<u8>> {
        use crate::{RegistrySchemaType, SchemaId};
        use sha2::{Digest, Sha256};
        let TransferPayloadPolicy::Confluent { schemas, .. } = &self.policy else {
            return Err(TransferError::ContractMismatch);
        };
        if payload.len() < 5 || payload[0] != 0 {
            return Err(TransferError::SchemaUnapproved);
        }
        let id = i32::from_be_bytes(
            payload[1..5]
                .try_into()
                .map_err(|_| TransferError::SchemaUnapproved)?,
        );
        let mapping = schemas
            .iter()
            .find(|mapping| mapping.source_id == id)
            .ok_or(TransferError::SchemaUnapproved)?;
        let bindings = self
            .registries
            .as_ref()
            .ok_or(TransferError::ContractMismatch)?;
        let source = self.runtime.block_on(async {
            tokio::time::timeout(
                remaining(deadline)?,
                bindings
                    .source
                    .schema_by_id(SchemaId::new(id).map_err(|_| TransferError::SchemaUnapproved)?),
            )
            .await
            .map_err(|_| TransferError::Timeout)?
            .map_err(registry_error)
        })?;
        let target = self.runtime.block_on(async {
            tokio::time::timeout(
                remaining(deadline)?,
                bindings.destination.schema_by_id(
                    SchemaId::new(mapping.destination_id)
                        .map_err(|_| TransferError::SchemaUnapproved)?,
                ),
            )
            .await
            .map_err(|_| TransferError::Timeout)?
            .map_err(registry_error)
        })?;
        let kind = match mapping.kind {
            super::TransferSchemaKind::Avro => RegistrySchemaType::Avro,
            super::TransferSchemaKind::Protobuf => RegistrySchemaType::Protobuf,
            super::TransferSchemaKind::Json => RegistrySchemaType::Json,
        };
        if source.id.get() != id
            || target.id.get() != mapping.destination_id
            || source.schema_type != kind
            || target.schema_type != kind
            || format!("{:x}", Sha256::digest(source.schema.as_bytes()))
                != mapping.schema_fingerprint
            || format!("{:x}", Sha256::digest(target.schema.as_bytes()))
                != mapping.schema_fingerprint
        {
            return Err(TransferError::SchemaMismatch);
        }
        let mut output = payload.to_vec();
        output[1..5].copy_from_slice(&mapping.destination_id.to_be_bytes());
        Ok(output)
    }
}
/// 业务作用：使 schema 和目的目录共享一次有限验证预算。
/// 参数说明：deadline 为单调截止时间。返回：正剩余预算，耗尽不再发起新操作。
fn remaining(deadline: Instant) -> TransferResult<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(TransferError::Timeout)
    } else {
        Ok(remaining)
    }
}
/// 业务作用：把来源登记或目的查询失败归为稳定、脱敏的转发类别。
/// 参数说明：error 为原始回放错误。返回：关闭、超时、容量和合同保留独立含义。
pub(crate) fn replay_error(error: ReplayError) -> TransferError {
    match error {
        ReplayError::Timeout => TransferError::Timeout,
        ReplayError::Authorization => TransferError::Authorization,
        ReplayError::Closed => TransferError::Closed,
        ReplayError::Capacity | ReplayError::Busy => TransferError::Capacity,
        ReplayError::ContractMismatch => TransferError::ContractMismatch,
        ReplayError::InvalidConfiguration => TransferError::InvalidConfiguration,
        _ => TransferError::ObservationFailed,
    }
}
/// 业务作用：保留 Registry 不可观测与 schema 不一致的区别，避免网络故障冒充正文不匹配。
/// 参数说明：error 为封闭 Registry 结局。返回：目标处理的稳定类别。
#[cfg(feature = "schema-registry")]
fn registry_error(error: crate::SchemaRegistryError) -> TransferError {
    match error {
        crate::SchemaRegistryError::Closed => TransferError::Closed,
        crate::SchemaRegistryError::Capacity => TransferError::Capacity,
        crate::SchemaRegistryError::Transport | crate::SchemaRegistryError::RemoteStatus(_) => {
            TransferError::ObservationFailed
        }
        _ => TransferError::SchemaMismatch,
    }
}
