//! 有限DLT回投；目标真实确认或显式过滤先于来源持久进度。

use super::{
    DltEvidence, DltInvalidPolicy, DltQuarantineTrace, DltReplayError as Error, DltReplayPlan,
    DltReplayResult as Result, DltReplayTrace,
};
use crate::replay::runtime::ReplayParticipant;
use crate::{
    CheckpointStore, KafkaProxy, ReplayError, ReplayHandle, ReplayIdentityProvider,
    ReplayProcessor, ReplayRecord, ReplayResult, ReplayStatus, Tp, TransferError,
    TransferRegistries, TransferTrace,
};
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

/// 计数属于本次执行实例，不能替代来源checkpoint或跨崩溃账本。
#[derive(Clone, Debug)]
pub struct DltReplayStatus {
    /// 持久来源进度及实际任务责任。
    pub replay: ReplayStatus,
    /// 首个稳定失败类别。
    pub failure: Option<Error>,
    /// 本实例回投真实确认数。
    pub forwarded_records: u64,
    /// 经过完整证据验证后的明确过滤数。
    pub skipped_records: u64,
    /// 原文隔离真实确认数。
    pub quarantined_records: u64,
}
impl DltReplayStatus {
    /// 业务作用：区分停止请求与来源、目标和lease实际退出。
    /// 参数说明：无。返回：所有实际责任排干才为true。
    pub fn drained(&self) -> bool {
        self.replay.drained()
    }
}
/// 同时受来源和目标关闭约束的一次DLT执行句柄。
#[derive(Clone)]
pub struct DltReplayHandle {
    replay: ReplayHandle,
    processor: Arc<Processor>,
}
impl DltReplayHandle {
    /// 业务作用：暂停后续准入，保留在途交付责任。
    /// 参数说明：无。返回：即时状态，不改变冻结来源范围。
    pub fn pause(&self) -> DltReplayStatus {
        self.replay.pause();
        self.status()
    }
    /// 业务作用：恢复尚未停止的任务。
    /// 参数说明：无。返回：即时状态，终态不能重开。
    pub fn resume(&self) -> DltReplayStatus {
        self.replay.resume();
        self.status()
    }
    /// 业务作用：永久关闭本实例后续准入，不遗弃实际native回执。
    /// 参数说明：无。返回：停止中或已排干状态。
    pub fn cancel(&self) -> DltReplayStatus {
        self.replay.cancel();
        self.status()
    }
    /// 业务作用：无需网络读取持久责任与本实例处理结果。
    /// 参数说明：无。返回：脱敏快照，确认计数可能领先于checkpoint。
    pub fn status(&self) -> DltReplayStatus {
        DltReplayStatus {
            replay: self.replay.status(),
            failure: *self
                .processor
                .failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            forwarded_records: self.processor.forwarded.load(Ordering::Acquire),
            skipped_records: self.processor.skipped.load(Ordering::Acquire),
            quarantined_records: self.processor.quarantined.load(Ordering::Acquire),
        }
    }
    /// 业务作用：等待真实任务与lease退出，超时不能解除后台责任。
    /// 参数说明：timeout为本次预算，零只查询。返回：实际排干才为true。
    pub async fn await_drained(&self, timeout: Duration) -> bool {
        self.replay.await_drained(timeout).await
    }
}
impl KafkaProxy {
    /// 业务作用：按固定来源过滤和批准映射回投有限DLT，不修改线上group位点。
    /// 参数说明：plan为共同合同，destination为固定目标源，store为独占后端，source_identity/destination_identity为资源代次目录，registries为可空schema能力。
    /// 返回：双方受管句柄；不可靠lane或错误绑定在副作用前拒绝。
    pub fn replay_dlt(
        &self,
        plan: DltReplayPlan,
        destination: &KafkaProxy,
        store: Arc<dyn CheckpointStore>,
        source_identity: Arc<dyn ReplayIdentityProvider>,
        destination_identity: Arc<dyn ReplayIdentityProvider>,
        registries: Option<TransferRegistries>,
    ) -> Result<DltReplayHandle> {
        plan.validate()?;
        if plan.replay.source_id != self.config().client_name {
            return Err(Error::ContractMismatch);
        }
        let delivery = crate::transfer::Delivery::new(
            plan.target.clone(),
            plan.payload_policy.clone(),
            destination,
            destination_identity,
            registries,
        )
        .map_err(transfer_error)?;
        let processor = Arc::new(Processor {
            routes: plan
                .routes
                .iter()
                .map(|r| (r.source.clone(), r.destination.clone()))
                .collect(),
            plan: plan.clone(),
            delivery,
            failure: Mutex::new(None),
            forwarded: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            quarantined: AtomicU64::new(0),
        });
        // 目标先持有待激活责任；来源只有在绑定完成后开放，双方并发停机均不会遗漏任务。
        let dependency = destination
            .inner
            .transfers
            .reserve(
                &destination.inner,
                format!("{}::{}", plan.replay.source_id, plan.replay.task_id),
                Arc::clone(&processor.delivery.inspection),
            )
            .map_err(transfer_error)?;
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
                return Err(transfer_error(crate::transfer::replay_error(error)));
            }
        };
        dependency.bind(replay.clone());
        Ok(DltReplayHandle { replay, processor })
    }
}
struct Processor {
    plan: DltReplayPlan,
    delivery: crate::transfer::Delivery,
    routes: BTreeMap<Tp, Tp>,
    failure: Mutex<Option<Error>>,
    forwarded: AtomicU64,
    skipped: AtomicU64,
    quarantined: AtomicU64,
}
impl ReplayProcessor for Processor {
    /// 业务作用：把来源持久任务绑定到完整DLT处理策略。
    /// 参数说明：无。返回：冻结计划的共同摘要。
    fn fingerprint(&self) -> String {
        self.plan.replay.pipeline_fingerprint.clone()
    }
    /// 业务作用：同步完成回投、确定过滤或真实隔离，失败阻止checkpoint推进。
    /// 参数说明：record为持有来源责任的原文。返回：只接受确定成功，首个失败保留稳定分类。
    fn process(&self, record: ReplayRecord) -> ReplayResult<()> {
        match crate::replay::runtime::isolate(|| self.process_record(&record))
            .unwrap_or(Err(Error::ObservationFailed))
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
    /// 业务作用：只允许确定输入拒绝由原文隔离承接，防止交付未知被误记成功。
    /// 参数说明：record为实际DLT输入。返回：隔离必须收到真实确认，否则保留来源失败坐标。
    fn process_record(&self, record: &ReplayRecord) -> Result<()> {
        if record.cluster_id != self.plan.replay.cluster_id {
            return Err(Error::ContractMismatch);
        }
        match self.forward(record) {
            Err(error) if DltQuarantineTrace::eligible(error) => {
                let DltInvalidPolicy::Quarantine(target) = &self.plan.invalid_policy else {
                    return Err(error);
                };
                let headers = DltQuarantineTrace::append(&self.plan, record, error)?;
                self.delivery
                    .send(record, target, &headers, false)
                    .map_err(transfer_error)?;
                self.quarantined.fetch_add(1, Ordering::Release);
                Ok(())
            }
            result => result,
        }
    }
    /// 业务作用：先验证完整证据再按原业务来源过滤，头字段不能直接授权路由。
    /// 参数说明：record为实际DLT记录。返回：过滤确认或真实回投成功；无效记录不得由过滤隐式丢弃。
    fn forward(&self, record: &ReplayRecord) -> Result<()> {
        let evidence = DltEvidence::read(&record.headers)?.ok_or(Error::MissingEvidence)?;
        DltReplayTrace::read(&record.headers)?;
        TransferTrace::read(&record.headers).map_err(|_| Error::HeaderInvalid)?;
        // 隔离原文需要业务显式处置，自动回流会丢失原失败责任。
        if DltQuarantineTrace::read(&record.headers)?.is_some() {
            return Err(Error::HeaderInvalid);
        }
        let target = self.routes.get(&Tp {
            topic: evidence.origin_topic.clone(),
            partition: evidence.origin_partition,
        });
        if target.is_none()
            || !self.plan.origin_groups.is_empty()
                && !self.plan.origin_groups.contains(&evidence.origin_group)
        {
            self.skipped.fetch_add(1, Ordering::Release);
            return Ok(());
        }
        let target = target.ok_or(Error::ContractMismatch)?;
        let headers = DltReplayTrace::forward(&self.plan, record, evidence, target)?;
        self.delivery
            .send(record, target, &headers, true)
            .map_err(transfer_error)?;
        self.forwarded.fetch_add(1, Ordering::Release);
        Ok(())
    }
}
/// 业务作用：保留共享目标交付的稳定失败类别，DLT不放松原Transfer路径规则。
/// 参数说明：error为转发错误。返回：对应DLT错误，路径证据错误统一为HeaderInvalid。
fn transfer_error(error: TransferError) -> Error {
    match error {
        TransferError::Authorization => Error::Authorization,
        TransferError::InvalidConfiguration => Error::InvalidConfiguration,
        TransferError::CorruptPlan => Error::CorruptPlan,
        TransferError::ContractMismatch => Error::ContractMismatch,
        TransferError::TargetIdentityChanged => Error::TargetIdentityChanged,
        TransferError::TargetPartitionsChanged => Error::TargetPartitionsChanged,
        TransferError::ObservationFailed => Error::ObservationFailed,
        TransferError::Timeout => Error::Timeout,
        TransferError::UnconfirmedLane => Error::UnconfirmedLane,
        TransferError::SchemaUnapproved => Error::SchemaUnapproved,
        TransferError::SchemaMismatch => Error::SchemaMismatch,
        TransferError::HeaderInvalid | TransferError::LoopDetected | TransferError::HopLimit => {
            Error::HeaderInvalid
        }
        TransferError::DeliveryFailed => Error::DeliveryFailed,
        TransferError::OutcomeUnknown => Error::OutcomeUnknown,
        TransferError::Closed => Error::Closed,
        TransferError::Capacity => Error::Capacity,
    }
}
