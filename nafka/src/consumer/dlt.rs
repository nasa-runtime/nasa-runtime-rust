//! 有界 DLT dispatcher；统一约束保留记录数、信封字节和实际并行投递数。

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::{NafkaError, Result};
use crate::producer::ProducerLane;
use crate::types::KafkaHeaders;

/// 一个连续失败单元内的单条拥有型死信记录。
pub(crate) struct DltJob {
    /// 按来源 offset 顺序排列的待投递记录。
    records: Vec<DltRecord>,
    /// 全 pipeline 字节 permit 口径。
    retained_bytes: u32,
}

/// 一条拥有型死信记录。
pub(crate) struct DltRecord {
    /// 目标死信 topic。
    pub(crate) topic: String,
    /// 原始可空 payload。
    pub(crate) payload: Option<Vec<u8>>,
    /// 原始可空 key。
    pub(crate) key: Option<Vec<u8>>,
    /// 原始 headers 与框架追加来源 headers。
    pub(crate) headers: KafkaHeaders,
    /// 原始 broker 时间戳；缺失时不向 DLT 记录写时间戳。
    pub(crate) timestamp: Option<i64>,
}

impl DltJob {
    /// 业务作用：构造单记录任务并计算保留字节。
    ///
    /// # 参数
    ///
    /// - `topic`: 目标死信 topic。
    /// - `payload`: 原始 payload。
    /// - `key`: 原始 key。
    /// - `headers`: 完整有序 headers。
    /// - `timestamp`: 原始 broker 时间戳；`None` 表示不可用。
    ///
    /// # 错误
    ///
    /// retained bytes 超过 semaphore 可表达范围时返回配置错误。
    #[allow(dead_code)]
    pub(crate) fn new(
        topic: String,
        payload: Option<Vec<u8>>,
        key: Option<Vec<u8>>,
        headers: KafkaHeaders,
        timestamp: Option<i64>,
    ) -> Result<Self> {
        Self::from_records(vec![DltRecord {
            topic,
            payload,
            key,
            headers,
            timestamp,
        }])
    }

    /// 业务作用：构造连续失败 run 的单个有界任务。
    ///
    /// # 参数
    ///
    /// - `records`: 按来源 offset 顺序排列的非空死信记录。
    ///
    /// # 错误
    ///
    /// 输入为空、累计 retained bytes 溢出或超过 semaphore 表达范围时返回错误。
    pub(crate) fn from_records(records: Vec<DltRecord>) -> Result<Self> {
        if records.is_empty() {
            return Err(NafkaError::Lifecycle("DLT job 不能是空 run".into()));
        }
        let retained = records
            .iter()
            .try_fold(0_usize, |job_total, record| {
                let record_total = record
                    .payload
                    .as_ref()
                    .map_or(0, Vec::len)
                    .checked_add(record.key.as_ref().map_or(0, Vec::len))
                    .and_then(|total| {
                        record.headers.iter().try_fold(total, |total, header| {
                            total.checked_add(header.name.len()).and_then(|total| {
                                total.checked_add(header.value.as_ref().map_or(0, Vec::len))
                            })
                        })
                    })?
                    // 来源扩展在实际发送前取得；准入时预留其最大字节，避免绕过全pipeline额度。
                    .checked_add(
                        2048 + crate::HEADER_DLT_ORIGIN_SOURCE.len()
                            + crate::HEADER_DLT_ORIGIN_CLUSTER.len(),
                    )?;
                job_total.checked_add(record_total)
            })
            .ok_or_else(|| NafkaError::Config("单个 DLT job retained bytes 计算溢出".into()))?;
        let retained_bytes = u32::try_from(retained)
            .map_err(|_| NafkaError::Config("单个 DLT job retained bytes 超过 u32".into()))?;
        Ok(Self {
            records,
            retained_bytes,
        })
    }
}

/// 全运行时共享的三重有界 DLT dispatcher。
#[derive(Clone)]
pub(crate) struct DltDispatcher {
    /// 弱关联所属源，观测Admin时升级，不形成源与dispatcher的引用环。
    owner: std::sync::Weak<crate::KafkaProxyInner>,
    /// 启动期固定的 DLT producer lane。
    lane: ProducerLane,
    /// 已准入信封内的记录总数 permit，批次不把多条记录折算成一份额度。
    jobs: Arc<Semaphore>,
    /// job semaphore 的冻结总容量。
    job_capacity: usize,
    /// waiting + in-flight retained bytes permit。
    bytes: Arc<Semaphore>,
    /// 单 job 请求前的显式字节上限，避免 acquire_many 永久等待。
    byte_capacity: u32,
    /// 实际 broker delivery 并行 permit。
    in_flight: Arc<Semaphore>,
    /// 包含已失去分配权威但尚未退出的实际任务，不能只依据 owner 的待完成表判断停机。
    workers: Arc<std::sync::atomic::AtomicUsize>,
    /// 仅为实际存活任务保留组计数，最后任务退出即删除身份。
    group_workers: Arc<std::sync::Mutex<std::collections::BTreeMap<String, usize>>>,
}

/// 源级实际任务责任；失权只撤销新发送，任务退出才归还。
pub(crate) struct DltWorkerGuard {
    dispatcher: DltDispatcher,
    group: String,
}

impl Drop for DltWorkerGuard {
    /// 业务作用：在死信任务及完成通知退出后归还源级关闭责任。
    /// 参数说明：无。返回：实际任务数减少一次，来源是否提交由 owner 独立裁决。
    fn drop(&mut self) {
        self.dispatcher
            .workers
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        let mut groups = self
            .dispatcher
            .group_workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = groups.get_mut(&self.group).expect("活动死信组已登记");
        *count -= 1;
        if let Some(owner) = self.dispatcher.owner.upgrade() {
            owner.gauge("dlt_in_flight", *count as i64, &[("group", &self.group)]);
        }
        if *count == 0 {
            groups.remove(&self.group);
        }
    }
}

/// 已取得全 pipeline job/byte 配额的死信信封。
pub(crate) struct DltEnvelope {
    /// 投递期间及外层重试之间始终持有的原始任务。
    job: DltJob,
    /// 从准入到最终收尾始终持有的记录数 permit。
    _job_permit: OwnedSemaphorePermit,
    /// 从准入到最终收尾始终持有的字节 permit。
    _byte_permit: OwnedSemaphorePermit,
}

/// 原文复制前取得的记录与字节责任，只有实际大小通过复验才能转成待交付信封。
pub(crate) struct DltReservation {
    records: usize,
    bytes: usize,
    record_permit: OwnedSemaphorePermit,
    byte_permit: OwnedSemaphorePermit,
}

impl DltReservation {
    /// 业务作用：把已预留责任与随后构造的信封绑定，阻止估算不足绕过字节边界。
    /// 参数说明：job 为取得额度之后物化的原文与来源证据。
    /// 返回：尺寸与记录数匹配时交付信封；不匹配时归还额度并保留来源未提交责任。
    pub(crate) fn attach(self, job: DltJob) -> Result<DltEnvelope> {
        if job.records.len() != self.records || job.retained_bytes as usize > self.bytes {
            return Err(NafkaError::Lifecycle("DLT 信封超过预留责任".into()));
        }
        Ok(DltEnvelope {
            job,
            _job_permit: self.record_permit,
            _byte_permit: self.byte_permit,
        })
    }
}

impl DltDispatcher {
    /// 业务作用：用冻结配置与 lane 构造 dispatcher。
    ///
    /// # 参数
    ///
    /// - `owner`: 所属源的共享内部责任。
    /// - `lane`: 预先解析的固定 DLT lane。
    /// - `job_capacity`: 全 pipeline 已准入的最大记录数。
    /// - `byte_capacity`: 全 pipeline 最大保留字节。
    /// - `max_in_flight`: 最大并行 delivery 数。
    ///
    /// # 错误
    ///
    /// 字节容量超过 semaphore 表达范围时返回配置错误。
    pub(crate) fn new(
        owner: &Arc<crate::KafkaProxyInner>,
        lane: ProducerLane,
        job_capacity: usize,
        byte_capacity: usize,
        max_in_flight: usize,
    ) -> Result<Self> {
        let byte_capacity = u32::try_from(byte_capacity).map_err(|_| {
            NafkaError::Config("behavior.dlt_queue_max_bytes 不能超过 u32::MAX".into())
        })?;
        Ok(Self {
            owner: Arc::downgrade(owner),
            lane,
            jobs: Arc::new(Semaphore::new(job_capacity)),
            job_capacity,
            bytes: Arc::new(Semaphore::new(byte_capacity as usize)),
            byte_capacity,
            in_flight: Arc::new(Semaphore::new(max_in_flight)),
            workers: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            group_workers: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
        })
    }

    /// 业务作用：在任务交给执行器前登记跨失权存活的实际责任。
    /// 参数说明：group 为原始消费组。返回：随任务退出释放的凭证，后续 owner 表移除不影响计数。
    pub(crate) fn worker(&self, group: &str) -> DltWorkerGuard {
        self.workers
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let mut groups = self
            .group_workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = groups.entry(group.to_owned()).or_default();
        *count += 1;
        if let Some(owner) = self.owner.upgrade() {
            owner.gauge("dlt_in_flight", *count as i64, &[("group", group)]);
        }
        DltWorkerGuard {
            dispatcher: self.clone(),
            group: group.to_owned(),
        }
    }
    /// 业务作用：将旧分配仍存活的死信任务纳入全源关闭诊断。
    /// 参数说明：无。
    /// 返回：实际任务数、信封记录数和字节数，观测不会归还额度。
    pub(crate) fn close_usage(&self) -> (usize, usize, usize) {
        let (records, bytes) = self.queue_usage();
        (
            self.workers.load(std::sync::atomic::Ordering::Acquire),
            records,
            bytes,
        )
    }

    /// 业务作用：等待当前源全部死信任务实际退出，覆盖旧分配遗留的原生发送。
    /// 参数说明：deadline 为消费与其它资源共用的单调截止时刻。
    /// 返回：任务与信封全部归还才成功；超时保留责任并报告当前占用。
    pub(crate) async fn await_idle(&self, deadline: std::time::Instant) -> Result<()> {
        loop {
            let active_tasks = self.workers.load(std::sync::atomic::Ordering::Acquire);
            let (retained_records, retained_bytes) = self.queue_usage();
            if active_tasks == 0 && retained_records == 0 {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(NafkaError::DltShutdownTimeout {
                    active_tasks,
                    retained_records,
                    retained_bytes,
                });
            }
            tokio::time::sleep(remaining.min(std::time::Duration::from_millis(10))).await;
        }
    }

    /// 业务作用：返回源 DLT pipeline 已准入的记录数与完整信封字节。
    /// 参数说明：无。
    /// 返回：覆盖等待交付、实际投递、完成与重试的占用，不把多记录批折成一条。
    pub(crate) fn queue_usage(&self) -> (usize, usize) {
        (
            self.job_capacity
                .saturating_sub(self.jobs.available_permits()),
            (self.byte_capacity as usize).saturating_sub(self.bytes.available_permits()),
        )
    }

    /// 业务作用：在复制原文之前同时预留源级 DLT 记录数和保守字节上界。
    /// 参数说明：records 为记录数，retained 为完整信封的保守计量。
    /// 返回：有额度时返回实际责任，暂时不足时为 None；空池也无法接纳时明确返回配置错误。
    pub(crate) fn try_reserve(
        &self,
        records: usize,
        retained: usize,
    ) -> Result<Option<DltReservation>> {
        // 永久超限必须显式保护，不能变成永远不会满足的容量等待。
        if records == 0 || records > self.job_capacity || retained > self.byte_capacity as usize {
            return Err(NafkaError::Config(
                "DLT 失败单元超过源级记录或字节边界".into(),
            ));
        }
        let count =
            u32::try_from(records).map_err(|_| NafkaError::Config("DLT 记录数不可表示".into()))?;
        let bytes =
            u32::try_from(retained).map_err(|_| NafkaError::Config("DLT 字节数不可表示".into()))?;
        let record_permit = match Arc::clone(&self.jobs).try_acquire_many_owned(count) {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => return Ok(None),
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Err(NafkaError::Lifecycle("DLT 记录准入已关闭".into()));
            }
        };
        let byte_permit = match Arc::clone(&self.bytes).try_acquire_many_owned(bytes) {
            Ok(permit) => permit,
            // 字节不足会在返回时释放尚未发布的记录凭证，不能留下半份责任。
            Err(tokio::sync::TryAcquireError::NoPermits) => return Ok(None),
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Err(NafkaError::Lifecycle("DLT 字节准入已关闭".into()));
            }
        };
        Ok(Some(DltReservation {
            records,
            bytes: retained,
            record_permit,
            byte_permit,
        }))
    }

    /// 业务作用：等待意图只在有希望取得额度时重新取入原坐标，避免反复复制或空转。
    /// 参数说明：retained 为先前已经验证可由空池容纳的单记录字节上界。
    /// 返回：当前有余量的提示，重新取入时仍须原子预留。
    pub(crate) fn can_reserve(&self, retained: usize) -> bool {
        self.jobs.available_permits() > 0 && self.bytes.available_permits() >= retained
    }

    /// 业务作用：在源级并行上限内尝试交付信封，停止后不再发送后继记录。
    /// 参数说明：envelope 为持有记录与字节额度、可跨重试保留的信封；stop_requested 汇合源停止与任务失权。
    /// 返回：全部记录确认才成功；停止、额度关闭或交付失败返回错误，已发送请求仍等待实际终态。
    pub(crate) async fn deliver(
        &self,
        envelope: &DltEnvelope,
        stop_requested: &impl Fn() -> bool,
    ) -> Result<()> {
        let permit = Arc::clone(&self.in_flight).acquire_owned();
        tokio::pin!(permit);
        let _in_flight = loop {
            // 等待额度不承担原生交付责任，可以响应停止；已发出的 send 不走此取消路径。
            if stop_requested() {
                return Err(NafkaError::Lifecycle("DLT 停止准入，原输入等待重投".into()));
            }
            tokio::select! {
                acquired = &mut permit => break acquired.map_err(|_| NafkaError::Lifecycle("DLT in-flight permit 已关闭".into()))?,
                _ = tokio::time::sleep(std::time::Duration::from_millis(25)) => {}
            }
        };
        for record in &envelope.job.records {
            // 批中前缀可能已投递；不提交来源位点，恢复时允许通过 DLT 原始身份识别重复。
            if stop_requested() {
                return Err(NafkaError::Lifecycle("DLT 停止准入，原输入等待重投".into()));
            }
            let owner = self
                .owner
                .upgrade()
                .ok_or_else(|| NafkaError::Lifecycle("DLT 所属源已退出".into()))?;
            let admin = crate::admin::shared_client(&owner)?;
            let origin = crate::DltOrigin {
                source_id: owner.config.client_name.clone(),
                cluster_id: admin.cluster_id().await?,
            };
            // 压缩会移除同key的旧失败和tombstone；每次发送前读取实际策略，不缓存配置权威。
            let target = admin.describe_topic(record.topic.clone(), true).await?;
            validate_retention(&target.configs)?;
            let headers = origin
                .attach(&record.headers)
                .map_err(|_| NafkaError::Config("DLT 来源身份无效".into()))?;
            // 超额证据保留原输入责任，不以静默裁剪业务头换取成功留证。
            crate::DltEvidence::read(&headers)
                .map_err(|_| NafkaError::Config("DLT 完整证据无效或超出容量".into()))?;
            if stop_requested() {
                return Err(NafkaError::Lifecycle("DLT 停止准入，原输入等待重投".into()));
            }
            self.lane
                .send_internal_raw(
                    &record.topic,
                    record.payload.as_deref(),
                    record.key.as_deref(),
                    &headers,
                    record.timestamp,
                )
                .await?;
        }
        Ok(())
    }
}

/// 业务作用：拒绝会因压缩或立即删除丢失失败历史的DLT目标。
/// 参数说明：configs为broker返回的有效topic配置。返回：仅delete且时间、容量保留值为正或-1时通过；缺失、隐藏和非法配置拒绝，不修改broker。
pub(crate) fn validate_retention(
    configs: &std::collections::BTreeMap<String, Option<String>>,
) -> Result<()> {
    if configs
        .get("cleanup.policy")
        .and_then(Option::as_deref)
        .map(str::trim)
        != Some("delete")
    {
        return Err(NafkaError::Config(
            "DLT 目标 cleanup.policy 必须为 delete".into(),
        ));
    }
    for key in ["retention.ms", "retention.bytes"] {
        let value = configs
            .get(key)
            .and_then(Option::as_deref)
            .and_then(|v| v.parse::<i64>().ok());
        if !matches!(value, Some(-1) | Some(1..)) {
            return Err(NafkaError::Config(
                "DLT 目标 retention 必须为正或 -1".into(),
            ));
        }
    }
    Ok(())
}
