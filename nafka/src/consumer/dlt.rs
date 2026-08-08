//! 有界 DLT dispatcher；统一约束保留 job 数、payload 字节和并行投递数。

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
                    })?;
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
    /// 启动期固定的 DLT producer lane。
    lane: ProducerLane,
    /// waiting + in-flight job 总数 permit。
    jobs: Arc<Semaphore>,
    /// job semaphore 的冻结总容量。
    job_capacity: usize,
    /// waiting + in-flight retained bytes permit。
    bytes: Arc<Semaphore>,
    /// 单 job 请求前的显式字节上限，避免 acquire_many 永久等待。
    byte_capacity: u32,
    /// 实际 broker delivery 并行 permit。
    in_flight: Arc<Semaphore>,
}

/// 已取得全 pipeline job/byte 配额的死信信封。
pub(crate) struct DltEnvelope {
    /// 投递期间及外层重试之间始终持有的原始任务。
    job: DltJob,
    /// 从准入到最终收尾始终持有的任务数 permit。
    _job_permit: OwnedSemaphorePermit,
    /// 从准入到最终收尾始终持有的字节 permit。
    _byte_permit: OwnedSemaphorePermit,
}

/// owner 对 DLT dispatcher 的无等待准入结果。
pub(crate) enum DltAdmission {
    /// job/byte 双 permit 已取得，可以把信封交给异步 worker。
    Admitted(DltEnvelope),
    /// 当前容量不足；返回原 job，由 owner 作为唯一 local pending 保留。
    Backpressured(DltJob),
}

impl DltDispatcher {
    /// 业务作用：用冻结配置与 lane 构造 dispatcher。
    ///
    /// # 参数
    ///
    /// - `lane`: 预先解析的固定 DLT lane。
    /// - `job_capacity`: 全 pipeline 最大任务数。
    /// - `byte_capacity`: 全 pipeline 最大保留字节。
    /// - `max_in_flight`: 最大并行 delivery 数。
    ///
    /// # 错误
    ///
    /// 字节容量超过 semaphore 表达范围时返回配置错误。
    pub(crate) fn new(
        lane: ProducerLane,
        job_capacity: usize,
        byte_capacity: usize,
        max_in_flight: usize,
    ) -> Result<Self> {
        let byte_capacity = u32::try_from(byte_capacity).map_err(|_| {
            NafkaError::Config("behavior.dlt_queue_max_bytes 不能超过 u32::MAX".into())
        })?;
        Ok(Self {
            lane,
            jobs: Arc::new(Semaphore::new(job_capacity)),
            job_capacity,
            bytes: Arc::new(Semaphore::new(byte_capacity as usize)),
            byte_capacity,
            in_flight: Arc::new(Semaphore::new(max_in_flight)),
        })
    }

    /// 业务作用：返回全局 DLT pipeline 当前占用的 job 与 retained bytes。
    pub(crate) fn queue_usage(&self) -> (usize, usize) {
        (
            self.job_capacity
                .saturating_sub(self.jobs.available_permits()),
            (self.byte_capacity as usize).saturating_sub(self.bytes.available_permits()),
        )
    }

    /// 业务作用：无等待地尝试取得 job/byte 双 permit。
    ///
    /// # 参数
    ///
    /// - `job`: 待纳入全局容量域的拥有型任务。
    ///
    /// # 错误
    ///
    /// dispatcher 已关闭或任务超过字节容量时返回错误；暂时无 permit 返回带原任务的反压结果。
    pub(crate) fn try_admit(&self, job: DltJob) -> Result<DltAdmission> {
        if job.retained_bytes > self.byte_capacity {
            return Err(NafkaError::Config(format!(
                "DLT job bytes={} 超过 dispatcher byte_capacity={}",
                job.retained_bytes, self.byte_capacity
            )));
        }
        let job_permit = match Arc::clone(&self.jobs).try_acquire_owned() {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                return Ok(DltAdmission::Backpressured(job))
            }
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Err(NafkaError::Lifecycle("DLT job permit 已关闭".into()))
            }
        };
        let byte_permit = match Arc::clone(&self.bytes).try_acquire_many_owned(job.retained_bytes) {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                drop(job_permit);
                return Ok(DltAdmission::Backpressured(job));
            }
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Err(NafkaError::Lifecycle("DLT byte permit 已关闭".into()))
            }
        };
        Ok(DltAdmission::Admitted(DltEnvelope {
            job,
            _job_permit: job_permit,
            _byte_permit: byte_permit,
        }))
    }

    /// 业务作用：在并行上限内执行信封的一次 broker delivery。
    ///
    /// # 参数
    ///
    /// - `envelope`: 已取得 job/byte permit 且可跨失败重试复用的信封。
    ///
    /// # 错误
    ///
    /// in-flight permit 关闭、同步入队或 broker delivery 失败时返回错误；信封本身不被消费。
    pub(crate) async fn deliver(&self, envelope: &DltEnvelope) -> Result<()> {
        let _in_flight = Arc::clone(&self.in_flight)
            .acquire_owned()
            .await
            .map_err(|_| NafkaError::Lifecycle("DLT in-flight permit 已关闭".into()))?;
        for record in &envelope.job.records {
            self.lane
                .send_internal_raw(
                    &record.topic,
                    record.payload.as_deref(),
                    record.key.as_deref(),
                    &record.headers,
                    record.timestamp,
                )
                .await?;
        }
        Ok(())
    }
}
