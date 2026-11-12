//! 固定已提交边界内的只读分页，不参与消费组，也不把读取位置当成持久 checkpoint。

use crate::{
    KafkaConfig, KafkaHeader, KafkaHeaders, ReplayError as Error, ReplayRecord,
    ReplayResult as Result, Tp,
};
use rdkafka::{consumer::Consumer, Message, Offset, TopicPartitionList};
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

/// 单分区只读分页合同；跨页调用方必须保留首轮冻结的 end_offset。
#[derive(Clone, Debug)]
pub struct CommittedRead {
    /// 已批准的实际物理集群身份。
    pub cluster_id: String,
    /// 明确的 topic 和分区。
    pub partition: Tp,
    /// 完整 topic 分区数，运行期变化拒绝继续读取。
    pub partition_count: u32,
    /// 精确起点；None 显式选择本轮仍保留的最早位置，不表示历史完整。
    pub start_offset: Option<i64>,
    /// 冻结的结束位置，不包含此 offset；None 使用本轮 read_committed LSO。
    pub end_offset: Option<i64>,
    /// 一页最多记录数，1 至 4096。
    pub max_records: u32,
    /// 一页原文、头、key 和固定开销总上限，1 至 64 MiB。
    pub max_bytes: u64,
    /// 排队、连接、查询及读取共用预算，最多五分钟。
    pub timeout: Duration,
}

/// 有界原文页；没有记录也必须依据实际消费位置确认是否到达冻结边界。
#[derive(Clone, Debug)]
pub struct CommittedPage {
    /// 当前观测的保留起点，不能证明更早历史从未存在。
    pub beginning_offset: i64,
    /// 本轮固定的已提交边界。
    pub end_offset: i64,
    /// 继续读取应使用的位置；等于 end_offset 才表示本范围完成。
    pub next_offset: i64,
    /// 仅包含已经提交的原始记录，保留 null 与空值差异。
    pub records: Vec<ReplayRecord>,
}

impl CommittedRead {
    /// 业务作用：在取得网络和内存责任前验证完整分页合同。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：身份、范围、容量和期限合法时通过；负位点、相反范围和无界声明拒绝。
    pub fn validate(&self) -> Result<()> {
        if self.cluster_id.is_empty()
            || self.cluster_id.len() > 1024
            || !(1..=4096).contains(&self.partition_count)
            || self.partition.partition < 0
            || self.partition.partition >= self.partition_count as i32
            || self.start_offset.is_some_and(|offset| offset < 0)
            || self.end_offset.is_some_and(|offset| offset < 0)
            || matches!((self.start_offset, self.end_offset), (Some(start),Some(end)) if start > end)
            || !(1..=4096).contains(&self.max_records)
            || !(1..=64 * 1024 * 1024).contains(&self.max_bytes)
            || self.timeout.is_zero()
            || self.timeout > Duration::from_secs(300)
        {
            return Err(Error::InvalidConfiguration);
        }
        crate::ReplayPosition {
            topic: self.partition.topic.clone(),
            partition: self.partition.partition,
            start_offset: 0,
            end_offset: 0,
            next_offset: 0,
        }
        .validate()
        .map_err(|_| Error::InvalidConfiguration)
    }
}

/// 业务作用：在独立阻塞任务内读取固定 LSO 范围，并将拷贝控制在页面总预算内。
///
/// 参数说明：
/// - `config`：受管来源冻结的连接和安全配置。
/// - `plan`：已经验证的单分区分页合同。
/// - `deadline`：从准入时刻起算的唯一截止时间。
///
/// 返回：完整页面；缺口、边界回退、拓扑变化或超时均不返回假完成位置。
pub(crate) fn read(
    mut config: KafkaConfig,
    plan: CommittedRead,
    deadline: Instant,
) -> Result<CommittedPage> {
    config
        .consumer
        .properties
        .insert("queued.min.messages".into(), "1".into());
    config.consumer.properties.insert(
        "queued.max.messages.kbytes".into(),
        (plan.max_bytes / 1024).max(1).to_string(),
    );
    config
        .consumer
        .properties
        .insert("enable.partition.eof".into(), "true".into());
    let consumer = crate::replay::inspection_consumer(&config, deadline)?;
    let topics = BTreeSet::from([plan.partition.topic.clone()]);
    let before = crate::replay::inspect_consumer(&consumer, &topics, None, deadline)?;
    if before.cluster_id != plan.cluster_id {
        return Err(Error::IdentityChanged);
    }
    if before.partitions.len() != plan.partition_count as usize {
        return Err(Error::PartitionsChanged);
    }
    let observed = before
        .partitions
        .iter()
        .find(|entry| entry.tp == plan.partition)
        .ok_or(Error::PartitionsChanged)?;
    let start = plan.start_offset.unwrap_or(observed.beginning_offset);
    let end = plan.end_offset.unwrap_or(observed.stable_end_offset);
    if start < observed.beginning_offset {
        return Err(Error::DataGap);
    }
    if start > end || end > observed.stable_end_offset {
        return Err(Error::EndRegressed);
    }
    let mut page = CommittedPage {
        beginning_offset: observed.beginning_offset,
        end_offset: end,
        next_offset: start,
        records: Vec::new(),
    };
    if start == end {
        return Ok(page);
    }
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(
            &plan.partition.topic,
            plan.partition.partition,
            Offset::Offset(start),
        )
        .map_err(|_| Error::InvalidConfiguration)?;
    consumer
        .assign(&assignment)
        .map_err(|_| Error::ObservationFailed)?;
    let mut bytes = 0u64;
    loop {
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        match consumer
            .poll(Duration::from_millis(20).min(deadline.saturating_duration_since(Instant::now())))
        {
            Some(Ok(message)) => {
                if message.topic() != plan.partition.topic
                    || message.partition() != plan.partition.partition
                    || message.offset() < page.next_offset
                {
                    return Err(Error::ObservationFailed);
                }
                if message.offset() >= end {
                    page.next_offset = end;
                    break;
                }
                let mut owned_headers = Vec::new();
                let mut size = 64
                    + message.key().map_or(0, |v| v.len() as u64)
                    + message.payload().map_or(0, |v| v.len() as u64);
                if let Some(headers) = message.headers() {
                    use rdkafka::message::Headers;
                    if headers.count() > 256 {
                        return Err(Error::Capacity);
                    }
                    for header in headers.iter() {
                        size += header.key.len() as u64
                            + header.value.map_or(0, |value| value.len() as u64);
                    }
                }
                if size > plan.max_bytes {
                    return Err(Error::Capacity);
                }
                if size > plan.max_bytes - bytes {
                    page.next_offset = message.offset();
                    break;
                }
                if let Some(headers) = message.headers() {
                    use rdkafka::message::Headers;
                    for header in headers.iter() {
                        owned_headers.push(KafkaHeader {
                            name: header.key.to_owned(),
                            value: header.value.map(ToOwned::to_owned),
                        });
                    }
                }
                bytes += size;
                page.next_offset = message.offset() + 1;
                page.records.push(ReplayRecord {
                    cluster_id: plan.cluster_id.clone(),
                    topic: plan.partition.topic.clone(),
                    partition: plan.partition.partition,
                    offset: message.offset(),
                    timestamp: message.timestamp().to_millis().unwrap_or(-1),
                    key: message.key().map(ToOwned::to_owned),
                    payload: message.payload().map(ToOwned::to_owned),
                    headers: KafkaHeaders::from_vec(owned_headers),
                });
                if page.records.len() >= plan.max_records as usize || page.next_offset == end {
                    break;
                }
            }
            Some(Err(rdkafka::error::KafkaError::PartitionEOF(_))) | None => {
                // 事务标记、已中止记录和压缩缺口没有业务记录；只有原生消费位置能证明跨过它们。
                let positions = consumer.position().map_err(|_| Error::ObservationFailed)?;
                if let Some(position) =
                    positions.find_partition(&plan.partition.topic, plan.partition.partition)
                {
                    if let Offset::Offset(next) = position.offset() {
                        page.next_offset = page.next_offset.max(next.min(end));
                    }
                }
                if page.next_offset == end {
                    break;
                }
            }
            Some(Err(error)) => {
                return Err(
                    if error
                        .rdkafka_error_code()
                        .is_some_and(crate::rd::errors::authorization)
                    {
                        Error::Authorization
                    } else {
                        Error::ObservationFailed
                    },
                );
            }
        }
    }
    let after = crate::replay::inspect_consumer(&consumer, &topics, None, deadline)?;
    if after.cluster_id != before.cluster_id {
        return Err(Error::IdentityChanged);
    }
    if after.partitions.len() != before.partitions.len() {
        return Err(Error::PartitionsChanged);
    }
    let observed = after
        .partitions
        .iter()
        .find(|entry| entry.tp == plan.partition)
        .ok_or(Error::PartitionsChanged)?;
    if observed.beginning_offset > start {
        return Err(Error::DataGap);
    }
    if observed.stable_end_offset < end {
        return Err(Error::EndRegressed);
    }
    Ok(page)
}
