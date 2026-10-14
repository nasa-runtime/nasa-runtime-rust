//! 不加入消费组的回放范围探测，所有原生调用共用一个总预算。

use super::{ReplayError, ReplayObservation, ReplayPartition, ReplayResult};
use crate::{KafkaConfig, ReplayCheckpoint, ReplayPosition, Tp};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

/// 业务作用：为只读回放规划查询完整保留起点和 LSO，不参与 group 协调或 offset 提交。
/// 参数说明：owner 为提供冻结配置与准入门禁的源，topics 为白名单，timestamp 为可选时间，budget 为全部请求共用预算。
/// 返回：前后物理身份和分区集合一致的完整证据；取消等待不把未完成底层查询当作完成。
pub(crate) async fn observe(
    owner: &crate::KafkaProxyInner,
    topics: BTreeSet<String>,
    timestamp: Option<i64>,
    budget: Duration,
) -> ReplayResult<ReplayObservation> {
    if topics.is_empty()
        || topics.len() > ReplayCheckpoint::MAX_PARTITIONS
        || timestamp.is_some_and(|value| value < 0)
        || budget.is_zero()
        || budget > Duration::from_secs(300)
    {
        return Err(ReplayError::InvalidConfiguration);
    }
    for topic in &topics {
        ReplayPosition {
            topic: topic.clone(),
            partition: 0,
            start_offset: 0,
            end_offset: 0,
            next_offset: 0,
        }
        .validate()
        .map_err(|_| ReplayError::InvalidConfiguration)?;
    }
    let deadline = Instant::now() + budget;
    let config = owner.config.clone();
    // 参数通过后才取得管理责任；与最终关闭共享门禁，禁止检查与创建之间跨过关闭边界。
    let owner = crate::admin::shared_client(owner).map_err(|error| match error {
        crate::NafkaError::Lifecycle(_) => ReplayError::Closed,
        _ => ReplayError::ObservationFailed,
    })?;
    let task = tokio::task::spawn_blocking(move || {
        // 等待 future 被取消不能解除已准入的原生查询责任；短连 consumer 真正销毁后才归还源级管理持有。
        let result = inspect(config, topics, timestamp, deadline);
        drop(owner);
        result
    });
    // 排队本身也计入调用预算；本地超时不撤销阻塞任务持有的管理责任。
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), task)
        .await
        .map_err(|_| ReplayError::Timeout)?
        .map_err(|_| ReplayError::ObservationFailed)?
}

/// 业务作用：在隔离的同步线程内完成原生查询，确保时间预算不随分区数量倍增。
/// 参数说明：config 为源配置，topics 为完整白名单，timestamp 为时间查询参数，deadline 为唯一截止值。
/// 返回：所有证据齐全时发布一次不可变快照；部分查询不能构成计划。
pub(super) fn inspect(
    config: KafkaConfig,
    topics: BTreeSet<String>,
    timestamp: Option<i64>,
    deadline: Instant,
) -> ReplayResult<ReplayObservation> {
    let consumer = inspection_consumer(&config, deadline)?;
    inspect_consumer(&consumer, &topics, timestamp, deadline)
}

/// 业务作用：创建不参与组协调的只读探测客户端，允许持有者在明确任务寿命内复用连接。
/// 参数说明：config 为冻结来源配置，deadline 为包含构造开销的总期限。
/// 返回：禁止自动提交和自动建主题的 read_committed 客户端；创建失败不开放读取。
pub(crate) fn inspection_consumer(
    config: &KafkaConfig,
    deadline: Instant,
) -> ReplayResult<BaseConsumer> {
    // 排队期间可能耗尽总预算；此时不再创建新的原生连接资源。
    remaining(deadline)?;
    let mut native = crate::rd::config::consumer_config(config, "nasa-replay-inspection")
        .map_err(|_| ReplayError::InvalidConfiguration)?;
    native
        .set("isolation.level", "read_committed")
        .set("allow.auto.create.topics", "false")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false");
    remaining(deadline)?;
    let consumer: BaseConsumer = native
        .create()
        .map_err(|_| ReplayError::InvalidConfiguration)?;
    Ok(consumer)
}

/// 业务作用：在回放 worker 已持有的已提交隔离客户端上复验来源，避免每条记录重建连接。
/// 参数说明：consumer 为当前线程专属 read_committed 客户端，topics 为完整白名单，timestamp 为可选查询，deadline 为总截止时间。
/// 返回：完整物理身份和范围证据，不订阅、seek 或提交该 consumer 的位置。
pub(crate) fn inspect_consumer(
    consumer: &BaseConsumer,
    topics: &BTreeSet<String>,
    timestamp: Option<i64>,
    deadline: Instant,
) -> ReplayResult<ReplayObservation> {
    let cluster = cluster_id(consumer, deadline)?;
    let before = metadata(consumer, topics, deadline)?;
    let mut partitions = Vec::with_capacity(before.len());
    for tp in &before {
        // 这里必须使用实时查询及 read_committed 客户端：原生 ListOffsets 携带隔离级别，缓存高水位不具有 LSO 语义。
        let (beginning_offset, stable_end_offset) = consumer
            .fetch_watermarks(&tp.topic, tp.partition, remaining(deadline)?)
            .map_err(|error| native_failure(error, deadline))?;
        partitions.push(ReplayPartition {
            tp: tp.clone(),
            beginning_offset,
            stable_end_offset,
            timestamp_offset: None,
        });
    }
    if let Some(time) = timestamp {
        let mut request = TopicPartitionList::with_capacity(before.len());
        for tp in &before {
            request
                .add_partition_offset(&tp.topic, tp.partition, Offset::Offset(time))
                .map_err(|_| ReplayError::InvalidConfiguration)?;
        }
        let response = consumer
            .offsets_for_times(request, remaining(deadline)?)
            .map_err(|error| native_failure(error, deadline))?;
        let mut values = BTreeMap::new();
        for item in response.elements() {
            item.error()
                .map_err(|error| native_failure(error, deadline))?;
            let offset = match item.offset() {
                Offset::Offset(value) if value >= 0 => Some(value),
                Offset::End => None,
                _ => return Err(ReplayError::ObservationFailed),
            };
            values.insert(
                Tp {
                    topic: item.topic().to_owned(),
                    partition: item.partition(),
                },
                offset,
            );
        }
        if values.len() != before.len() {
            return Err(ReplayError::ObservationFailed);
        }
        for partition in &mut partitions {
            partition.timestamp_offset = *values
                .get(&partition.tp)
                .ok_or(ReplayError::ObservationFailed)?;
        }
    }
    // safe metadata 不暴露 topic UUID；跨查询的删除重建还必须由调用方重新查询外部 generation 证明，名称不承担代次作用。
    if before != metadata(consumer, topics, deadline)? {
        return Err(ReplayError::PartitionsChanged);
    }
    if cluster != cluster_id(consumer, deadline)? {
        return Err(ReplayError::IdentityChanged);
    }
    ReplayObservation::new(cluster, timestamp, partitions)
        .map_err(|_| ReplayError::ObservationFailed)
}

/// 业务作用：读取 broker 真实物理身份，缺失证据时禁止以 bootstrap 地址替代。
/// 参数说明：consumer 为专用只读客户端，deadline 为总截止时刻。
/// 返回：非空 cluster ID 或明确不可观测/超时。
fn cluster_id(consumer: &BaseConsumer, deadline: Instant) -> ReplayResult<String> {
    consumer
        .client()
        .fetch_cluster_id(remaining(deadline)?)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| failure(deadline))
}

/// 业务作用：只接受所选 topic 的完整分区集合，限制元数据进入后续范围查询的数量。
/// 参数说明：consumer 为专用客户端，topics 为白名单，deadline 为总截止时刻。
/// 返回：严格排序的完整分区；topic 缺失、任一分区错误或数量超额拒绝。
fn metadata(
    consumer: &BaseConsumer,
    topics: &BTreeSet<String>,
    deadline: Instant,
) -> ReplayResult<BTreeSet<Tp>> {
    let mut result = BTreeSet::new();
    for topic in topics {
        let response = consumer
            .fetch_metadata(Some(topic), remaining(deadline)?)
            .map_err(|error| native_failure(error, deadline))?;
        let item = response
            .topics()
            .iter()
            .find(|item| item.name() == topic)
            .ok_or(ReplayError::ObservationFailed)?;
        if item
            .error()
            .is_some_and(|error| crate::rd::errors::authorization(error.into()))
        {
            return Err(ReplayError::Authorization);
        }
        if item.error().is_some_and(|error| {
            rdkafka::error::RDKafkaErrorCode::from(error)
                == rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition
        }) {
            return Err(ReplayError::TopicMissing);
        }
        if item.error().is_some() || item.partitions().is_empty() {
            return Err(ReplayError::ObservationFailed);
        }
        for partition in item.partitions() {
            if partition
                .error()
                .is_some_and(|error| crate::rd::errors::authorization(error.into()))
            {
                return Err(ReplayError::Authorization);
            }
            if partition.error().is_some() {
                return Err(ReplayError::ObservationFailed);
            }
            if result.len() >= ReplayCheckpoint::MAX_PARTITIONS {
                return Err(ReplayError::InvalidConfiguration);
            }
            if !result.insert(Tp {
                topic: topic.clone(),
                partition: partition.id(),
            }) {
                return Err(ReplayError::ObservationFailed);
            }
        }
    }
    Ok(result)
}

/// 业务作用：后续请求只使用剩余总预算，避免逐分区重新计时。
/// 参数说明：deadline 为唯一截止时刻。
/// 返回：正剩余时间；耗尽后不再发起原生请求。
fn remaining(deadline: Instant) -> ReplayResult<Timeout> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(ReplayError::Timeout)
    } else {
        Ok(Timeout::After(remaining))
    }
}

/// 业务作用：让明确的 broker 授权拒绝优先于经过时间，避免误导调用方重复不可授权的读取。
/// 参数说明：error 为原生请求失败，deadline 为当前请求总截止时刻。返回：结构化授权拒绝或预算对应的不可观测类别。
fn native_failure(error: rdkafka::error::KafkaError, deadline: Instant) -> ReplayError {
    if error
        .rdkafka_error_code()
        .is_some_and(crate::rd::errors::authorization)
    {
        ReplayError::Authorization
    } else {
        failure(deadline)
    }
}

/// 业务作用：对缺少原生错误证据的失败输出不包含地址或凭据的共同类别。
/// 参数说明：deadline 为当前请求总截止时间。
/// 返回：已耗尽为 Timeout，其余为不可观测。
fn failure(deadline: Instant) -> ReplayError {
    if Instant::now() >= deadline {
        ReplayError::Timeout
    } else {
        ReplayError::ObservationFailed
    }
}
