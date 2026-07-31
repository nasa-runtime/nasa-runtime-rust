//! 管理端、metadata 与离线 offset 底层适配。

use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::admin::{
    AdminClient, AdminOptions, NewPartitions, NewTopic, ResourceSpecifier, TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;

use crate::admin::{TopicDescription, TopicSpec};
use crate::config::KafkaConfig;
use crate::error::{NafkaError, Result};
use crate::types::Tp;

use super::config::{admin_config, consumer_config};

/// 一个 topic 的元数据条目；`error` 非空表示本轮观测不可信，不代表 topic 不存在。
pub(crate) struct TopicMetadata {
    /// topic 名。
    pub(crate) name: String,
    /// 已排序分区号。
    pub(crate) partitions: Vec<i32>,
    /// broker 为该 topic 返回的错误分类；`None` 表示观测有效。
    pub(crate) error: Option<String>,
}

/// 可复用的底层管理客户端和冻结超时。
pub(crate) struct AdminHandle {
    /// 管理客户端。
    client: AdminClient<DefaultClientContext>,
    /// 请求总超时。
    timeout: Duration,
    /// 单次 broker 请求超时；必须 <= `timeout`(API 总超时)。
    request_timeout: Duration,
    /// 构造短命查询 consumer 所需的顶层配置。
    config: KafkaConfig,
}

impl AdminHandle {
    /// 构造管理客户端。
    ///
    /// # 参数
    ///
    /// - `config`: 冻结后的顶层配置。
    ///
    /// # 错误
    ///
    /// 原生配置或客户端构造失败时返回错误。
    pub(crate) fn create(config: &KafkaConfig) -> Result<Arc<Self>> {
        let client = admin_config(config)?
            .create::<AdminClient<DefaultClientContext>>()
            .map_err(|error| NafkaError::Config(format!("admin 客户端构造失败: {error}")))?;
        //：request_timeout_ms 是单请求超时，default_api_timeout_ms 是 API 总超时；
        // 两者不能互为回退，否则同时配置时单请求会错用总超时值。
        let api_millis = config
            .admin
            .default_api_timeout_ms
            .unwrap_or(config.behavior.publish_timeout_ms);
        let request_millis = config
            .admin
            .request_timeout_ms
            .unwrap_or(api_millis)
            .min(api_millis);
        Ok(Arc::new(Self {
            client,
            timeout: Duration::from_millis(api_millis),
            request_timeout: Duration::from_millis(request_millis),
            config: config.clone(),
        }))
    }

    /// 查询全部可见 topic 及其分区。
    ///
    /// # 错误
    ///
    /// metadata 请求失败时返回 broker 错误。
    pub(crate) async fn topic_metadata(self: &Arc<Self>) -> Result<Vec<TopicMetadata>> {
        let owner = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let metadata = owner
                .client
                .inner()
                .fetch_metadata(None, Timeout::After(owner.timeout))
                .map_err(broker_error)?;
            let mut topics = Vec::with_capacity(metadata.topics().len());
            for topic in metadata.topics() {
                // 携带错误的条目**不能**被静默丢弃：调用方无法把
                // "broker 明确说没有这个 topic" 与 "controller 切换/分区迁移期间这一轮读不到"
                // 区分开，会把一次滚动重启误判成确定的契约漂移并永久停机。
                // 这里把错误原样带出去，由调用方按语义裁决。
                let mut partitions: Vec<i32> =
                    topic.partitions().iter().map(|item| item.id()).collect();
                partitions.sort_unstable();
                topics.push(TopicMetadata {
                    name: topic.name().to_owned(),
                    partitions,
                    error: topic.error().map(|error| format!("{error:?}")),
                });
            }
            topics.sort_by(|left, right| left.name.cmp(&right.name));
            Ok(topics)
        })
        .await
        .map_err(join_error)?
    }

    /// 查询单个 topic 的分区、副本和可选配置。
    ///
    /// # 参数
    ///
    /// - `topic`: 已校验的 topic 名。
    /// - `include_configs`: 是否调用 DescribeConfigs。
    ///
    /// # 错误
    ///
    /// metadata、DescribeConfigs 或阻塞任务失败时返回错误。
    pub(crate) async fn describe_topic(
        self: &Arc<Self>,
        topic: String,
        include_configs: bool,
    ) -> Result<TopicDescription> {
        let owner = Arc::clone(self);
        let metadata_topic = topic.clone();
        let (partitions, replication_factor) = tokio::task::spawn_blocking(move || {
            let metadata = owner
                .client
                .inner()
                .fetch_metadata(Some(&metadata_topic), Timeout::After(owner.timeout))
                .map_err(broker_error)?;
            let item = metadata
                .topics()
                .iter()
                .find(|item| item.name() == metadata_topic)
                .ok_or_else(|| NafkaError::Broker(format!("topic 不存在: {metadata_topic}")))?;
            if let Some(error) = item.error() {
                return Err(NafkaError::Broker(format!(
                    "topic `{metadata_topic}` metadata 失败: {error:?}"
                )));
            }
            let mut partitions = Vec::with_capacity(item.partitions().len());
            let mut factors = std::collections::BTreeSet::new();
            for partition in item.partitions() {
                if let Some(error) = partition.error() {
                    return Err(NafkaError::Broker(format!(
                        "topic `{metadata_topic}` partition {} metadata 失败: {error:?}",
                        partition.id()
                    )));
                }
                partitions.push(partition.id());
                factors.insert(
                    i32::try_from(partition.replicas().len())
                        .map_err(|_| NafkaError::Broker("topic 副本数超出 i32".into()))?,
                );
            }
            partitions.sort_unstable();
            let replication_factor = (factors.len() == 1)
                .then(|| factors.into_iter().next())
                .flatten();
            Ok((partitions, replication_factor))
        })
        .await
        .map_err(join_error)??;

        let configs = if include_configs {
            let resource = ResourceSpecifier::Topic(&topic);
            let mut results = self
                .client
                .describe_configs(
                    [&resource],
                    &AdminOptions::new().request_timeout(Some(self.request_timeout)),
                )
                .await
                .map_err(broker_error)?;
            let described = results
                .pop()
                .ok_or_else(|| NafkaError::Broker("DescribeConfigs 未返回结果".into()))?
                .map_err(|error| {
                    NafkaError::Broker(format!("topic `{topic}` DescribeConfigs 失败: {error}"))
                })?;
            if described.entries.is_empty() {
                return Err(NafkaError::Broker(format!(
                    "topic `{topic}` DescribeConfigs 返回空配置集，可能缺少 DescribeConfigs 权限"
                )));
            }
            described
                .entries
                .into_iter()
                .map(|entry| (entry.name, entry.value))
                .collect()
        } else {
            std::collections::BTreeMap::new()
        };
        Ok(TopicDescription {
            name: topic,
            partitions,
            replication_factor,
            configs,
        })
    }

    /// 创建一个 topic。
    ///
    /// # 参数
    ///
    /// - `spec`: 已完成公共层校验的 topic 契约。
    ///
    /// # 错误
    ///
    /// 请求或逐项 broker 结果失败时返回错误。
    pub(crate) async fn create_topic(&self, spec: &TopicSpec) -> Result<()> {
        self.create_topic_inner(spec, false).await
    }

    /// 幂等创建一个 topic，已存在视为成功。
    ///
    /// # 参数
    ///
    /// - `spec`: 已完成公共层校验的 topic 契约。
    ///
    /// # 错误
    ///
    /// 除已存在外的请求或逐项 broker 结果失败时返回错误。
    pub(crate) async fn create_topic_if_absent(&self, spec: &TopicSpec) -> Result<()> {
        self.create_topic_inner(spec, true).await
    }

    /// 执行 topic 创建并按调用语义处理已存在错误。
    ///
    /// # 参数
    ///
    /// - `spec`: 已完成公共层校验的 topic 契约。
    /// - `allow_existing`: 是否把已存在视为成功。
    ///
    /// # 错误
    ///
    /// 请求或不允许忽略的逐项 broker 结果失败时返回错误。
    /// 修改一个已存在 topic 的动态配置项。
    ///
    /// # 参数
    ///
    /// - `topic`: 目标 topic。
    /// - `key`: 动态配置键（如 `max.message.bytes`）。
    /// - `value`: 新值。
    ///
    /// # 错误
    ///
    /// broker 拒绝或超时返回错误。
    pub(crate) async fn alter_topic_config(
        &self,
        topic: &str,
        key: &str,
        value: &str,
    ) -> Result<()> {
        let config =
            rdkafka::admin::AlterConfig::new(ResourceSpecifier::Topic(topic)).set(key, value);
        let results = self
            .client
            .alter_configs(
                [&config],
                &AdminOptions::new()
                    .request_timeout(Some(self.request_timeout))
                    .operation_timeout(Some(self.timeout)),
            )
            .await
            .map_err(broker_error)?;
        for result in results {
            result.map_err(|(resource, code)| {
                NafkaError::Broker(format!("修改 topic 配置失败 {resource:?}: {code}"))
            })?;
        }
        Ok(())
    }

    /// 向 broker 提交 topic 创建请求，并按调用语义决定是否容忍“已存在”。
    async fn create_topic_inner(&self, spec: &TopicSpec, allow_existing: bool) -> Result<()> {
        let mut topic = NewTopic::new(
            &spec.name,
            spec.partitions,
            TopicReplication::Fixed(spec.replication_factor),
        );
        for (key, value) in &spec.configs {
            topic = topic.set(key, value);
        }
        let results = self
            .client
            .create_topics(
                [&topic],
                &AdminOptions::new()
                    .request_timeout(Some(self.request_timeout))
                    .operation_timeout(Some(self.timeout)),
            )
            .await
            .map_err(broker_error)?;
        check_topic_result(results, allow_existing)
    }

    /// 删除一个 topic。
    ///
    /// # 参数
    ///
    /// - `topic`: 目标 topic。
    ///
    /// # 错误
    ///
    /// 请求或逐项 broker 结果失败时返回错误。
    pub(crate) async fn delete_topic(&self, topic: &str) -> Result<()> {
        let results = self
            .client
            .delete_topics(
                &[topic],
                &AdminOptions::new()
                    .request_timeout(Some(self.request_timeout))
                    .operation_timeout(Some(self.timeout)),
            )
            .await
            .map_err(broker_error)?;
        check_topic_result(results, false)
    }

    /// 把 topic 总分区数增加到指定值。
    ///
    /// # 参数
    ///
    /// - `topic`: 目标 topic。
    /// - `total`: 新的总分区数。
    ///
    /// # 错误
    ///
    /// 请求或逐项 broker 结果失败时返回错误。
    pub(crate) async fn increase_partitions(&self, topic: &str, total: usize) -> Result<()> {
        let partitions = NewPartitions::new(topic, total);
        let results = self
            .client
            .create_partitions(
                [&partitions],
                &AdminOptions::new()
                    .request_timeout(Some(self.request_timeout))
                    .operation_timeout(Some(self.timeout)),
            )
            .await
            .map_err(broker_error)?;
        check_topic_result(results, false)
    }

    /// 查询一组分区的低、高水位。
    ///
    /// # 参数
    ///
    /// - `tps`: 已校验的目标分区。
    ///
    /// # 错误
    ///
    /// 任一分区查询失败时返回错误。
    pub(crate) async fn watermarks(self: &Arc<Self>, tps: Vec<Tp>) -> Result<Vec<(Tp, i64, i64)>> {
        let owner = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let mut values = Vec::with_capacity(tps.len());
            for tp in tps {
                let (low, high) = owner
                    .client
                    .inner()
                    .fetch_watermarks(&tp.topic, tp.partition, Timeout::After(owner.timeout))
                    .map_err(broker_error)?;
                values.push((tp, low, high));
            }
            Ok(values)
        })
        .await
        .map_err(join_error)?
    }

    /// 使用不订阅 topic 的短命 consumer 查询 group committed offsets。
    ///
    /// # 参数
    ///
    /// - `group`: group.id。
    /// - `tps`: 目标分区。
    ///
    /// # 错误
    ///
    /// 客户端构造、列表构造或 broker 查询失败时返回错误。
    pub(crate) async fn committed_offsets(
        self: &Arc<Self>,
        group: String,
        tps: Vec<Tp>,
    ) -> Result<Vec<(Tp, Option<i64>)>> {
        let owner = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let consumer = consumer_config(&owner.config, &group)?
                .create::<BaseConsumer>()
                .map_err(|error| {
                    NafkaError::Broker(format!("offset 查询客户端构造失败: {error}"))
                })?;
            let deadline = Instant::now() + owner.timeout;
            let committed = loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(NafkaError::Broker(format!(
                        "查询 group `{group}` committed offsets 超时"
                    )));
                }
                let mut query = TopicPartitionList::with_capacity(tps.len());
                for tp in &tps {
                    query.add_partition(&tp.topic, tp.partition);
                }
                match consumer
                    .committed_offsets(query, Timeout::After(remaining.min(Duration::from_secs(5))))
                {
                    Ok(committed) => break committed,
                    Err(error) if is_coordinator_lookup_transient(&error) => {
                        let sleep_for = remaining.min(Duration::from_millis(100));
                        if sleep_for.is_zero() {
                            return Err(broker_error(error));
                        }
                        let _ = consumer.poll(Duration::from_millis(10));
                        std::thread::sleep(sleep_for);
                    }
                    Err(error) => return Err(broker_error(error)),
                }
            };
            let values = tps
                .into_iter()
                .map(|tp| {
                    let offset =
                        committed
                            .find_partition(&tp.topic, tp.partition)
                            .and_then(|element| match element.offset() {
                                Offset::Offset(value) if value >= 0 => Some(value),
                                _ => None,
                            });
                    (tp, offset)
                })
                .collect();
            Ok(values)
        })
        .await
        .map_err(join_error)?
    }
}

/// 判断 offset 查询中的 coordinator 发现错误是否适合在同一 admin deadline 内重试。
///
/// 短命 `BaseConsumer` 首次查询一个从未提交过的 group 时，broker 可能刚完成
/// FindCoordinator / offset metadata 初始化，Kafka 4.1 单机上实测会返回
/// `NotCoordinator`。这类错误不代表 topic 或权限失败，立即暴露会让无提交历史查询
/// 偶发失败；有界重试后应稳定返回 `None`。
///
/// # 参数
///
/// - `error`: librdkafka offset/metadata 查询错误。
fn is_coordinator_lookup_transient(error: &KafkaError) -> bool {
    let code = match error {
        KafkaError::OffsetFetch(code)
        | KafkaError::MetadataFetch(code)
        | KafkaError::GroupListFetch(code)
        | KafkaError::MessageConsumption(code) => code,
        _ => return false,
    };
    matches!(
        code,
        RDKafkaErrorCode::WaitingForCoordinator
            | RDKafkaErrorCode::CoordinatorLoadInProgress
            | RDKafkaErrorCode::CoordinatorNotAvailable
            | RDKafkaErrorCode::NotCoordinator
    )
}

/// 检查单 topic 管理操作的逐项结果。
///
/// # 参数
///
/// - `results`: 底层返回的逐 topic 结果。
///
/// # 错误
///
/// 结果缺失或任一 topic 被 broker 拒绝时返回错误。
fn check_topic_result(
    results: Vec<std::result::Result<String, (String, RDKafkaErrorCode)>>,
    allow_existing: bool,
) -> Result<()> {
    match results.into_iter().next() {
        Some(Ok(_)) => Ok(()),
        Some(Err((_topic, RDKafkaErrorCode::TopicAlreadyExists))) if allow_existing => Ok(()),
        Some(Err((topic, error))) => Err(NafkaError::Broker(format!(
            "topic `{topic}` 管理操作失败: {error}"
        ))),
        None => Err(NafkaError::Broker("管理操作未返回逐项结果".into())),
    }
}

/// 把底层错误收敛为公共 broker 错误。
///
/// # 参数
///
/// - `error`: 不允许穿透公共 API 的底层错误。
fn broker_error(error: impl std::fmt::Display) -> NafkaError {
    NafkaError::Broker(error.to_string())
}

/// 把阻塞任务 join 失败收敛为公共 broker 错误。
///
/// # 参数
///
/// - `error`: 阻塞任务 join 错误。
fn join_error(error: tokio::task::JoinError) -> NafkaError {
    NafkaError::Broker(format!("阻塞查询任务失败: {error}"))
}
