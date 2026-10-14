//! 管理端、metadata 与离线 offset 底层适配。

use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;

use crate::admin::{TopicCreation, TopicDescription, TopicSpec};
use crate::config::KafkaConfig;
use crate::error::{NafkaError, Result};
use crate::types::Tp;

use super::config::{admin_config, consumer_config};

/// 一个 topic 的元数据条目；`error` 非空表示本轮观测不可信，不代表 topic 不存在。
pub(crate) struct TopicMetadata {
    /// topic 名。
    pub(crate) name: String,
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

/// 独立于关闭等待者的原生寿命；取消等待不能释放仍被请求使用的 Admin。
pub(crate) struct AdminClose {
    /// 原生销毁已返回，关闭线程只剩自身收尾。
    finished: std::sync::atomic::AtomicBool,
    /// 原生资源的其它持有者数量，不等同于 Kafka 逻辑请求数。
    holders: std::sync::atomic::AtomicUsize,
    /// 实际关闭线程，确认完成时只回收一次。
    worker: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl AdminClose {
    /// 业务作用：只读核对原生销毁和实际关闭线程均已退出，不取走线程或等待。
    /// 参数说明：无。
    /// 返回：完整本地资源退出才为 true；仍被其它请求借用时保持 false。
    pub(crate) fn is_closed(&self) -> bool {
        self.finished.load(std::sync::atomic::Ordering::Acquire)
            && self
                .worker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .is_none_or(|thread| thread.is_finished())
    }
    /// 业务作用：为已关闭准入的 Admin 建立唯一关闭责任，等待已接纳操作归还原生资源。
    /// 参数说明：client 为旧资源；调用者在启动成功前仍须保留源槽位引用。
    /// 返回：与调用方 future 无关的关闭状态；线程创建失败保留源槽位供再次关闭。
    pub(crate) fn start(client: Arc<AdminHandle>) -> Result<Arc<Self>> {
        let state = Arc::new(Self {
            finished: std::sync::atomic::AtomicBool::new(false),
            holders: std::sync::atomic::AtomicUsize::new(
                Arc::strong_count(&client).saturating_sub(1),
            ),
            worker: std::sync::Mutex::new(None),
        });
        let progress = Arc::clone(&state);
        let worker = std::thread::Builder::new()
            .name("nafka-admin-close".into())
            .spawn(move || {
                loop {
                    let remaining = Arc::strong_count(&client).saturating_sub(1);
                    progress
                        .holders
                        .store(remaining, std::sync::atomic::Ordering::Release);
                    if remaining == 0 {
                        break;
                    }
                    // 阻塞查询与被取消的修改仍持有资源，不能先报告关闭或在异步执行器线程上销毁客户端。
                    std::thread::sleep(Duration::from_millis(5));
                }
                drop(client);
                progress
                    .finished
                    .store(true, std::sync::atomic::Ordering::Release);
            })
            .map_err(|_| NafkaError::Lifecycle("无法建立 Admin 关闭线程".into()))?;
        *state
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(worker);
        Ok(state)
    }

    /// 业务作用：在宿主总期限内观察原生资源真正退出，不把本地等待结束当作关闭完成。
    /// 参数说明：deadline 为整个停机共用的绝对期限。返回：实际销毁和关闭线程均退出才为 true，超时保留责任供重试。
    pub(crate) async fn await_closed(&self, deadline: Instant) -> bool {
        while !self.finished.load(std::sync::atomic::Ordering::Acquire) {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return false;
            }
            tokio::time::sleep(left.min(Duration::from_millis(5))).await;
        }
        let worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        worker.is_none_or(|worker| worker.join().is_ok())
    }

    /// 业务作用：为停机期限耗尽提供当前资源责任证据。参数说明：无。返回：最近观测到的其它持有者数量；零不代表原生销毁已经结束。
    pub(crate) fn resource_holders(&self) -> usize {
        self.holders.load(std::sync::atomic::Ordering::Acquire)
    }
}

impl AdminHandle {
    /// 业务作用：在有界阻塞任务中读取 broker 的物理集群身份，不能用连接地址伪造恢复证据。
    /// 参数说明：无，使用当前 Admin 的总预算。
    /// 返回：非空 cluster ID；本轮无法获得身份时拒绝。
    pub(crate) async fn cluster_id(self: &Arc<Self>) -> Result<String> {
        let deadline = Instant::now() + self.timeout;
        let owner = Arc::clone(self);
        blocking_until(deadline, None, move || {
            owner
                .client
                .inner()
                .fetch_cluster_id(Timeout::After(query_budget(deadline)?))
                .filter(|identity| !identity.is_empty())
                .ok_or_else(|| NafkaError::Broker("Kafka 物理集群身份不可观测".into()))
        })
        .await
    }
    /// 业务作用：构造管理客户端。
    ///
    /// # 参数
    ///
    /// - `config`: 冻结后的顶层配置。
    ///
    /// # 错误
    ///
    /// 原生配置或客户端构造失败时返回错误。
    pub(crate) fn create(config: &KafkaConfig) -> Result<Arc<Self>> {
        let mut native = admin_config(config)?;
        // 元数据观测不能创建资源；缺失和无权必须作为证据不足返回给调用方。
        native.set("allow.auto.create.topics", "false");
        let client = native
            .create::<AdminClient<DefaultClientContext>>()
            .map_err(|error| {
                NafkaError::Config(format!(
                    "admin 客户端构造失败: {}",
                    super::config::redact(&error)
                ))
            })?;
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

    /// 业务作用：查询全部可见 topic 及其元数据状态。
    /// 参数说明：无。返回：可见资源名称和资源级错误，使用单一查询预算。
    ///
    /// # 错误
    ///
    /// metadata 请求失败时返回 broker 错误。
    pub(crate) async fn topic_metadata(self: &Arc<Self>) -> Result<Vec<TopicMetadata>> {
        let deadline = Instant::now() + self.timeout;
        let owner = Arc::clone(self);
        blocking_until(deadline, None, move || {
            let metadata = owner
                .client
                .inner()
                .fetch_metadata(None, Timeout::After(query_budget(deadline)?))
                .map_err(broker_error)?;
            let mut topics = Vec::with_capacity(metadata.topics().len());
            for topic in metadata.topics() {
                // 携带错误的条目**不能**被静默丢弃：调用方无法把
                // "broker 明确说没有这个 topic" 与 "controller 切换/分区迁移期间这一轮读不到"
                // 区分开，会把一次滚动重启误判成确定的契约漂移并永久停机。
                // 这里把错误原样带出去，由调用方按语义裁决。
                topics.push(TopicMetadata {
                    name: topic.name().to_owned(),
                    error: topic.error().map(|error| format!("{error:?}")),
                });
            }
            topics.sort_by(|left, right| left.name.cmp(&right.name));
            Ok(topics)
        })
        .await
    }

    /// 业务作用：查询明确指定的资源是否存在，不把列表中不可见的 topic 当作不存在。
    /// 参数说明：topic 为经过校验的资源名。返回：仅 broker 明确 UnknownTopicOrPartition 才为 false；权限或不完整元数据均拒绝。
    pub(crate) async fn topic_exists(self: &Arc<Self>, topic: String) -> Result<bool> {
        let owner = Arc::clone(self);
        let deadline = Instant::now() + self.timeout;
        blocking_until(deadline, None, move || {
            let metadata = owner
                .client
                .inner()
                .fetch_metadata(Some(&topic), Timeout::After(query_budget(deadline)?))
                .map_err(broker_error)?;
            let resource = metadata
                .topics()
                .iter()
                .find(|entry| entry.name() == topic)
                .ok_or_else(|| NafkaError::Broker("资源查询缺少目标结果".into()))?;
            match resource.error().map(RDKafkaErrorCode::from) {
                None => Ok(true),
                Some(RDKafkaErrorCode::UnknownTopicOrPartition) => Ok(false),
                Some(code) if super::errors::authorization(code) => Err(NafkaError::Authorization),
                Some(code) => Err(NafkaError::Broker(format!("资源存在性不可观测: {code:?}"))),
            }
        })
        .await
    }

    /// 业务作用：在一次总预算内读取 topic 的分区、副本及所请求配置。
    /// 参数说明：topic 为已校验目标名；include_configs 决定是否执行 DescribeConfigs。
    /// 返回：完整描述；metadata、配置查询或阻塞任务失败均返回错误，不把不完整证据视为资源不存在。
    pub(crate) async fn describe_topic(
        self: &Arc<Self>,
        topic: String,
        include_configs: bool,
    ) -> Result<TopicDescription> {
        let deadline = Instant::now() + self.timeout;
        self.describe_topic_until(topic, include_configs, deadline)
            .await
    }

    /// 业务作用：为只读合同核验提供同一截止时刻内的完整 topic 证据。
    /// 参数说明：topic 为目标，include_configs 表示请求配置，deadline 为调用方总期限。
    /// 返回：完整描述或查询失败；阻塞查询与实际客户端寿命继续由源持有。
    pub(crate) async fn describe_topic_until(
        self: &Arc<Self>,
        topic: String,
        include_configs: bool,
        deadline: Instant,
    ) -> Result<TopicDescription> {
        let owner = Arc::clone(self);
        let metadata_topic = topic.clone();
        let (partitions, replication_factor) = blocking_until(deadline, None, move || {
            let metadata = owner
                .client
                .inner()
                .fetch_metadata(
                    Some(&metadata_topic),
                    Timeout::After(query_budget(deadline)?),
                )
                .map_err(broker_error)?;
            let item = metadata
                .topics()
                .iter()
                .find(|item| item.name() == metadata_topic)
                .ok_or_else(|| NafkaError::Broker(format!("topic 不存在: {metadata_topic}")))?;
            if let Some(error) = item.error() {
                if super::errors::authorization(error.into()) {
                    return Err(NafkaError::Authorization);
                }
                return Err(NafkaError::Broker(format!(
                    "topic `{metadata_topic}` metadata 失败: {error:?}"
                )));
            }
            let mut partitions = Vec::with_capacity(item.partitions().len());
            let mut factors = std::collections::BTreeSet::new();
            for partition in item.partitions() {
                if let Some(error) = partition.error() {
                    if super::errors::authorization(error.into()) {
                        return Err(NafkaError::Authorization);
                    }
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
        .await?;

        let configs = if include_configs {
            let owner = Arc::clone(self);
            let config_topic = topic.clone();
            // 配置决定 DLT 是否可安全留存；必须检查资源级响应，不能仅凭请求级成功采用空配置。
            blocking_until(deadline, None, move || {
                let remaining = query_budget(deadline)?;
                super::admin_mutation::describe_topic_configs(
                    &owner.client,
                    &config_topic,
                    remaining,
                    owner.request_timeout.min(remaining),
                )
            })
            .await?
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

    /// 业务作用：创建一个 topic。
    ///
    /// # 参数
    ///
    /// - `spec`: 已完成公共层校验的 topic 契约。
    ///
    /// # 错误
    ///
    /// 请求或逐项 broker 结果失败时返回错误。
    pub(crate) async fn create_topic(&self, spec: &TopicSpec) -> Result<()> {
        self.create_topic_inner(spec, false).await.map(|_| ())
    }

    /// 业务作用：创建尚不存在的 topic，并区分新建与已存在的结果。
    /// 参数说明：spec 为公共层已校验的 topic 声明。
    /// 返回：明确新建为 Created，明确已存在为 AlreadyExists；不据已存在结果推定配置相符。权限拒绝为 Authorization，已提交请求超时为 AdminOutcomeUnknown。
    pub(crate) async fn create_topic_if_absent(&self, spec: &TopicSpec) -> Result<TopicCreation> {
        self.create_topic_inner(spec, true).await
    }

    /// 业务作用：在独立有界等待中增量设置或恢复指定 topic 配置，不覆盖其它项。
    /// 参数说明：topic 为目标，changes 为配置变更，None 表示恢复默认值。
    /// 返回：目标资源明确确认才成功；拒绝和结果未知均保留，不通过缺失响应推定成功。
    pub(crate) async fn alter_topic_configs(
        self: &Arc<Self>,
        topic: &str,
        changes: std::collections::BTreeMap<String, Option<String>>,
    ) -> Result<()> {
        let owner = Arc::clone(self);
        let topic = topic.to_owned();
        let deadline = Instant::now() + self.timeout;
        // 等待 future 被丢弃时，实际请求仍持有客户端直到结果或期限；不能释放其原生指针。
        blocking_until(deadline, Some("incremental-alter-configs"), move || {
            let remaining = query_budget(deadline)?;
            super::admin_mutation::set_topic_configs(
                &owner.client,
                &topic,
                &changes,
                remaining,
                owner.request_timeout.min(remaining),
            )
        })
        .await
    }

    /// 业务作用：向 broker 提交 topic 创建请求，只以该资源的明确响应区分创建与已经存在。
    /// 参数说明：spec 为完整资源声明，allow_existing 决定是否接纳同名已存在结果。
    /// 返回：目标资源的确认结果；超时或缺失/错配响应为未知，不能推导尚未创建。
    async fn create_topic_inner(
        &self,
        spec: &TopicSpec,
        allow_existing: bool,
    ) -> Result<TopicCreation> {
        let mut topic = NewTopic::new(
            &spec.name,
            spec.partitions,
            TopicReplication::Fixed(spec.replication_factor),
        );
        for (key, value) in &spec.configs {
            topic = topic.set(key, value);
        }
        let request = self.client.create_topics(
            [&topic],
            &AdminOptions::new()
                .request_timeout(Some(self.request_timeout))
                .operation_timeout(Some(self.timeout)),
        );
        await_topic_result(
            request,
            &spec.name,
            "create-topics",
            allow_existing,
            self.timeout,
        )
        .await
    }

    /// 业务作用：删除一个 topic。
    ///
    /// # 参数
    ///
    /// - `topic`: 目标 topic。
    ///
    /// # 错误
    ///
    /// 请求或逐项 broker 结果失败时返回错误。
    pub(crate) async fn delete_topic(&self, topic: &str) -> Result<()> {
        let request = self.client.delete_topics(
            &[topic],
            &AdminOptions::new()
                .request_timeout(Some(self.request_timeout))
                .operation_timeout(Some(self.timeout)),
        );
        await_topic_result(request, topic, "delete-topics", false, self.timeout)
            .await
            .map(|_| ())
    }

    /// 业务作用：把 topic 总分区数增加到指定值。
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
        let request = self.client.create_partitions(
            [&partitions],
            &AdminOptions::new()
                .request_timeout(Some(self.request_timeout))
                .operation_timeout(Some(self.timeout)),
        );
        await_topic_result(request, topic, "create-partitions", false, self.timeout)
            .await
            .map(|_| ())
    }

    /// 业务作用：在共同预算内读取指定分区的保留起点和普通日志末尾。
    /// 参数说明：tps 为已校验的目标分区。
    /// 返回：完整逐分区水位；任一查询失败返回错误，不交付部分位置；普通末尾不能代替 read_committed 的 LSO。
    pub(crate) async fn watermarks(self: &Arc<Self>, tps: Vec<Tp>) -> Result<Vec<(Tp, i64, i64)>> {
        self.watermarks_until(tps, Instant::now() + self.timeout)
            .await
    }

    /// 业务作用：让组合 lag 查询中的所有分区共享原调用预算。
    /// 参数说明：tps 为已校验目标，deadline 为组合查询截止时间。返回：完整水位或失败，不重新授予等待额度。
    pub(crate) async fn watermarks_until(
        self: &Arc<Self>,
        tps: Vec<Tp>,
        deadline: Instant,
    ) -> Result<Vec<(Tp, i64, i64)>> {
        let owner = Arc::clone(self);
        blocking_until(deadline, None, move || {
            let mut values = Vec::with_capacity(tps.len());
            for tp in tps {
                let (low, high) = owner
                    .client
                    .inner()
                    .fetch_watermarks(
                        &tp.topic,
                        tp.partition,
                        Timeout::After(query_budget(deadline)?),
                    )
                    .or_else(|error| {
                        // 原生水位路径可能把不可见 topic 折叠为本地 UnknownPartition；用明确目标响应保留真正的 ACL 拒绝。
                        if super::errors::code(&error) == Some(RDKafkaErrorCode::UnknownPartition) {
                            let metadata = owner
                                .client
                                .inner()
                                .fetch_metadata(
                                    Some(&tp.topic),
                                    Timeout::After(query_budget(deadline)?),
                                )
                                .map_err(broker_error)?;
                            if let Some(code) = metadata
                                .topics()
                                .iter()
                                .find(|topic| topic.name() == tp.topic)
                                .and_then(|topic| topic.error())
                            {
                                return Err(broker_error(KafkaError::MetadataFetch(code.into())));
                            }
                        }
                        Err(broker_error(error))
                    })?;
                values.push((tp, low, high));
            }
            Ok(values)
        })
        .await
    }

    /// 业务作用：通过不订阅 topic 的短命 consumer 查询组提交位置。
    /// 参数说明：group 为实际 group.id；tps 为目标分区。
    /// 返回：逐分区明确位置或 broker 确认的无历史；资源拒绝、缺项和非法位置不转为 None，整个查询共用一个总预算。
    pub(crate) async fn committed_offsets(
        self: &Arc<Self>,
        group: String,
        tps: Vec<Tp>,
    ) -> Result<Vec<(Tp, Option<i64>)>> {
        self.committed_offsets_until(group, tps, Instant::now() + self.timeout)
            .await
    }

    /// 业务作用：在组合查询的剩余预算内取得组位点，避免 coordinator 重试延长整体等待。
    /// 参数说明：group 为组名，tps 为已校验目标，deadline 为共同截止时间。返回：完整组位点；无权限、缺项或期限耗尽均失败。
    pub(crate) async fn committed_offsets_until(
        self: &Arc<Self>,
        group: String,
        tps: Vec<Tp>,
        deadline: Instant,
    ) -> Result<Vec<(Tp, Option<i64>)>> {
        let owner = Arc::clone(self);
        blocking_until(deadline, None, move || {
            let consumer = consumer_config(&owner.config, &group)?
                .create::<BaseConsumer>()
                .map_err(|error| {
                    NafkaError::Config(format!(
                        "offset 查询客户端构造失败: {}",
                        super::config::redact(&error)
                    ))
                })?;
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
                let result = consumer.committed_offsets(
                    query,
                    Timeout::After(remaining.min(Duration::from_millis(250))),
                );
                // 协调器拒绝可能只进入 consumer 事件队列；先读明确拒绝，不能被位点等待的超时覆盖。
                if let Some(error) = poll_query_rejection(&consumer) {
                    return Err(broker_error(error));
                }
                match result {
                    Ok(committed) => break committed,
                    Err(error) if is_coordinator_lookup_transient(&error) => {
                        let sleep_for = deadline
                            .saturating_duration_since(Instant::now())
                            .min(Duration::from_millis(100));
                        if sleep_for.is_zero() {
                            return Err(broker_error(error));
                        }
                        std::thread::sleep(sleep_for);
                    }
                    Err(error) => return Err(broker_error(error)),
                }
            };
            let mut values = Vec::with_capacity(tps.len());
            for tp in tps {
                let element = committed
                    .find_partition(&tp.topic, tp.partition)
                    .ok_or_else(|| NafkaError::Broker("位点查询缺少目标分区结果".into()))?;
                // 资源级拒绝不是没有提交历史；检查错误后才允许把 Offset::Invalid 解释为无位点。
                element.error().map_err(broker_error)?;
                let offset = match element.offset() {
                    Offset::Offset(value) if value >= 0 => Some(value),
                    Offset::Invalid => None,
                    _ => return Err(NafkaError::Broker("位点查询返回非法位置".into())),
                };
                values.push((tp, offset));
            }
            Ok(values)
        })
        .await
    }
}

/// 业务作用：限制包含阻塞线程排队在内的本地等待，调用者期限耗尽不丢弃实际原生责任。
/// 参数说明：deadline 为共同截止时间，operation 为可能修改 broker 的操作名，job 持有真实客户端直到退出。
/// 返回：期限内的真实结果；超时只结束调用者等待，修改结果为未知，后台仍由源级关闭追踪。
async fn blocking_until<T: Send + 'static>(
    deadline: Instant,
    operation: Option<&'static str>,
    job: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    let task = tokio::task::spawn_blocking(job);
    // 丢弃 JoinHandle 不会取消已经排队或执行的阻塞任务；其 Arc 在实际任务内持有，关闭不能提前成功。
    match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), task).await {
        Ok(result) => result.map_err(join_error)?,
        Err(_) => Err(operation.map_or_else(
            || NafkaError::Broker("管理查询总预算已耗尽".into()),
            |operation| NafkaError::AdminOutcomeUnknown { operation },
        )),
    }
}

/// 业务作用：让组合只读查询和阻塞任务排队共用同一预算，避免按分区或步骤延长等待。
/// 参数说明：deadline 为整个查询的绝对截止时间。返回：剩余正预算；耗尽时只表示本轮观测失败，不改变任何组位点。
fn query_budget(deadline: Instant) -> Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(NafkaError::Broker("管理查询总预算已耗尽".into()))
    } else {
        Ok(remaining)
    }
}

/// 业务作用：允许协调器发现与短查询切片耗尽在同一总期限内重试。
/// 参数说明：error 为原生 offset/metadata 查询错误；调用方已经读取消费事件中的明确权限拒绝。
/// 返回：仅协调器暂不可用或本轮等待耗尽为 true；不把权限拒绝转换为无提交历史。
pub(super) fn is_coordinator_lookup_transient(error: &KafkaError) -> bool {
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
            | RDKafkaErrorCode::OperationTimedOut
            | RDKafkaErrorCode::RequestTimedOut
            | RDKafkaErrorCode::TimedOutQueue
    )
}

/// 业务作用：读取未订阅查询客户端的明确权限拒绝，避免协调器事件被查询超时掩盖。
/// 参数说明：consumer 为本次只读查询专用客户端，不承载业务记录。
/// 返回：本轮至多处理16个即时事件；仅结构化授权错误作为拒绝证据，其它事件不推导权限。
pub(super) fn poll_query_rejection(consumer: &BaseConsumer) -> Option<KafkaError> {
    for _ in 0..16 {
        match consumer.poll(Duration::ZERO) {
            Some(Err(error))
                if super::errors::code(&error).is_some_and(super::errors::authorization) =>
            {
                return Some(error);
            }
            Some(_) => {}
            None => break,
        }
    }
    None
}

/// 业务作用：在单一总预算内等待管理修改，并核对唯一目标资源的确认。
/// 参数说明：request 为已提交原生请求；topic 为唯一目标；operation 为固定操作名；allow_existing 为幂等创建策略；timeout 为总预算。
/// 返回：真实成功或允许的已存在结果；超时、丢失或错配响应均为结果未知，权限拒绝保留独立类别。
async fn await_topic_result(
    request: impl std::future::Future<
        Output = rdkafka::error::KafkaResult<Vec<rdkafka::admin::TopicResult>>,
    >,
    topic: &str,
    operation: &'static str,
    allow_existing: bool,
    timeout: Duration,
) -> Result<TopicCreation> {
    // 丢弃等待只能撤回本地观察者，已提交的原生修改可能继续完成，不能自动重发业务操作。
    let results = tokio::time::timeout(timeout, request)
        .await
        .map_err(|_| NafkaError::AdminOutcomeUnknown { operation })?
        .map_err(|error| mutation_error(error, operation))?;
    if results.len() != 1 {
        return Err(NafkaError::AdminOutcomeUnknown { operation });
    }
    match results.into_iter().next() {
        Some(Ok(name)) if name == topic => Ok(TopicCreation::Created),
        Some(Err((name, _))) if name != topic => Err(NafkaError::AdminOutcomeUnknown { operation }),
        Some(Err((_, RDKafkaErrorCode::TopicAlreadyExists))) if allow_existing => {
            Ok(TopicCreation::AlreadyExists)
        }
        Some(Err((_, error))) if super::errors::authorization(error) => {
            Err(NafkaError::Authorization)
        }
        Some(Err((_, error))) if uncertain_mutation_code(error) => {
            Err(NafkaError::AdminOutcomeUnknown { operation })
        }
        Some(Err((topic, error))) => Err(NafkaError::Broker(format!(
            "topic `{topic}` 管理操作失败: {error}"
        ))),
        _ => Err(NafkaError::AdminOutcomeUnknown { operation }),
    }
}

/// 业务作用：识别不能据以判定管理修改未发生的原生通信结果。
/// 参数说明：code 为请求或资源错误码。返回：超时、传输中断和无效响应属于未知结果。
fn uncertain_mutation_code(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::OperationTimedOut
            | RDKafkaErrorCode::BadMessage
            | RDKafkaErrorCode::RequestTimedOut
            | RDKafkaErrorCode::TimedOutQueue
            | RDKafkaErrorCode::BrokerTransportFailure
            | RDKafkaErrorCode::NetworkException
            | RDKafkaErrorCode::AllBrokersDown
    )
}

/// 业务作用：按是否具有明确拒绝证据转换已提交管理请求的失败。
/// 参数说明：error 为原生请求错误，operation 为固定管理操作名。返回：授权拒绝、结果未知或普通明确失败，不通过错误文本猜测。
fn mutation_error(error: KafkaError, operation: &'static str) -> NafkaError {
    if matches!(error, KafkaError::Canceled)
        || super::errors::code(&error).is_some_and(uncertain_mutation_code)
    {
        NafkaError::AdminOutcomeUnknown { operation }
    } else {
        broker_error(error)
    }
}

/// 业务作用：把底层错误收敛为公共类别，保留明确授权拒绝供调用方选择权限处置。
/// 参数说明：error 为不允许穿透公共 API 的底层错误。返回：授权拒绝或脱敏 broker 诊断，不推断超时请求的最终结果。
fn broker_error(error: KafkaError) -> NafkaError {
    if super::errors::code(&error).is_some_and(super::errors::authorization) {
        NafkaError::Authorization
    } else {
        NafkaError::Broker(error.to_string())
    }
}

/// 业务作用：把阻塞任务 join 失败收敛为公共 broker 错误。
///
/// # 参数
///
/// - `error`: 阻塞任务 join 错误。
fn join_error(error: tokio::task::JoinError) -> NafkaError {
    NafkaError::Broker(format!("阻塞查询任务失败: {error}"))
}
