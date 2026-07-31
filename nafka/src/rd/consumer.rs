//! 底层 BaseConsumer、消息借用视图与 owner 控制适配。

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use rdkafka::client::ClientContext;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Headers, Message, Timestamp};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;

use crate::config::KafkaConfig;
use crate::error::{NafkaError, Result};
use crate::metrics::MetricsSink;
use crate::types::{StartOffset, Tp};

use super::config::consumer_config;

/// poll loop 可恢复性分类后的底层错误。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RdPollError {
    /// 临时传输、协调器或 metadata 错误；owner 应按预算重建 consumer。
    Transient(String),
    /// 认证、授权、fencing 或底层 fatal 错误；owner 不得自动重建。
    Fatal(String),
}

impl std::fmt::Display for RdPollError {
    /// 输出不暴露底层类型的分类文本。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transient(message) => write!(formatter, "transient poll error: {message}"),
            Self::Fatal(message) => write!(formatter, "fatal poll error: {message}"),
        }
    }
}

/// owner 可观察的 rebalance 结果类型。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RdRebalanceKind {
    /// 新 assignment 已由底层安装完成。
    Assigned,
    /// revoke 已由底层执行完成。
    Revoked,
    /// 协调器返回 rebalance 错误。
    Error,
}

/// 单槽 rebalance mailbox 事件。
#[derive(Clone, Debug)]
pub(crate) struct RdRebalanceEvent {
    /// 每次 callback 单调递增的 owner epoch。
    pub(crate) epoch: u64,
    /// 事件类型。
    pub(crate) kind: RdRebalanceKind,
    /// callback 提供的实际分区集合。
    pub(crate) partitions: Vec<Tp>,
    /// 错误事件的安全诊断文本。
    pub(crate) error: Option<String>,
    /// revoke 窗口内已同步提交成功的安全前沿。
    pub(crate) committed: BTreeMap<Tp, i64>,
}

/// pre-rebalance 提交结果，等待 post callback 合并进单槽事件。
struct PreRebalanceResult {
    /// 成功时的已提交 offsets；失败时为空。
    committed: BTreeMap<Tp, i64>,
    /// 提交失败或超过时长预算的诊断文本。
    error: Option<String>,
}

/// revoke 窗口内有硬截止时间的底层提交结果。
#[derive(Clone, Debug, PartialEq, Eq)]
enum BoundedCommitOutcome {
    /// broker 已在窗口内确认全部安全前沿。
    Committed,
    /// broker 或客户端在窗口内返回确定错误。
    Failed(String),
    /// 专用结果队列在截止时间内没有得到提交结果。
    TimedOut,
}

/// 后台 revoke commit 生命周期守卫，确保成功、错误或 panic 都释放单飞门禁。
struct PreCommitGuard {
    /// 被任务持有到提交结束的 owner consumer。
    owner: Arc<BaseConsumer<OwnerConsumerContext>>,
}

impl Drop for PreCommitGuard {
    /// 释放当前 consumer 的 revoke commit 单飞门禁。
    fn drop(&mut self) {
        self.owner
            .context()
            .pre_commit_in_flight
            .store(false, Ordering::Release);
    }
}

/// 底层 consumer context，只把合并后的 rebalance 事实交给 owner。
struct OwnerConsumerContext {
    /// 下一次 callback epoch。
    epoch: AtomicU64,
    /// 当前 poll 周期最后一个事件；容量恒为一。
    mailbox: Mutex<Option<RdRebalanceEvent>>,
    /// owner 最近发布的安全提交快照。
    pending_offsets: Mutex<BTreeMap<Tp, i64>>,
    /// Revoke 前置提交结果。
    pre_rebalance: Mutex<Option<PreRebalanceResult>>,
    /// revoke 同步提交时长预算，用于越界观测。
    rebalance_commit_timeout: Duration,
    /// 构造完成后回填的 owner consumer 弱引用，供有界后台提交安全持有生命周期。
    owner_consumer: OnceLock<Weak<BaseConsumer<OwnerConsumerContext>>>,
    /// 每个 consumer 最多允许一个超时后仍在收尾的 revoke commit。
    pre_commit_in_flight: AtomicBool,
    /// librdkafka 后台线程 broker 级错误(ALL_BROKERS_DOWN/认证/SSL 等)的最新一条
    /// callback 只做分类与单槽覆盖写,由 `GroupHealth` 读侧合并进 `last_error`,不进 owner 控制流。
    global_error: Arc<Mutex<Option<String>>>,
}

impl OwnerConsumerContext {
    /// 构造空 mailbox。
    ///
    /// # 参数
    ///
    /// - `rebalance_commit_timeout`: revoke 同步提交预算。
    /// - `global_error`: 与 supervisor 共享的 broker 级错误单槽(跨会话重建持续存活)。
    fn new(rebalance_commit_timeout: Duration, global_error: Arc<Mutex<Option<String>>>) -> Self {
        Self {
            epoch: AtomicU64::new(0),
            mailbox: Mutex::new(None),
            pending_offsets: Mutex::new(BTreeMap::new()),
            pre_rebalance: Mutex::new(None),
            rebalance_commit_timeout,
            owner_consumer: OnceLock::new(),
            pre_commit_in_flight: AtomicBool::new(false),
            global_error,
        }
    }

    /// 在 consumer 构造完成后回填自身弱引用，且只允许成功一次。
    ///
    /// # 参数
    ///
    /// - `consumer`: owner 唯一 consumer 的弱引用。
    fn set_owner_consumer(&self, consumer: Weak<BaseConsumer<OwnerConsumerContext>>) {
        let _ = self.owner_consumer.set(consumer);
    }

    /// 覆盖写入当前 callback 结果。
    ///
    /// # 参数
    ///
    /// - `kind`: rebalance 类型。
    /// - `partitions`: callback 提供的分区。
    /// - `error`: 可选错误文本。
    fn publish(
        &self,
        kind: RdRebalanceKind,
        partitions: Vec<Tp>,
        error: Option<String>,
        committed: BTreeMap<Tp, i64>,
    ) {
        let epoch = self.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        *self
            .mailbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(RdRebalanceEvent {
            epoch,
            kind,
            partitions,
            error,
            committed,
        });
    }

    /// 取走当前合并事件。
    fn take(&self) -> Option<RdRebalanceEvent> {
        self.mailbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    /// 判断 mailbox 是否已有待处理事件但不取走它。
    fn has_event(&self) -> bool {
        self.mailbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }

    /// 替换 owner 当前可以安全提交的 offsets 快照。
    ///
    /// # 参数
    ///
    /// - `offsets`: 按分区合并后的安全前沿。
    fn set_pending_offsets(&self, offsets: &BTreeMap<Tp, i64>) {
        *self
            .pending_offsets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = offsets.clone();
    }
}

impl ClientContext for OwnerConsumerContext {
    /// 把 librdkafka 后台线程的 broker 级错误写入单槽,供 `GroupHealth.last_error` 读侧合并。
    ///
    /// 只收 ALL_BROKERS_DOWN 与认证/授权/SSL 类(其余 per-broker transport 抖动过滤——重连期噪声大且
    /// 会被聚合成 ALL_BROKERS_DOWN);覆盖写保留最新一条。文本含 broker host:port(与 安全 endpoint
    /// 同口径,不含凭据),可经 `last_error` 对外展示。
    ///
    /// # 参数
    ///
    /// - `error`: librdkafka 上报的客户端级错误。
    /// - `reason`: librdkafka 附带的人类可读原因。
    fn error(&self, error: KafkaError, reason: &str) {
        if error
            .rdkafka_error_code()
            .is_some_and(is_global_health_code)
        {
            *self
                .global_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some(format!("{error}: {reason}"));
        }
    }
}

/// 判定一个错误码是否值得进入 `GroupHealth.last_error` 的 broker 级单槽。
///
/// # 参数
///
/// - `code`: librdkafka 错误码。
fn is_global_health_code(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::AllBrokersDown
            | RDKafkaErrorCode::Authentication
            | RDKafkaErrorCode::SaslAuthenticationFailed
            | RDKafkaErrorCode::UnsupportedSASLMechanism
            | RDKafkaErrorCode::SSL
            | RDKafkaErrorCode::GroupAuthorizationFailed
            | RDKafkaErrorCode::TopicAuthorizationFailed
            | RDKafkaErrorCode::ClusterAuthorizationFailed
    )
}

impl ConsumerContext for OwnerConsumerContext {
    /// 在 generation 仍有效的 revoke 窗口内同步提交被收回分区的安全前沿。
    fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let Rebalance::Revoke(partitions) = rebalance else {
            return;
        };
        let revoked = elements_to_tps(partitions);
        let snapshot = self
            .pending_offsets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let offsets = select_revoke_offsets(&revoked, &snapshot, consumer.assignment_lost());
        let result = if offsets.is_empty() {
            PreRebalanceResult {
                committed: BTreeMap::new(),
                error: None,
            }
        } else {
            match commit_offsets_bounded(consumer, &offsets, self.rebalance_commit_timeout) {
                BoundedCommitOutcome::Committed => PreRebalanceResult {
                    committed: offsets,
                    error: None,
                },
                BoundedCommitOutcome::Failed(error) => PreRebalanceResult {
                    committed: BTreeMap::new(),
                    error: Some(error),
                },
                BoundedCommitOutcome::TimedOut => PreRebalanceResult {
                    committed: BTreeMap::new(),
                    error: Some(format!(
                        "revoke commit 超过硬截止时间: budget_ms={}",
                        self.rebalance_commit_timeout.as_millis()
                    )),
                },
            }
        };
        *self
            .pre_rebalance
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
    }

    /// 默认 assign/unassign 完成后发布事实，避免用启动前的空 assignment 冒充已 join。
    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(partitions) => self.publish(
                RdRebalanceKind::Assigned,
                elements_to_tps(partitions),
                None,
                BTreeMap::new(),
            ),
            Rebalance::Revoke(partitions) => {
                let result = self
                    .pre_rebalance
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .unwrap_or(PreRebalanceResult {
                        committed: BTreeMap::new(),
                        error: None,
                    });
                self.publish(
                    RdRebalanceKind::Revoked,
                    elements_to_tps(partitions),
                    result.error,
                    result.committed,
                );
            }
            Rebalance::Error(error) => self.publish(
                RdRebalanceKind::Error,
                Vec::new(),
                Some(error.to_string()),
                BTreeMap::new(),
            ),
        }
    }
}

/// 底层 header 的只读借用视图。
#[derive(Clone, Copy)]
pub(crate) struct RdHeaderRef<'a> {
    /// header 名称。
    pub(crate) name: &'a str,
    /// 可空 header 值。
    pub(crate) value: Option<&'a [u8]>,
}

/// poll 返回消息的底层隔离借用视图。
pub(crate) struct RdMessage<'a> {
    /// 只在本模块出现的底层借用消息。
    inner: rdkafka::message::BorrowedMessage<'a>,
}

impl RdMessage<'_> {
    /// 返回 topic。
    pub(crate) fn topic(&self) -> &str {
        self.inner.topic()
    }

    /// 返回 partition。
    pub(crate) fn partition(&self) -> i32 {
        self.inner.partition()
    }

    /// 返回当前 offset。
    pub(crate) fn offset(&self) -> i64 {
        self.inner.offset()
    }

    /// 返回消息时间戳毫秒，缺失时为 -1。
    pub(crate) fn timestamp(&self) -> i64 {
        match self.inner.timestamp() {
            Timestamp::NotAvailable => -1,
            Timestamp::CreateTime(value) | Timestamp::LogAppendTime(value) => value,
        }
    }

    /// 返回原始 key 借用。
    pub(crate) fn key(&self) -> Option<&[u8]> {
        self.inner.key()
    }

    /// 返回原始 payload 借用。
    pub(crate) fn payload(&self) -> Option<&[u8]> {
        self.inner.payload()
    }

    /// 物化轻量 header 描述符，header 名和值本身仍借用底层消息。
    ///
    /// rust-rdkafka 0.39 的借用 header 迭代器会对非 UTF-8 名称执行 `unwrap`。Kafka wire
    /// 并不保证外部生产者写入的名称是 UTF-8，因此必须在隔离层截获该 panic，并把它交给
    /// `invalid_record_policy`，不能让一条毒消息击穿 owner 会话。
    pub(crate) fn headers(&self) -> Result<Vec<RdHeaderRef<'_>>> {
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.inner
                .headers()
                .map(|headers| {
                    headers
                        .iter()
                        .map(|header| RdHeaderRef {
                            name: header.key,
                            value: header.value,
                        })
                        .collect()
                })
                .unwrap_or_default()
        }))
        .map_err(|_| NafkaError::MalformedRoute("header 名不是 UTF-8".into()))
    }

    /// 返回复制为拥有型记录前的主要保留字节估算。
    pub(crate) fn retained_bytes(&self) -> usize {
        self.retained_bytes_with(self.headers().as_ref().ok())
    }

    /// 用已解析好的 headers 计量保留字节，避免对同一条消息重复解析。
    ///
    /// # 参数
    ///
    /// - `headers`: 已解析的 header 描述符；`None` 表示 header 名非 UTF-8 无法计量。
    pub(crate) fn retained_bytes_with(&self, headers: Option<&Vec<RdHeaderRef<'_>>>) -> usize {
        self.payload().map_or(0, <[u8]>::len)
            + self.key().map_or(0, <[u8]>::len)
            + headers.map_or(0, |headers| {
                headers
                    .iter()
                    .map(|header| header.name.len() + header.value.map_or(0, <[u8]>::len))
                    .sum::<usize>()
            })
    }
}

/// 只允许 owner 线程持有的底层 consumer 包装。
pub(crate) struct ConsumerClient {
    /// 唯一底层 BaseConsumer。
    consumer: Arc<BaseConsumer<OwnerConsumerContext>>,
    /// owner 控制与 commit 请求超时。
    timeout: Duration,
    /// 所属运行时的后端无关指标出口。
    metrics: Arc<dyn MetricsSink>,
    /// 最终 group id，仅用作低基数指标标签。
    group: Arc<str>,
}

impl ConsumerClient {
    /// 构造一个禁用自动提交与自动存储的 consumer。
    ///
    /// # 参数
    ///
    /// - `config`: 冻结顶层配置。
    /// - `group`: 最终 group.id。
    /// - `metrics`: 后端无关指标出口。
    /// - `global_error`: 与 supervisor 共享的 broker 级错误单槽。
    ///
    /// # 错误
    ///
    /// 原生配置或 consumer 构造失败时返回错误。
    pub(crate) fn create(
        config: &KafkaConfig,
        group: &str,
        metrics: Arc<dyn MetricsSink>,
        global_error: Arc<Mutex<Option<String>>>,
    ) -> Result<Self> {
        let consumer = consumer_config(config, group)?
            .create_with_context::<_, BaseConsumer<OwnerConsumerContext>>(
                OwnerConsumerContext::new(
                    Duration::from_millis(config.behavior.rebalance_commit_timeout_ms),
                    global_error,
                ),
            )
            .map_err(|error| NafkaError::Broker(format!("consumer 构造失败: {error}")))?;
        let consumer = Arc::new(consumer);
        consumer
            .context()
            .set_owner_consumer(Arc::downgrade(&consumer));
        Ok(Self {
            consumer,
            timeout: Duration::from_millis(config.behavior.control_timeout_ms),
            metrics,
            group: Arc::from(group),
        })
    }

    /// 发出一个带 group 标签的 consumer counter。
    pub(crate) fn counter(&self, name: &'static str, delta: u64) {
        // 用户实现的 panic 必须被隔离：本函数在 owner 线程上调用（commit 路径），
        // 一次 panic 会穿到 run_owner 的 catch_unwind 把 group 打成 Crashed。
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.metrics.counter(name, delta, &[("group", &self.group)]);
        }))
        .is_err()
        {
            tracing::error!(
                metric = name,
                "MetricsSink 实现 panic，已隔离并忽略本次上报"
            );
        }
    }

    /// 订阅一组 topic。
    ///
    /// # 参数
    ///
    /// - `topics`: 非空 topic 列表。
    ///
    /// # 错误
    ///
    /// 底层订阅失败时返回错误。
    pub(crate) fn subscribe(&self, topics: &[String]) -> Result<()> {
        let refs: Vec<&str> = topics.iter().map(String::as_str).collect();
        self.consumer.subscribe(&refs).map_err(broker_error)
    }

    /// 取消当前 subscribe 订阅。
    pub(crate) fn unsubscribe(&self) {
        self.consumer.unsubscribe();
    }

    /// 固定 assign 分区并解析起始位点。
    ///
    /// # 参数
    ///
    /// - `starts`: 每个固定分区的起始策略。
    /// - `auto_offset_reset`: Committed 无历史位点时的回退策略。
    ///
    /// # 错误
    ///
    /// 时间戳解析、列表构造或 assign 失败时返回错误。
    pub(crate) fn assign(
        &self,
        starts: &BTreeMap<Tp, StartOffset>,
        auto_offset_reset: &str,
    ) -> Result<()> {
        let resolution_deadline = Instant::now() + self.timeout;
        let mut direct = TopicPartitionList::with_capacity(starts.len());
        let mut timestamps = TopicPartitionList::with_capacity(starts.len());
        let mut committed_query = TopicPartitionList::with_capacity(starts.len());
        let mut has_timestamp = false;
        let mut has_committed = false;
        for (tp, start) in starts {
            match start {
                StartOffset::Timestamp(value) => {
                    timestamps
                        .add_partition_offset(&tp.topic, tp.partition, Offset::Offset(*value))
                        .map_err(broker_error)?;
                    has_timestamp = true;
                }
                StartOffset::Committed => {
                    committed_query.add_partition(&tp.topic, tp.partition);
                    has_committed = true;
                }
                StartOffset::Earliest => direct
                    .add_partition_offset(
                        &tp.topic,
                        tp.partition,
                        self.absolute_watermark(tp, false, resolution_deadline)?,
                    )
                    .map_err(broker_error)?,
                StartOffset::Latest => direct
                    .add_partition_offset(
                        &tp.topic,
                        tp.partition,
                        self.absolute_watermark(tp, true, resolution_deadline)?,
                    )
                    .map_err(broker_error)?,
                StartOffset::Offset(value) => direct
                    .add_partition_offset(&tp.topic, tp.partition, Offset::Offset(*value))
                    .map_err(broker_error)?,
            }
        }
        if has_committed {
            let committed = self
                .consumer
                .committed_offsets(committed_query, Timeout::After(self.timeout))
                .map_err(broker_error)?;
            for element in committed.elements() {
                let target = match element.offset() {
                    Offset::Offset(value) if value >= 0 => Offset::Offset(value),
                    _ => match auto_offset_reset {
                        "earliest" => self.absolute_watermark(
                            &Tp::new(element.topic(), element.partition()),
                            false,
                            resolution_deadline,
                        )?,
                        "latest" => self.absolute_watermark(
                            &Tp::new(element.topic(), element.partition()),
                            true,
                            resolution_deadline,
                        )?,
                        "none" => {
                            return Err(NafkaError::Broker(format!(
                                "分区 {}:{} 没有 committed offset，且 auto.offset.reset=none",
                                element.topic(),
                                element.partition()
                            )))
                        }
                        other => {
                            return Err(NafkaError::Config(format!(
                                "未知 auto_offset_reset: {other}"
                            )))
                        }
                    },
                };
                direct
                    .add_partition_offset(element.topic(), element.partition(), target)
                    .map_err(broker_error)?;
            }
        }
        if has_timestamp {
            let resolved = self
                .consumer
                .offsets_for_times(timestamps, Timeout::After(self.timeout))
                .map_err(broker_error)?;
            for element in resolved.elements() {
                let offset = match element.offset() {
                    Offset::Offset(value) if value >= 0 => Offset::Offset(value),
                    _ => self.absolute_watermark(
                        &Tp::new(element.topic(), element.partition()),
                        true,
                        resolution_deadline,
                    )?,
                };
                direct
                    .add_partition_offset(element.topic(), element.partition(), offset)
                    .map_err(broker_error)?;
            }
        }
        self.consumer.assign(&direct).map_err(broker_error)?;
        let results = self
            .consumer
            .seek_partitions(direct, Timeout::After(self.timeout))
            .map_err(broker_error)?;
        for element in results.elements() {
            element.error().map_err(broker_error)?;
        }
        Ok(())
    }

    /// 在 fixed assign 启动总 deadline 内把逻辑 Beginning/End 冻结为绝对 offset。
    ///
    /// # 参数
    ///
    /// - `tp`: 目标分区。
    /// - `high`: true 取当前末尾，false 取 retention 后的最早位置。
    /// - `deadline`: 整次多分区起点解析共享的绝对截止点。
    ///
    /// # 错误
    ///
    /// 共享 deadline 耗尽或 broker watermark 查询失败时返回错误。
    fn absolute_watermark(&self, tp: &Tp, high: bool, deadline: Instant) -> Result<Offset> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(NafkaError::Broker(format!(
                "fixed assign 起点解析超时: {}:{}",
                tp.topic, tp.partition
            )));
        }
        let (low, high_value) = self
            .consumer
            .fetch_watermarks(&tp.topic, tp.partition, Timeout::After(remaining))
            .map_err(broker_error)?;
        Ok(Offset::Offset(if high { high_value } else { low }))
    }

    /// poll 一条消息或消费错误。
    ///
    /// # 参数
    ///
    /// - `timeout`: 单次 poll 最长等待。
    ///
    /// # 错误
    ///
    /// broker poll 返回错误时返回公共 broker 错误。
    pub(crate) fn poll(
        &self,
        timeout: Duration,
    ) -> std::result::Result<Option<RdMessage<'_>>, RdPollError> {
        match self.consumer.poll(timeout) {
            Some(Ok(message)) => Ok(Some(RdMessage { inner: message })),
            Some(Err(KafkaError::PartitionEOF(_))) => Ok(None),
            Some(Err(error)) => Err(classify_poll_error(error)),
            None => Ok(None),
        }
    }

    /// 同步批量提交每个分区唯一安全的 next-offset。
    ///
    /// # 参数
    ///
    /// - `values`: 非空分区安全前沿表。
    ///
    /// # 错误
    ///
    /// 列表构造或同步 commit 失败时返回错误。
    pub(crate) fn commit_offsets(&self, values: &BTreeMap<Tp, i64>) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let mut offsets = TopicPartitionList::with_capacity(values.len());
        for (tp, next_offset) in values {
            offsets
                .add_partition_offset(&tp.topic, tp.partition, Offset::Offset(*next_offset))
                .map_err(broker_error)?;
        }
        self.consumer
            .commit(&offsets, CommitMode::Sync)
            .map_err(broker_error)
    }

    /// 主循环取走最近一次实际 rebalance callback 结果。
    pub(crate) fn take_rebalance(&self) -> Option<RdRebalanceEvent> {
        self.consumer.context().take()
    }

    /// 判断 owner 是否应先退出 commit 重试并处理 rebalance。
    pub(crate) fn has_rebalance(&self) -> bool {
        self.consumer.context().has_event()
    }

    /// 更新供 pre-rebalance callback 使用的安全提交快照。
    ///
    /// # 参数
    ///
    /// - `offsets`: owner 当前安全前沿。
    pub(crate) fn set_rebalance_offsets(&self, offsets: &BTreeMap<Tp, i64>) {
        self.consumer.context().set_pending_offsets(offsets);
    }

    /// 主动清空当前 assignment。
    ///
    /// # 错误
    ///
    /// 底层拒绝 unassign 时返回错误。
    pub(crate) fn unassign(&self) -> Result<()> {
        self.consumer.unassign().map_err(broker_error)
    }

    /// 返回当前 assignment 快照。
    ///
    /// # 错误
    ///
    /// 底层查询失败时返回错误。
    pub(crate) fn assignment(&self) -> Result<Vec<Tp>> {
        let list = self.consumer.assignment().map_err(broker_error)?;
        Ok(elements_to_tps(&list))
    }

    /// 查询指定分区 position。
    ///
    /// # 参数
    ///
    /// - `tps`: 目标分区；空列表使用当前 assignment。
    ///
    /// # 错误
    ///
    /// 底层查询失败时返回错误。
    pub(crate) fn position(&self, tps: &[Tp]) -> Result<BTreeMap<Tp, Option<i64>>> {
        let positions = self.consumer.position().map_err(broker_error)?;
        let requested = if tps.is_empty() {
            elements_to_tps(&positions)
        } else {
            tps.to_vec()
        };
        Ok(requested
            .into_iter()
            .map(|tp| {
                let offset =
                    positions
                        .find_partition(&tp.topic, tp.partition)
                        .and_then(|element| match element.offset() {
                            Offset::Offset(value) if value >= 0 => Some(value),
                            _ => None,
                        });
                (tp, offset)
            })
            .collect())
    }

    /// 在 assignment 后显式冻结每个分区的初始消费位置。
    ///
    /// committed offset 优先；没有提交时按配置使用 logical Beginning/End，并通过一次批量
    /// `seek_partitions` 冻结全部位置。该步骤完成前不能发布 broker ready，否则 latest group 可能
    /// 越过 ready 后立即生产的首条消息。
    ///
    /// # 参数
    ///
    /// - `tps`: 本次完整 assignment。
    /// - `auto_offset_reset`: `earliest`、`latest` 或 `none`。
    ///
    /// # 错误
    ///
    /// committed/seek 查询失败，或 none 模式遇到无提交时返回错误。
    pub(crate) fn prime_assignment_positions(
        &self,
        tps: &[Tp],
        auto_offset_reset: &str,
    ) -> Result<()> {
        if tps.is_empty() {
            return Ok(());
        }
        let mut query = TopicPartitionList::with_capacity(tps.len());
        for tp in tps {
            query.add_partition(&tp.topic, tp.partition);
        }
        let committed = self
            .consumer
            .committed_offsets(query, Timeout::After(self.timeout))
            .map_err(broker_error)?;
        let mut targets = TopicPartitionList::with_capacity(tps.len());
        for tp in tps {
            let stored = committed
                .find_partition(&tp.topic, tp.partition)
                .and_then(|element| match element.offset() {
                    Offset::Offset(value) if value >= 0 => Some(value),
                    _ => None,
                });
            let target = match (stored, auto_offset_reset) {
                (Some(value), _) => Offset::Offset(value),
                (None, "earliest") => Offset::Beginning,
                (None, "latest") => Offset::End,
                (None, "none") => {
                    return Err(NafkaError::Broker(format!(
                        "分区 {}:{} 没有 committed offset，且 auto.offset.reset=none",
                        tp.topic, tp.partition
                    )))
                }
                (None, other) => {
                    return Err(NafkaError::Config(format!(
                        "未知 auto_offset_reset: {other}"
                    )))
                }
            };
            targets
                .add_partition_offset(&tp.topic, tp.partition, target)
                .map_err(broker_error)?;
        }
        let results = self
            .consumer
            .seek_partitions(targets, Timeout::After(self.timeout))
            .map_err(broker_error)?;
        for element in results.elements() {
            element.error().map_err(broker_error)?;
        }
        Ok(())
    }

    /// 暂停指定分区；空列表表示当前 assignment。
    ///
    /// # 参数
    ///
    /// - `tps`: 目标分区。
    ///
    /// # 错误
    ///
    /// assignment 查询或底层暂停失败时返回错误。
    pub(crate) fn pause(&self, tps: &[Tp]) -> Result<Vec<Tp>> {
        let targets = self.targets_or_assignment(tps)?;
        self.consumer
            .pause(&tps_to_list(&targets))
            .map_err(broker_error)?;
        Ok(targets)
    }

    /// 恢复指定分区；空列表表示当前 assignment。
    ///
    /// # 参数
    ///
    /// - `tps`: 目标分区。
    ///
    /// # 错误
    ///
    /// assignment 查询或底层恢复失败时返回错误。
    pub(crate) fn resume(&self, tps: &[Tp]) -> Result<Vec<Tp>> {
        let targets = self.targets_or_assignment(tps)?;
        self.consumer
            .resume(&tps_to_list(&targets))
            .map_err(broker_error)?;
        Ok(targets)
    }

    /// seek 一组显式 offsets。
    ///
    /// # 参数
    ///
    /// - `offsets`: 每分区目标 offset。
    ///
    /// # 错误
    ///
    /// 任一 seek 失败时返回错误。
    pub(crate) fn seek_offsets(&self, offsets: &BTreeMap<Tp, i64>) -> Result<()> {
        for (tp, offset) in offsets {
            self.seek_one(tp, Offset::Offset(*offset))?;
        }
        Ok(())
    }

    /// 尽力回退每个分区的位置：单个分区失败不影响其余分区。
    ///
    /// 批级回退表里**故意**包含预期会失败的分区（刚被 revoke 的、已不在 assignment 里的），
    /// 而 `BTreeMap<Tp, _>` 按 `(topic, partition)` 排序遍历。若沿用 fail-fast 语义，
    /// 一个排序靠前的失败分区会取消掉后面所有分区的回退——那些仍被本 owner 持有、
    /// 已 poll 未派发的记录就被跳过并被后续安全前沿提交越过（静默丢失）。
    ///
    /// # 参数
    ///
    /// - `offsets`: 每分区目标 offset。
    ///
    /// # 返回
    ///
    /// 未能回退的分区数；调用方可据此告警，但不应因此中止收尾流程。
    pub(crate) fn rewind_offsets(&self, offsets: &BTreeMap<Tp, i64>) -> Vec<Tp> {
        let mut failed = Vec::new();
        for (tp, offset) in offsets {
            if let Err(error) = self.seek_one(tp, Offset::Offset(*offset)) {
                tracing::debug!(
                    topic = %tp.topic,
                    partition = tp.partition,
                    offset,
                    error = %error,
                    "批级回退未生效"
                );
                failed.push(tp.clone());
            }
        }
        failed
    }

    /// seek 一组分区到指定特殊 offset。
    ///
    /// 只接受 Beginning/End 与绝对 offset。`StartOffset::Committed` 必须走
    /// [`Self::seek_to_committed`]：底层的 `Offset::Stored` 读的是客户端本地 offset store，
    /// 而框架强制 `enable.auto.offset.store=false`，该 store 恒为空。
    ///
    /// # 参数
    ///
    /// - `tps`: 目标分区。
    /// - `offset`: Beginning/End 或绝对 offset。
    ///
    /// # 错误
    ///
    /// 传入 Committed 或任一 seek 失败时返回错误。
    pub(crate) fn seek_special(&self, tps: &[Tp], offset: StartOffset) -> Result<()> {
        if matches!(offset, StartOffset::Committed) {
            return Err(NafkaError::Config(
                "Committed 起点必须经 seek_to_committed 解析为绝对 offset".into(),
            ));
        }
        let native = start_offset(offset);
        for tp in tps {
            self.seek_one(tp, native)?;
        }
        Ok(())
    }

    /// 把一组分区定位回 broker 已提交位点。
    ///
    /// 为什么不能直接 seek 到底层的 `Offset::Stored`：框架把
    /// `enable.auto.offset.store=false` 定为不可覆盖的不变量，
    /// 框架自身也从不写入本地 offset store，因此该 store 恒为空，seek 会直接返回
    /// `Invalid argument`——即该路径在任何配置下都不可能成功。这里改为与 fixed assign
    /// 起点解析（[`Self::assign`]）同一套做法：先读 broker 的 committed offset，
    /// 再 seek 到解析出的绝对位置。
    ///
    /// # 参数
    ///
    /// - `tps`: 目标分区；空列表直接成功。
    /// - `auto_offset_reset`: 分区没有提交历史时的回退策略（`earliest`/`latest`/`none`）。
    ///
    /// # 错误
    ///
    /// committed 查询失败、`none` 模式遇到无提交，或任一 seek 失败时返回错误。
    pub(crate) fn seek_to_committed(&self, tps: &[Tp], auto_offset_reset: &str) -> Result<()> {
        if tps.is_empty() {
            return Ok(());
        }
        let mut query = TopicPartitionList::with_capacity(tps.len());
        for tp in tps {
            query.add_partition(&tp.topic, tp.partition);
        }
        let committed = self
            .consumer
            .committed_offsets(query, Timeout::After(self.timeout))
            .map_err(broker_error)?;
        for tp in tps {
            let stored = committed
                .find_partition(&tp.topic, tp.partition)
                .and_then(|element| match element.offset() {
                    Offset::Offset(value) if value >= 0 => Some(value),
                    _ => None,
                });
            let target = match (stored, auto_offset_reset) {
                (Some(value), _) => Offset::Offset(value),
                (None, "earliest") => Offset::Beginning,
                (None, "latest") => Offset::End,
                (None, "none") => {
                    return Err(NafkaError::Broker(format!(
                        "分区 {}:{} 没有 committed offset，且 auto.offset.reset=none",
                        tp.topic, tp.partition
                    )))
                }
                (None, other) => {
                    return Err(NafkaError::Config(format!(
                        "未知 auto_offset_reset: {other}"
                    )))
                }
            };
            self.seek_one(tp, target)?;
        }
        Ok(())
    }

    /// 按时间戳解析并 seek 一组分区。
    ///
    /// # 参数
    ///
    /// - `times`: 每分区目标时间戳。
    ///
    /// # 错误
    ///
    /// broker 查询或任一 seek 失败时返回错误。
    pub(crate) fn seek_times(&self, times: &BTreeMap<Tp, i64>) -> Result<()> {
        let mut query = TopicPartitionList::with_capacity(times.len());
        for (tp, timestamp) in times {
            query
                .add_partition_offset(&tp.topic, tp.partition, Offset::Offset(*timestamp))
                .map_err(broker_error)?;
        }
        let resolved = self
            .consumer
            .offsets_for_times(query, Timeout::After(self.timeout))
            .map_err(broker_error)?;
        for element in resolved.elements() {
            let tp = Tp::new(element.topic(), element.partition());
            let offset = match element.offset() {
                Offset::Offset(value) if value >= 0 => Offset::Offset(value),
                _ => Offset::End,
            };
            self.seek_one(&tp, offset)?;
        }
        Ok(())
    }

    /// 返回底层是否判定当前 assignment 已丢失。
    pub(crate) fn assignment_lost(&self) -> bool {
        self.consumer.assignment_lost()
    }

    /// 将空目标列表替换为当前 assignment。
    ///
    /// # 参数
    ///
    /// - `tps`: 调用方目标列表。
    ///
    /// # 错误
    ///
    /// assignment 查询失败时返回错误。
    fn targets_or_assignment(&self, tps: &[Tp]) -> Result<Vec<Tp>> {
        if tps.is_empty() {
            self.assignment()
        } else {
            Ok(tps.to_vec())
        }
    }

    /// seek 单分区。
    ///
    /// # 参数
    ///
    /// - `tp`: 目标分区。
    /// - `offset`: 底层 offset 表达。
    ///
    /// # 错误
    ///
    /// 底层 seek 失败时返回错误。
    fn seek_one(&self, tp: &Tp, offset: Offset) -> Result<()> {
        self.consumer
            .seek(
                &tp.topic,
                tp.partition,
                offset,
                Timeout::After(self.timeout),
            )
            .map_err(broker_error)
    }
}

/// 把公开起始策略映射为底层 offset 表达。
///
/// `Committed` 不在此映射：它需要一次 broker 查询才能变成绝对 offset，
/// 由 [`ConsumerClient::seek_to_committed`] 负责；这里返回 Beginning 只是为了让
/// match 完整，调用方在进入本函数前已经拒绝该分支。
///
/// # 参数
///
/// - `offset`: 非时间戳、非 Committed 的起始策略。
fn start_offset(offset: StartOffset) -> Offset {
    match offset {
        StartOffset::Earliest | StartOffset::Committed => Offset::Beginning,
        StartOffset::Latest => Offset::End,
        StartOffset::Timestamp(value) | StartOffset::Offset(value) => Offset::Offset(value),
    }
}

/// 从 owner 安全快照中只选取本次实际被收回的分区。
///
/// # 参数
///
/// - `revoked`: callback 提供的实际 revoke 分区。
/// - `snapshot`: owner 最近发布的安全 next-offset。
/// - `assignment_lost`: 底层是否已判定 assignment 丢失。
fn select_revoke_offsets(
    revoked: &[Tp],
    snapshot: &BTreeMap<Tp, i64>,
    assignment_lost: bool,
) -> BTreeMap<Tp, i64> {
    if assignment_lost {
        return BTreeMap::new();
    }
    revoked
        .iter()
        .filter_map(|tp| snapshot.get(tp).map(|offset| (tp.clone(), *offset)))
        .collect()
}

/// 在独立线程执行可注入工作，并把 callback 等待严格限制在给定时长内。
///
/// # 参数
///
/// - `timeout`: 调用线程允许等待的硬上限。
/// - `work`: 后台执行的提交工作。
fn wait_bounded_commit<F>(timeout: Duration, work: F) -> BoundedCommitOutcome
where
    F: FnOnce() -> Result<()> + Send + 'static,
{
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let spawned = std::thread::Builder::new()
        .name("nafka-revoke-commit".into())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work))
                .map_err(|_| "revoke commit 任务 panic".to_owned())
                .and_then(|result| result.map_err(|error| error.to_string()));
            let _ = sender.send(result);
        });
    if let Err(error) = spawned {
        return BoundedCommitOutcome::Failed(format!("无法启动 revoke commit 任务: {error}"));
    }
    match receiver.recv_timeout(timeout) {
        Ok(Ok(())) => BoundedCommitOutcome::Committed,
        Ok(Err(error)) => BoundedCommitOutcome::Failed(error),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => BoundedCommitOutcome::TimedOut,
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            BoundedCommitOutcome::Failed("revoke commit 任务异常退出".into())
        }
    }
}

/// 使用单飞后台任务在硬截止时间内提交 revoke 安全前沿。
///
/// 高层同步提交在协调器不可用时可能等待完整 session timeout，无法满足 callback 的硬边界；这里让
/// 后台任务持有 owner consumer 生命周期，而 callback 只等待调用方预算。超时后的晚到成功不再被本地
/// 当成已确认；即使 broker 最终接受，它也只包含安全前沿，因此最坏结果仍是重放而不是丢消息。
///
/// # 参数
///
/// - `consumer`: 当前 generation 的底层 consumer。
/// - `offsets`: 被 revoke 分区的安全 next-offset。
/// - `timeout`: callback 允许等待的硬上限。
fn commit_offsets_bounded(
    consumer: &BaseConsumer<OwnerConsumerContext>,
    offsets: &BTreeMap<Tp, i64>,
    timeout: Duration,
) -> BoundedCommitOutcome {
    let context = consumer.context();
    if context
        .pre_commit_in_flight
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return BoundedCommitOutcome::Failed("已有 revoke commit 在后台收尾".into());
    }
    let Some(owner) = context
        .owner_consumer
        .get()
        .and_then(std::sync::Weak::upgrade)
    else {
        context.pre_commit_in_flight.store(false, Ordering::Release);
        return BoundedCommitOutcome::Failed("owner consumer 生命周期引用尚未就绪".into());
    };
    let owned_offsets = offsets.clone();
    let outcome = wait_bounded_commit(timeout, move || {
        let _guard = PreCommitGuard {
            owner: Arc::clone(&owner),
        };
        offsets_to_list(&owned_offsets)
            .and_then(|list| owner.commit(&list, CommitMode::Sync).map_err(broker_error))
    });
    if outcome != BoundedCommitOutcome::TimedOut {
        context.pre_commit_in_flight.store(false, Ordering::Release);
    }
    outcome
}

/// 把自有分区列表转换为底层列表。
///
/// # 参数
///
/// - `tps`: 自有分区列表。
fn tps_to_list(tps: &[Tp]) -> TopicPartitionList {
    let mut list = TopicPartitionList::with_capacity(tps.len());
    for tp in tps {
        list.add_partition(&tp.topic, tp.partition);
    }
    list
}

/// 把安全 offset 表转换为底层提交列表。
///
/// # 参数
///
/// - `offsets`: 每分区 next-offset。
///
/// # 错误
///
/// 任一 offset 无法写入底层列表时返回 broker 错误。
fn offsets_to_list(offsets: &BTreeMap<Tp, i64>) -> Result<TopicPartitionList> {
    let mut list = TopicPartitionList::with_capacity(offsets.len());
    for (tp, offset) in offsets {
        list.add_partition_offset(&tp.topic, tp.partition, Offset::Offset(*offset))
            .map_err(broker_error)?;
    }
    Ok(list)
}

/// 把底层列表转换为稳定排序自有分区列表。
///
/// # 参数
///
/// - `list`: 底层分区列表。
fn elements_to_tps(list: &TopicPartitionList) -> Vec<Tp> {
    let mut tps: Vec<Tp> = list
        .elements()
        .into_iter()
        .map(|element| Tp::new(element.topic(), element.partition()))
        .collect();
    tps.sort();
    tps
}

/// 把底层 consumer 错误收敛为公共 broker 错误。
///
/// # 参数
///
/// - `error`: 底层错误。
fn broker_error(error: impl std::fmt::Display) -> NafkaError {
    NafkaError::Broker(error.to_string())
}

/// 集中把底层 poll 错误分成 fatal 或 transient，禁止调用点自行猜测。
///
/// # 参数
///
/// - `error`: 底层 poll 返回的具体错误。
fn classify_poll_error(error: KafkaError) -> RdPollError {
    let fatal = matches!(
        error,
        KafkaError::MessageConsumptionFatal(_) | KafkaError::ClientConfig(..) | KafkaError::Nul(_)
    ) || error.rdkafka_error_code().is_some_and(is_fatal_code);
    if fatal {
        RdPollError::Fatal(error.to_string())
    } else {
        RdPollError::Transient(error.to_string())
    }
}

/// 判断底层错误码是否要求永久停止当前 group。
///
/// # 参数
///
/// - `code`: 底层稳定错误码。
fn is_fatal_code(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::InvalidArgument
            | RDKafkaErrorCode::Authentication
            | RDKafkaErrorCode::Fatal
            | RDKafkaErrorCode::Fenced
            | RDKafkaErrorCode::UnsupportedFeature
            | RDKafkaErrorCode::TopicAuthorizationFailed
            | RDKafkaErrorCode::GroupAuthorizationFailed
            | RDKafkaErrorCode::ClusterAuthorizationFailed
            | RDKafkaErrorCode::UnsupportedSASLMechanism
            | RDKafkaErrorCode::SaslAuthenticationFailed
            | RDKafkaErrorCode::DelegationTokenAuthorizationFailed
            | RDKafkaErrorCode::FencedInstanceId
            | RDKafkaErrorCode::ProducerFenced
            | RDKafkaErrorCode::FencedMemberEpoch
            | RDKafkaErrorCode::UnsupportedAssignor
    )
}
