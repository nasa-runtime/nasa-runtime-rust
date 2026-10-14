//! 原生事务隔离层：consumer 只在 poll 线程使用，producer 只执行一个串行批次。

use crate::transaction::{context::header_bytes, runtime::Task};
use crate::{
    KafkaConfig, KafkaHeader, KafkaHeaders, ReplayRecord, Tp, TransactionContext,
    TransactionError as Error, TransactionPlan, TransactionResult as Result,
    TransactionState as State,
};
use rdkafka::{
    consumer::{BaseConsumer, Consumer, ConsumerContext, ConsumerGroupMetadata, Rebalance},
    error::{KafkaError, RDKafkaErrorCode as Code},
    message::{Header, Headers, OwnedHeaders},
    producer::{FutureProducer, FutureRecord, Producer},
    util::Timeout,
    ClientConfig, ClientContext, Message, Offset, TopicPartitionList,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    thread::JoinHandle,
    time::{Duration, Instant},
};

struct Context {
    task: Arc<Task>,
}
impl ClientContext for Context {}
impl ConsumerContext for Context {
    /// 业务作用：在原生撤回执行前撤销整个旧事务批次，禁止普通 offset commit。
    /// 参数说明：consumer 为真实 owner，rebalance 为协调器通知。返回：只修改当前事务权威，不等待业务线程。
    fn pre_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Revoke(partitions) = rebalance {
            self.task.assignment_changed(tps(partitions), false);
        }
    }
    /// 业务作用：原生 assignment 完成后发布新代次，并在旧批次仍存活时暂停新分区。
    /// 参数说明：consumer 为当前 owner，rebalance 为实际通知。返回：不复用旧组 metadata，不提交旧批次位点。
    fn post_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(partitions) => {
                self.task.assignment_changed(tps(partitions), true);
                if !matches!(self.task.status().state, State::Idle | State::Starting) {
                    if let Ok(assignment) = consumer.assignment() {
                        let _ = consumer.pause(&assignment);
                    }
                }
            }
            Rebalance::Error(error) => self.task.fail(classify(error)),
            Rebalance::Revoke(_) => {}
        }
    }
}

/// 业务作用：建连前拒绝显式隔离级别冲突并验证基础原生配置。
/// 参数说明：config 为命名源配置，plan 为事务合同。返回：事务可构造；非法组或配置映射为 InvalidConfiguration，隔离冲突为 IsolationMismatch，不创建 producer 或取得身份。
pub(crate) fn validate(config: &KafkaConfig, plan: &TransactionPlan) -> Result<()> {
    configs(config, plan).map(|_| ())
}

/// 业务作用：为专属事务固定 read_committed、幂等 producer 与有限原生队列。
/// 参数说明：config 为源的连接与安全配置，plan 为原子合同。返回：隔离的消费、发布配置；普通 lane 不受改写。
fn configs(config: &KafkaConfig, plan: &TransactionPlan) -> Result<(ClientConfig, ClientConfig)> {
    // 原生插件可在框架之外改写输入或发送，不能让冻结白名单和整批位点责任失去依据。
    if [
        &config.properties,
        &config.consumer.properties,
        &config.producer.properties,
    ]
    .iter()
    .any(|properties| {
        properties
            .get("plugin.library.paths")
            .is_some_and(|value| !value.trim().is_empty())
    }) {
        return Err(Error::InvalidConfiguration);
    }
    // 原生配置边界独立复验组身份，并把普通配置失败转换为稳定的事务错误，禁止泄露另一个执行域的错误类型。
    let mut consumer = super::config::consumer_config(config, &plan.group)
        .map_err(|_| Error::InvalidConfiguration)?;
    if consumer
        .get("isolation.level")
        .is_some_and(|value| value != "read_committed")
    {
        return Err(Error::IsolationMismatch);
    }
    consumer
        .set("isolation.level", "read_committed")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("allow.auto.create.topics", "false")
        .set("queued.min.messages", "1")
        .set(
            "queued.max.messages.kbytes",
            (plan.limits.max_batch_bytes / 1024).max(1).to_string(),
        )
        .set(
            "fetch.message.max.bytes",
            plan.limits.max_batch_bytes.to_string(),
        );
    let mut producer = super::config::producer_config(config, "default")
        .map_err(|_| Error::InvalidConfiguration)?;
    producer
        .set("transactional.id", plan.identity.transactional_id()?)
        .set(
            "transaction.timeout.ms",
            plan.limits.transaction_timeout_ms.to_string(),
        )
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("retries", "2147483647")
        .set("max.in.flight.requests.per.connection", "1")
        .set("linger.ms", "0")
        .set(
            "request.timeout.ms",
            plan.limits.operation_timeout_ms.to_string(),
        )
        .set(
            "delivery.timeout.ms",
            plan.limits.transaction_timeout_ms.to_string(),
        )
        .set(
            "queue.buffering.max.messages",
            plan.limits.max_output_records.to_string(),
        )
        .set(
            "queue.buffering.max.kbytes",
            (plan.limits.max_output_bytes / 1024).max(1).to_string(),
        );
    Ok((consumer, producer))
}

/// 业务作用：维持组成员资格和失权观测，同时以唯一批次线程承担阻塞业务及事务 I/O。
/// 参数说明：task 为已经登记身份的真实任务。返回：批次线程与原生资源全部退出后才返回，未知事务结果保留为失败。
pub(crate) fn run(task: &Arc<Task>) -> Result<()> {
    let (consumer_config, producer_config) = configs(&task.config, &task.plan)?;
    let budget = operation(task);
    let mut producer = Some(match initialize_producer(task, &producer_config) {
        Ok(producer) => producer,
        Err(Error::Cancelled) => return Ok(()),
        Err(error) => return Err(error),
    });
    if task.stopping() {
        return Ok(());
    }
    let consumer: BaseConsumer<Context> = consumer_config
        .create_with_context(Context {
            task: Arc::clone(task),
        })
        .map_err(|e| classify(&e))?;
    let topics: Vec<_> = task.plan.input_topics.iter().map(String::as_str).collect();
    consumer.subscribe(&topics).map_err(|e| classify(&e))?;
    task.initialized();
    let mut worker = Worker {
        task: Arc::clone(task),
        thread: None,
        epoch: 0,
        starts: BTreeMap::new(),
    };
    let mut attempt = 1;
    let mut retry_after = Instant::now();
    loop {
        if worker.thread.as_ref().is_some_and(JoinHandle::is_finished) {
            let outcome = worker
                .thread
                .take()
                .expect("finished worker")
                .join()
                .unwrap_or(Err(Error::HandlerFailed));
            let mut retry = false;
            let mut rebuild_producer = false;
            match outcome {
                Ok(Outcome::ProducerRecovered(recovered)) => {
                    producer = Some(recovered);
                    task.initialized();
                }
                Ok(Outcome::Committed) => attempt = 1,
                Ok(Outcome::Aborted(reason)) if !task.stopping() => {
                    if attempt > task.plan.limits.max_aborted_retries {
                        task.fail(reason);
                    } else {
                        attempt += 1;
                        retry = true;
                        rebuild_producer = task.status().assignment_epoch != worker.epoch
                            || matches!(
                                reason,
                                Error::Rebalanced | Error::TransactionTimeout | Error::BrokerError
                            );
                        if task.status().assignment_epoch == worker.epoch {
                            // 只有同一分配权威仍在时才回到本地起点；新代次必须重新读取 broker 的组位置。
                            // 全部参与分区共享一次重定位预算，分区数量不能延长暂停和组失联窗口。
                            let rewind_deadline = Instant::now() + budget;
                            for (tp, offset) in &worker.starts {
                                consumer
                                    .seek(
                                        &tp.topic,
                                        tp.partition,
                                        Offset::Offset(*offset),
                                        remaining(task, rewind_deadline)?,
                                    )
                                    .map_err(|e| classify(&e))?;
                            }
                        } else {
                            consumer.unsubscribe();
                            consumer.subscribe(&topics).map_err(|e| classify(&e))?;
                        }
                        retry_after = Instant::now()
                            + Duration::from_millis(task.plan.limits.retry_backoff_ms.into());
                    }
                }
                Ok(_) => {}
                Err(Error::Cancelled) if task.stopping() => {}
                Err(error) => task.fail(error),
            }
            task.settled(retry);
            if rebuild_producer {
                // 中止已确认且旧批次已 join，才能销毁其原生 producer；稳定身份仍由本任务持有。
                // 协调器变化后重新初始化，避免把旧原生组协调状态带入下一次位点登记。
                task.recovering_producer();
                let previous = producer.take().ok_or(Error::BrokerError)?;
                let recovery_task = Arc::clone(task);
                let config = producer_config.clone();
                worker.thread = Some(
                    std::thread::Builder::new()
                        .name("nafka-transaction-recovery".into())
                        .spawn(move || {
                            // 原生析构和新初始化都在责任线程完成，poll owner 持续接收失权与心跳通知。
                            drop(previous);
                            if recovery_task.stopping() {
                                return Ok(Outcome::Cancelled);
                            }
                            initialize_producer(&recovery_task, &config)
                                .map(Outcome::ProducerRecovered)
                        })
                        .map_err(|_| Error::Capacity)?,
                );
            }
        }
        task.expire();
        if task.stopping() && worker.thread.is_none() {
            return Ok(());
        }
        let paused = worker.thread.is_some() || task.stopping() || Instant::now() < retry_after;
        let assignment = consumer.assignment().map_err(|e| classify(&e))?;
        if assignment.count() > 0 {
            if paused {
                consumer.pause(&assignment)
            } else {
                consumer.resume(&assignment)
            }
            .map_err(|e| classify(&e))?;
        }
        let Some(message) = consumer.poll(Duration::from_millis(50)) else {
            continue;
        };
        let message = match message {
            Ok(message) => message,
            // 临时连接通知不是组失权证明；持续 poll 接收真正的撤回，不能掩盖在途事务的最终裁决。
            Err(error) if transient_consumer_error(&error) => continue,
            Err(error) => return Err(classify(&error)),
        };
        if paused {
            let tp = Tp::new(message.topic(), message.partition());
            let offset = message.offset();
            drop(message);
            if task.status().assignment.contains(&tp) {
                consumer
                    .seek(&tp.topic, tp.partition, Offset::Offset(offset), budget)
                    .map_err(|e| classify(&e))?;
            }
            continue;
        }
        let collecting_epoch = task.status().assignment_epoch;
        let mut records = vec![owned(&message, &task.plan.cluster_id)];
        drop(message);
        let mut bytes = input_bytes(&records[0]);
        while records.len() < task.plan.limits.max_batch_records as usize {
            let Some(next) = consumer.poll(Duration::ZERO) else {
                break;
            };
            let next = match next {
                Ok(message) => message,
                // 本轮已经取得的输入仍受同一代次和期限约束，网络通知只结束当前收集窗口。
                Err(error) if transient_consumer_error(&error) => break,
                Err(error) => return Err(classify(&error)),
            };
            let record = owned(&next, &task.plan.cluster_id);
            bytes += input_bytes(&record);
            records.push(record);
            if bytes > task.plan.limits.max_batch_bytes {
                break;
            }
        }
        // 批次收集期间发生重平衡不能把不同代次的输入拼接；重新订阅由组协调器恢复位置。
        if collecting_epoch != task.status().assignment_epoch || consumer.assignment_lost() {
            consumer.unsubscribe();
            consumer.subscribe(&topics).map_err(|e| classify(&e))?;
            continue;
        }
        let mut starts = BTreeMap::new();
        let mut ends = BTreeMap::new();
        for record in &records {
            let tp = Tp::new(&record.topic, record.partition);
            starts
                .entry(tp.clone())
                .and_modify(|o: &mut i64| *o = (*o).min(record.offset))
                .or_insert(record.offset);
            ends.entry(tp)
                .and_modify(|o: &mut i64| *o = (*o).max(record.offset + 1))
                .or_insert(record.offset + 1);
        }
        let (epoch, deadline) = task.begin(starts.clone(), ends.clone())?;
        if bytes > task.plan.limits.max_batch_bytes
            || records
                .iter()
                .any(|r| r.headers.len() > 256 || header_bytes(&r.headers) > 65536)
        {
            return Err(Error::Capacity);
        }
        let metadata = consumer.group_metadata().ok_or(Error::Rebalanced)?;
        consumer
            .pause(&consumer.assignment().map_err(|e| classify(&e))?)
            .map_err(|e| classify(&e))?;
        worker.epoch = epoch;
        worker.starts = starts;
        let batch_task = Arc::clone(task);
        let batch_producer = Arc::clone(producer.as_ref().ok_or(Error::BrokerError)?);
        worker.thread = Some(
            std::thread::Builder::new()
                .name("nafka-transaction-batch".into())
                .spawn(move || {
                    execute(
                        &batch_task,
                        &batch_producer,
                        records,
                        ends,
                        metadata,
                        epoch,
                        deadline,
                        attempt,
                    )
                })
                .map_err(|_| Error::Capacity)?,
        );
    }
}

enum Outcome {
    ProducerRecovered(Arc<FutureProducer>),
    Committed,
    Aborted(Error),
    Cancelled,
}

/// 业务作用：为稳定身份取得新的专属原生 producer，并在 fencing 前核对实际集群。
/// 参数说明：task 为仍持有身份的任务，config 为冻结的原生配置。返回：集群一致且初始化成功才交付资源，取消或失败时关闭本次实例。
fn initialize_producer(task: &Task, config: &ClientConfig) -> Result<Arc<FutureProducer>> {
    let producer: Arc<FutureProducer> = Arc::new(
        config
            .create()
            .map_err(|e| initialization_error("create", &e))?,
    );
    let budget = operation(task);
    // 地址不是物理身份；新连接必须先复验集群，不能对错误集群执行稳定身份的 fencing。
    let cluster = producer
        .client()
        .fetch_cluster_id(Timeout::After(budget))
        .ok_or_else(|| {
            tracing::warn!(
                stage = "cluster_identity",
                "Kafka 事务初始化未取得物理集群证据"
            );
            Error::BrokerError
        })?;
    if cluster != task.plan.cluster_id {
        return Err(Error::ClusterMismatch);
    }
    if task.stopping() {
        return Err(Error::Cancelled);
    }
    producer
        .init_transactions(budget)
        .map_err(|e| initialization_error("init_transactions", &e))?;
    Ok(producer)
}

/// 业务作用：保留事务启动失败的阶段与固定原生码，便于区分身份探测、认证和协调器故障。
/// 参数说明：stage 为静态阶段名，error 为原生错误。返回：稳定公共错误类别，日志不记录配置或自由错误文本。
fn initialization_error(stage: &'static str, error: &KafkaError) -> Error {
    tracing::warn!(stage, code = ?error.rdkafka_error_code(), "Kafka 事务初始化失败");
    classify(error)
}
struct Worker {
    task: Arc<Task>,
    thread: Option<JoinHandle<Result<Outcome>>>,
    epoch: u64,
    starts: BTreeMap<Tp, i64>,
}
impl Drop for Worker {
    /// 业务作用：poll 线程提前失败时仍承担已经开始的回调和原生事务责任。
    /// 参数说明：无。返回：撤销准入并真实 join 后才能允许外层释放 producer 和稳定身份。
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            self.task.cancel();
            // 消费观察失败不能覆盖原子提交或中止的未知结果；先保存批次真实裁决再归还原生资源。
            if let Err(error) = thread.join().unwrap_or(Err(Error::HandlerFailed)) {
                self.task.fail(error);
            }
        }
    }
}

/// 业务作用：组织唯一事务的全部输出与输入位点，提交等待失败后禁止切换为 abort。
/// 参数说明：task/producer 为实际 owner，records/ends 为完整输入，metadata/epoch 为冻结权威，deadline 为整批期限，attempt 为确认中止后的序号。返回：只有原生 commit 或 abort 已确认才返回对应结局。
#[allow(clippy::too_many_arguments)]
fn execute(
    task: &Arc<Task>,
    producer: &FutureProducer,
    records: Vec<ReplayRecord>,
    ends: BTreeMap<Tp, i64>,
    metadata: ConsumerGroupMetadata,
    epoch: u64,
    deadline: Instant,
    attempt: u32,
) -> Result<Outcome> {
    let mut begun = false;
    let mut committing = false;
    let result = (|| {
        task.check(epoch, None)?;
        producer.begin_transaction().map_err(|e| classify(&e))?;
        begun = true;
        let input_count = records.len();
        let authority = Arc::clone(task);
        let mut context = TransactionContext::new(
            Arc::clone(&task.plan),
            records,
            Box::new(move || authority.check(epoch, None)),
            attempt,
        );
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            task.processor.process(&mut context)
        }))
        .map_err(|_| Error::HandlerFailed)??;
        let outputs = context.seal()?;
        task.check(epoch, Some(State::Sending))?;
        if !context.dlt_topics().is_empty() {
            let admin = task.dlt_admin()?;
            for topic in context.dlt_topics() {
                task.check(epoch, None)?;
                // 事务原子性不能阻止后续压缩；全部DLT目标先验证，再发出本批任何输出。
                let target = task
                    .runtime
                    .block_on(admin.describe_topic(topic.clone(), true))
                    .map_err(|error| {
                        if matches!(error, crate::NafkaError::Authorization) {
                            Error::Authorization
                        } else {
                            Error::BrokerError
                        }
                    })?;
                crate::consumer::dlt::validate_retention(&target.configs)
                    .map_err(|_| Error::InvalidConfiguration)?;
            }
        }
        let output_count = outputs.len();
        for output in outputs {
            task.check(epoch, None)?;
            let mut headers = OwnedHeaders::new_with_capacity(output.headers.len());
            for header in output.headers.iter() {
                headers = headers.insert(Header {
                    key: &header.name,
                    value: header.value.as_deref(),
                });
            }
            let mut record = FutureRecord::to(&output.destination.topic)
                .partition(output.destination.partition)
                .headers(headers);
            if let Some(key) = &output.key {
                record = record.key(key);
            }
            if let Some(payload) = &output.payload {
                record = record.payload(payload);
            }
            if let Some(timestamp) = output.timestamp {
                record = record.timestamp(timestamp);
            }
            // 交付回调仍只是事务内部证据，不能提前发布最终输出成功或输入水位。
            let delivery = producer
                .send_result(record)
                .map_err(|(error, _)| classify_delivery(producer, &error))?;
            task.runtime.block_on(async {
                tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), delivery)
                    .await
                    .map_err(|_| Error::TransactionTimeout)?
                    .map_err(|_| Error::BrokerError)?
                    .map_err(|(error, _)| classify_delivery(producer, &error))
                    .map(|_| ())
            })?;
        }
        task.check(epoch, Some(State::Preparing))?;
        let mut offsets = TopicPartitionList::new();
        for (tp, offset) in ends {
            offsets
                .add_partition_offset(&tp.topic, tp.partition, Offset::Offset(offset))
                .map_err(|e| classify(&e))?;
        }
        producer
            .send_offsets_to_transaction(&offsets, &metadata, remaining(task, deadline)?)
            .map_err(|e| classify(&e))?;
        // 与失权共用的控制门只覆盖提交准入；原生提交期间不占锁，取消只能等待真实结果。
        task.check(epoch, Some(State::Committing))?;
        committing = true;
        producer
            .commit_transaction(remaining(task, deadline)?)
            .map_err(|e| classify(&e))?;
        task.committed(input_count, output_count);
        Ok(Outcome::Committed)
    })();
    match result {
        Ok(outcome) => Ok(outcome),
        Err(error) if committing => Err(if matches!(error, Error::Fenced | Error::Authorization) {
            error
        } else {
            Error::CommitUnknown
        }),
        Err(error) if begun && !matches!(error, Error::Fenced | Error::Authorization) => {
            task.aborting();
            match producer.abort_transaction(operation(task)) {
                Ok(()) => {
                    task.aborted();
                    Ok(Outcome::Aborted(error))
                }
                Err(native) => {
                    let error = classify(&native);
                    Err(if matches!(error, Error::Fenced | Error::Authorization) {
                        error
                    } else {
                        Error::AbortUnknown
                    })
                }
            }
        }
        Err(Error::Cancelled) => Ok(Outcome::Cancelled),
        Err(error) => Err(error),
    }
}

/// 业务作用：保留真实原文输入，防止原生 poll 缓冲回收改变本批确认依据。
/// 参数说明：message 为本轮借用记录，cluster 为已核对物理集群。返回：独立记录，null、空值和重复头保持区别。
fn owned(message: &rdkafka::message::BorrowedMessage<'_>, cluster: &str) -> ReplayRecord {
    let headers = message.headers().map_or_else(Vec::new, |headers| {
        (0..headers.count())
            .map(|index| {
                let header = headers.get(index);
                KafkaHeader {
                    name: header.key.into(),
                    value: header.value.map(ToOwned::to_owned),
                }
            })
            .collect()
    });
    ReplayRecord {
        cluster_id: cluster.into(),
        topic: message.topic().into(),
        partition: message.partition(),
        offset: message.offset(),
        timestamp: message.timestamp().to_millis().unwrap_or(-1),
        key: message.key().map(ToOwned::to_owned),
        payload: message.payload().map(ToOwned::to_owned),
        headers: KafkaHeaders::from_vec(headers),
    }
}

/// 业务作用：为独立输入按共同字节口径计费。
/// 参数说明：record 为完整原文。返回：key、payload、头以及64字节固定责任开销之和。
fn input_bytes(record: &ReplayRecord) -> u64 {
    64 + record.key.as_ref().map_or(0, |v| v.len() as u64)
        + record.payload.as_ref().map_or(0, |v| v.len() as u64)
        + header_bytes(&record.headers)
}

/// 业务作用：将原生分配集合转换成产品自有定位类型。
/// 参数说明：partitions 为协调器返回集合。返回：有序且不重复的 topic/partition。
fn tps(partitions: &TopicPartitionList) -> BTreeSet<Tp> {
    partitions
        .elements()
        .iter()
        .map(|p| Tp::new(p.topic(), p.partition()))
        .collect()
}

/// 业务作用：获取当前 owner 声明的原生操作预算。
/// 参数说明：task 为冻结事务合同。返回：毫秒预算，不为每条记录延长整批截止时间。
fn operation(task: &Task) -> Duration {
    Duration::from_millis(task.plan.limits.operation_timeout_ms.into())
}

/// 业务作用：把原生操作限制在整批剩余准备时间内。
/// 参数说明：task 为操作上限，deadline 为本批截止。返回：正的最小剩余预算，耗尽拒绝新副作用。
fn remaining(task: &Task, deadline: Instant) -> Result<Duration> {
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        return Err(Error::TransactionTimeout);
    }
    Ok(left.min(operation(task)))
}

/// 业务作用：从事务清队列通知中取回原生已保存的权限或 fencing 原因，避免错误的中止掩盖拒绝证据。
/// 参数说明：producer 为本批次独占且已 begin、尚未 commit/abort 的客户端；error 为输出回执或同步入队失败。
/// 返回：原生保存的授权或失权类别；没有此类证据时保留原始失败，不推断远端是否提交。
fn classify_delivery(producer: &FutureProducer, error: &KafkaError) -> Error {
    if matches!(
        error.rdkafka_error_code(),
        Some(Code::PurgeQueue | Code::PurgeInflight)
    ) {
        // 本线程独占事务 API，已 begin 且尚无结束调用，原生不可能处于可开始新事务的 Ready。
        // 重复 begin 在活动/abortable/fatal 状态只返回状态证据，不发送 EndTxn；后者会覆盖最初的授权拒绝。
        if let Err(reason) = producer.begin_transaction() {
            let classified = classify(&reason);
            if matches!(classified, Error::Authorization | Error::Fenced) {
                return classified;
            }
        }
    }
    classify(error)
}

/// 业务作用：区分可由原生客户端重连的消费通知与必须终止 owner 的错误。
/// 参数说明：error 为 poll 返回的原生事件。返回：仅非致命的连接或请求超时通知可继续 poll，授权和真实失权仍由各自路径处理。
fn transient_consumer_error(error: &KafkaError) -> bool {
    matches!(
        error,
        KafkaError::MessageConsumption(
            Code::BrokerTransportFailure
                | Code::Resolve
                | Code::AllBrokersDown
                | Code::NetworkException
                | Code::OperationTimedOut
                | Code::RequestTimedOut
        )
    )
}

/// 业务作用：从原生错误码选择固定恢复类别，不将底层凭据或任意文本带入产品状态。
/// 参数说明：error 为当前阶段原生错误。返回：fencing、授权、重平衡、超时或通用 broker 类别。
fn classify(error: &KafkaError) -> Error {
    match error.rdkafka_error_code() {
        Some(
            Code::Fenced
            | Code::ProducerFenced
            | Code::InvalidProducerEpoch
            | Code::FencedInstanceId,
        ) => Error::Fenced,
        Some(
            Code::Authentication
            | Code::SaslAuthenticationFailed
            | Code::TopicAuthorizationFailed
            | Code::GroupAuthorizationFailed
            | Code::ClusterAuthorizationFailed
            | Code::TransactionalIdAuthorizationFailed,
        ) => Error::Authorization,
        Some(
            Code::IllegalGeneration
            | Code::UnknownMemberId
            | Code::RebalanceInProgress
            | Code::PollExceeded,
        ) => Error::Rebalanced,
        Some(Code::OperationTimedOut | Code::MessageTimedOut | Code::RequestTimedOut) => {
            Error::TransactionTimeout
        }
        _ => Error::BrokerError,
    }
}
