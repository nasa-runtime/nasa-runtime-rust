//! 单分区分配决定 producer 初始化；备用实例持续 poll，但不取得稳定事务身份的远端代次。

use super::*;

/// 业务作用：让独占单分区分配、producer fencing 和事务实际责任按顺序衔接。
///
/// 参数说明：
/// - `task`：已登记本地事务身份和组责任的任务。
///
/// 返回：取消后排干资源；失权撤销未提交批次，未知提交、未知中止或 fencing 保留为失败。
pub(super) fn run(task: &Arc<Task>) -> Result<()> {
    let (consumer_config, producer_config) = configs(&task.config, &task.plan)?;
    let consumer: BaseConsumer<Context> = consumer_config
        .create_with_context(Context {
            task: Arc::clone(task),
        })
        .map_err(|error| classify(&error))?;
    let topic = task
        .plan
        .input_topics
        .first()
        .ok_or(Error::InvalidConfiguration)?;
    consumer
        .subscribe(&[topic.as_str()])
        .map_err(|error| classify(&error))?;
    let expected = Tp::new(topic, 0);
    let budget = operation(task);
    let mut producer: Option<Arc<FutureProducer>> = None;
    let mut producer_epoch = 0;
    let mut worker = Worker {
        task: Arc::clone(task),
        thread: None,
        epoch: 0,
        starts: BTreeMap::new(),
    };
    let mut initializing = false;
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
            match outcome {
                Ok(Outcome::ProducerRecovered(recovered)) => {
                    producer = Some(recovered);
                    producer_epoch = worker.epoch;
                    if task.check_assignment(producer_epoch).is_ok() {
                        // 分配时查询的组位点可能早于旧事务的最终裁决；取得新 fencing 后必须重新查询再 seek。
                        // 这样旧事务晚确认的输入不会因为预取队列或旧 assignment 起点再次进入业务回调。
                        let mut requested = TopicPartitionList::new();
                        requested.add_partition(&expected.topic, expected.partition);
                        let committed = consumer
                            .committed_offsets(requested, budget)
                            .map_err(|error| classify(&error))?;
                        let offset = committed
                            .find_partition(&expected.topic, expected.partition)
                            .ok_or(Error::BrokerError)?
                            .offset();
                        let offset = match offset {
                            Offset::Offset(value) if value >= 0 => Offset::Offset(value),
                            Offset::Invalid => {
                                match task.config.consumer.auto_offset_reset.as_str() {
                                    "earliest" => Offset::Beginning,
                                    "latest" => Offset::End,
                                    _ => return Err(Error::InvalidConfiguration),
                                }
                            }
                            _ => return Err(Error::BrokerError),
                        };
                        consumer
                            .seek(&expected.topic, expected.partition, offset, budget)
                            .map_err(|error| classify(&error))?;
                        task.initialized();
                    }
                }
                Ok(Outcome::Committed) => attempt = 1,
                Ok(Outcome::Aborted(reason)) if !task.stopping() => {
                    if reason == Error::Rebalanced {
                        attempt = 1;
                    } else if attempt > task.plan.limits.max_aborted_retries {
                        task.fail(reason);
                    } else {
                        attempt += 1;
                    }
                    if !task.stopping() {
                        retry = true;
                        if task.check_assignment(worker.epoch).is_ok() {
                            // 只有旧分配仍有效时才能回退本地读取位置；确认中止不允许跳过输入。
                            let deadline = Instant::now() + budget;
                            for (tp, offset) in &worker.starts {
                                consumer
                                    .seek(
                                        &tp.topic,
                                        tp.partition,
                                        Offset::Offset(*offset),
                                        remaining(task, deadline)?,
                                    )
                                    .map_err(|error| classify(&error))?;
                            }
                        }
                        retry_after = Instant::now()
                            + Duration::from_millis(task.plan.limits.retry_backoff_ms.into());
                        if matches!(
                            reason,
                            Error::BrokerError | Error::TransactionTimeout | Error::Rebalanced
                        ) {
                            producer_epoch = 0;
                        }
                    }
                }
                Err(Error::Rebalanced) if initializing => {
                    // 初始化结果只属于其原始分配，失权后丢弃并等待新的真实分配。
                    retry_after = Instant::now()
                        + Duration::from_millis(task.plan.limits.retry_backoff_ms.into());
                }
                Err(Error::Cancelled) if task.stopping() => {}
                Err(error) => task.fail(error),
                Ok(_) => {}
            }
            initializing = false;
            task.settled(retry);
        }
        task.expire();
        if task.stopping() && worker.thread.is_none() {
            return Ok(());
        }
        let status = task.status();
        if !status.assignment.is_empty()
            && (status.assignment.len() != 1 || !status.assignment.contains(&expected))
        {
            // 运行期扩分区破坏单序列合同，不能把其中一部分继续当作完整权威。
            return Err(Error::InvalidConfiguration);
        }
        if worker.thread.is_none()
            && producer.is_some()
            && task.check_assignment(producer_epoch).is_err()
        {
            task.recovering_producer();
            let previous = producer.take().expect("owned producer");
            worker.thread = Some(
                std::thread::Builder::new()
                    .name("nafka-transaction-retire".into())
                    .spawn(move || {
                        drop(previous);
                        Ok(Outcome::Cancelled)
                    })
                    .map_err(|_| Error::Capacity)?,
            );
        }
        if worker.thread.is_none()
            && producer.is_none()
            && !task.stopping()
            && Instant::now() >= retry_after
            && task.check_assignment(status.assignment_epoch).is_ok()
        {
            // 仅在实际取得分区后执行 init_transactions，空分配实例不会干扰当前 producer。
            task.recovering_producer();
            let initialize_task = Arc::clone(task);
            let config = producer_config.clone();
            worker.epoch = status.assignment_epoch;
            let epoch = worker.epoch;
            initializing = true;
            worker.thread = Some(
                std::thread::Builder::new()
                    .name("nafka-transaction-initialize".into())
                    .spawn(move || {
                        initialize_producer_at(&initialize_task, &config, Some(epoch))
                            .map(Outcome::ProducerRecovered)
                    })
                    .map_err(|_| Error::Capacity)?,
            );
        }
        let paused = worker.thread.is_some()
            || producer.is_none()
            || task.stopping()
            || Instant::now() < retry_after
            || task.check_assignment(producer_epoch).is_err();
        let assignment = consumer.assignment().map_err(|error| classify(&error))?;
        if assignment.count() > 0 {
            if paused {
                consumer.pause(&assignment)
            } else {
                consumer.resume(&assignment)
            }
            .map_err(|error| classify(&error))?;
        }
        let poll_epoch = task.status().assignment_epoch;
        let Some(message) = consumer.poll(Duration::from_millis(50)) else {
            continue;
        };
        let message = match message {
            Ok(message) => message,
            Err(error) if transient_consumer_error(&error) => continue,
            Err(error) => return Err(classify(&error)),
        };
        if paused {
            let tp = Tp::new(message.topic(), message.partition());
            let offset = message.offset();
            drop(message);
            let current = task.status();
            if current.assignment_epoch == poll_epoch && current.assignment.contains(&tp) {
                consumer
                    .seek(&tp.topic, tp.partition, Offset::Offset(offset), budget)
                    .map_err(|error| classify(&error))?;
            }
            continue;
        }
        let epoch = producer_epoch;
        let mut records = vec![owned(&message, &task.plan.cluster_id)];
        drop(message);
        let mut bytes = input_bytes(&records[0]);
        while records.len() < task.plan.limits.max_batch_records as usize {
            let Some(next) = consumer.poll(Duration::ZERO) else {
                break;
            };
            let next = match next {
                Ok(message) => message,
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
        if task.check_assignment(epoch).is_err() || consumer.assignment_lost() {
            // 已撤回分配的输入不能交给处理器，下一 owner 从 broker 已提交位置接续。
            continue;
        }
        if bytes > task.plan.limits.max_batch_bytes
            || records.iter().any(|record| {
                record.topic != expected.topic
                    || record.partition != 0
                    || record.headers.len() > 256
                    || header_bytes(&record.headers) > 65536
            })
        {
            return Err(Error::Capacity);
        }
        let starts = BTreeMap::from([(expected.clone(), records.first().expect("input").offset)]);
        let ends = BTreeMap::from([(expected.clone(), records.last().expect("input").offset + 1)]);
        let (epoch, deadline) = task.begin(starts.clone(), ends.clone())?;
        let metadata = consumer.group_metadata().ok_or(Error::Rebalanced)?;
        consumer
            .pause(&consumer.assignment().map_err(|error| classify(&error))?)
            .map_err(|error| classify(&error))?;
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
