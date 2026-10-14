//! 只读分区证据采集；等待取消不释放仍执行中的原生资源。

use super::admin::AdminHandle;
use crate::{
    KafkaConfig, NafkaError, ObservedOffset, OffsetIsolation, OffsetMissing, OffsetObservation,
    PartitionOffsets, Result, Tp,
};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

/// 尚未封存的有限目标报告；实际查询不在该锁内执行。
struct Progress {
    active: bool,
    report: OffsetObservation,
}

/// 等待者退出时撤销新查询和晚到结果的发布权。
struct WaitGuard(Arc<Mutex<Progress>>);

impl Drop for WaitGuard {
    /// 业务作用：等待取消只封闭后继查询，已接纳原生调用仍由其任务持有。
    /// 参数说明：无。返回：报告不再接受新证据，不取消其它源或消费组。
    fn drop(&mut self) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active = false;
    }
}

/// 本轮位置维度，不用字符串决定字段语义。
#[derive(Clone, Copy)]
enum Field {
    Beginning,
    End,
    Committed,
    Timestamp,
}

/// 业务作用：启动不加入组的有界只读查询，返回截止前已取得的逐字段证据。
/// 参数说明：owner 为源级原生资源责任，config 为冻结配置，group 为可选组，tps 为有效目标，isolation 为隔离级别，timestamp 为可选时间，deadline 为总期限。
/// 返回：不可变报告；调用取消或期限耗尽后，实际原生客户端仍随任务持有 owner 到析构完成。
pub(crate) async fn observe(
    owner: Arc<AdminHandle>,
    config: KafkaConfig,
    group: Option<String>,
    mut tps: Vec<Tp>,
    isolation: OffsetIsolation,
    timestamp: Option<i64>,
    deadline: Instant,
) -> Result<OffsetObservation> {
    tps.sort();
    let started = SystemTime::now();
    let report = OffsetObservation {
        source_id: config.client_name.clone(),
        group: group.clone(),
        isolation,
        started_at: started,
        completed_at: started,
        partitions: tps
            .iter()
            .cloned()
            .map(|tp| PartitionOffsets {
                tp,
                beginning: ObservedOffset::absent(OffsetMissing::Timeout),
                end: ObservedOffset::absent(OffsetMissing::Timeout),
                committed: ObservedOffset::absent(if group.is_some() {
                    OffsetMissing::Timeout
                } else {
                    OffsetMissing::NotRequested
                }),
                timestamp_offset: ObservedOffset::absent(if timestamp.is_some() {
                    OffsetMissing::Timeout
                } else {
                    OffsetMissing::NotRequested
                }),
                lag: ObservedOffset::absent(if group.is_some() {
                    OffsetMissing::Timeout
                } else {
                    OffsetMissing::NotRequested
                }),
            })
            .collect(),
    };
    let state = Arc::new(Mutex::new(Progress {
        active: true,
        report,
    }));
    let _wait = WaitGuard(Arc::clone(&state));
    let worker_state = Arc::clone(&state);
    let (sender, receiver) = tokio::sync::oneshot::channel();
    tokio::task::spawn_blocking(move || {
        // 原生资源和 Admin 借用一起存活；发送报告后仍可能执行客户端析构。
        let _owner = owner;
        if remaining(&worker_state, deadline).is_none() {
            let _ = sender.send(Ok(()));
            return;
        }
        let native = super::config::consumer_config(
            &config,
            group.as_deref().unwrap_or("nasa-offset-observation"),
        );
        let consumer = native.and_then(|mut native| {
            native
                .set(
                    "isolation.level",
                    match isolation {
                        OffsetIsolation::ReadUncommitted => "read_uncommitted",
                        OffsetIsolation::ReadCommitted => "read_committed",
                    },
                )
                .set("allow.auto.create.topics", "false")
                .set("enable.auto.commit", "false")
                .set("enable.auto.offset.store", "false");
            native
                .create::<BaseConsumer>()
                .map_err(|_| NafkaError::Config("只读位置客户端构造失败".into()))
        });
        match consumer {
            Ok(consumer) => {
                inspect(
                    &consumer,
                    &worker_state,
                    &tps,
                    group.is_some(),
                    timestamp,
                    deadline,
                );
                let _ = sender.send(Ok(()));
                // 结果等待结束不释放此客户端；最终 source 关闭还要等真实析构和 owner 借用归还。
                drop(consumer);
            }
            Err(error) => {
                let _ = sender.send(Err(error));
            }
        }
    });
    let outcome = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), receiver).await;
    let report = seal(&state);
    match outcome {
        Ok(Ok(result)) => {
            result?;
            Ok(report)
        }
        Ok(Err(_)) => Err(NafkaError::Broker("只读位置查询任务未完成".into())),
        Err(_) => Ok(report),
    }
}

/// 业务作用：在共同期限和等待者权威仍有效时给下一原生查询分配剩余预算。
/// 参数说明：state 为本轮报告，deadline 为固定截止时刻。
/// 返回：可查询时为正预算；取消或超时不再发出后继请求。
fn remaining(state: &Mutex<Progress>, deadline: Instant) -> Option<Duration> {
    if !state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .active
    {
        return None;
    }
    let budget = deadline.saturating_duration_since(Instant::now());
    (!budget.is_zero()).then_some(budget)
}

/// 业务作用：只在本轮仍有权发布时保存一个字段，晚到结果不能改变已交付报告。
/// 参数说明：state 为报告，index 为目标下标，field 为维度，value 为证据，deadline 为总期限。
/// 返回：期限内有效报告被更新；取消或封存后丢弃本地结果，原生责任仍独立收口。
fn publish(
    state: &Mutex<Progress>,
    index: usize,
    field: Field,
    value: ObservedOffset,
    deadline: Instant,
) {
    let mut progress = state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !progress.active || Instant::now() >= deadline {
        return;
    }
    let partition = &mut progress.report.partitions[index];
    match field {
        Field::Beginning => partition.beginning = value,
        Field::End => partition.end = value,
        Field::Committed => partition.committed = value,
        Field::Timestamp => partition.timestamp_offset = value,
    }
}

/// 业务作用：封存已获得证据并计算同一报告内的位置距离，拒绝矛盾保留范围。
/// 参数说明：state 为正在收集的报告。
/// 返回：独立报告副本；未请求或缺失维度不被填成零。
fn seal(state: &Mutex<Progress>) -> OffsetObservation {
    let mut progress = state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    progress.active = false;
    let now = SystemTime::now();
    progress.report.completed_at = now;
    for item in &mut progress.report.partitions {
        if matches!((item.beginning.value, item.end.value), (Some(low), Some(high)) if low > high) {
            item.beginning = ObservedOffset::absent(OffsetMissing::Inconsistent);
            item.end = ObservedOffset::absent(OffsetMissing::Inconsistent);
        }
        item.lag = match (item.committed.value, item.end.value) {
            (Some(committed), Some(end)) => ObservedOffset {
                value: Some(end.saturating_sub(committed).max(0)),
                missing: None,
                observed_at: item.committed.observed_at.max(item.end.observed_at),
            },
            (None, _) => item.committed.clone(),
            (_, None) => item.end.clone(),
        };
        for value in [
            &mut item.beginning,
            &mut item.end,
            &mut item.committed,
            &mut item.timestamp_offset,
            &mut item.lag,
        ] {
            if value.missing == Some(OffsetMissing::Timeout) {
                value.observed_at = now;
            }
        }
    }
    progress.report.clone()
}

/// 业务作用：逐目标核实存在性，再读取隔离水位及可选提交和时间证据。
/// 参数说明：consumer 为未订阅客户端，state 为报告，tps 为有界目标，group 表示请求提交，timestamp 为时间条件，deadline 为共同期限。
/// 返回：已取得字段写入报告；失败只影响相应证据，不修改 broker 或消费组。
fn inspect(
    consumer: &BaseConsumer,
    state: &Mutex<Progress>,
    tps: &[Tp],
    group: bool,
    timestamp: Option<i64>,
    deadline: Instant,
) {
    let mut valid = BTreeSet::new();
    let mut topics = BTreeMap::<&str, Vec<usize>>::new();
    for (index, tp) in tps.iter().enumerate() {
        topics.entry(&tp.topic).or_default().push(index);
    }
    for (topic, indexes) in topics {
        let Some(budget) = remaining(state, deadline) else {
            return;
        };
        let metadata = consumer.fetch_metadata(Some(topic), Timeout::After(budget));
        for index in indexes {
            let existence = match &metadata {
                Ok(metadata) => metadata
                    .topics()
                    .iter()
                    .find(|item| item.name() == topic)
                    .ok_or(OffsetMissing::Unavailable)
                    .and_then(|item| {
                        if let Some(error) = item.error() {
                            return Err(classify(&KafkaError::MetadataFetch(error.into())));
                        }
                        item.partitions()
                            .iter()
                            .find(|part| part.id() == tps[index].partition)
                            .ok_or(OffsetMissing::ResourceMissing)
                            .and_then(|part| {
                                part.error().map_or(Ok(()), |error| {
                                    Err(classify(&KafkaError::MetadataFetch(error.into())))
                                })
                            })
                    }),
                Err(error) => Err(classify(error)),
            };
            match existence {
                Ok(()) => {
                    valid.insert(index);
                }
                Err(reason) => {
                    for field in [Field::Beginning, Field::End] {
                        publish(
                            state,
                            index,
                            field,
                            ObservedOffset::absent(reason),
                            deadline,
                        );
                    }
                    if group {
                        publish(
                            state,
                            index,
                            Field::Committed,
                            ObservedOffset::absent(reason),
                            deadline,
                        );
                    }
                    if timestamp.is_some() {
                        publish(
                            state,
                            index,
                            Field::Timestamp,
                            ObservedOffset::absent(reason),
                            deadline,
                        );
                    }
                }
            }
        }
    }
    for &index in &valid {
        let Some(budget) = remaining(state, deadline) else {
            return;
        };
        let tp = &tps[index];
        // 客户端已冻结 isolation.level，实时 ListOffsets 的末尾才具备相应 LSO/HW 语义。
        match consumer.fetch_watermarks(&tp.topic, tp.partition, Timeout::After(budget)) {
            Ok((low, high)) if low >= 0 && high >= low => {
                publish(
                    state,
                    index,
                    Field::Beginning,
                    ObservedOffset::present(low),
                    deadline,
                );
                publish(
                    state,
                    index,
                    Field::End,
                    ObservedOffset::present(high),
                    deadline,
                );
            }
            result => {
                let reason = result
                    .err()
                    .as_ref()
                    .map_or(OffsetMissing::Inconsistent, classify);
                for field in [Field::Beginning, Field::End] {
                    publish(
                        state,
                        index,
                        field,
                        ObservedOffset::absent(reason),
                        deadline,
                    );
                }
            }
        }
    }
    if valid.is_empty() {
        return;
    }
    if group {
        committed(consumer, state, tps, &valid, deadline);
    }
    if let Some(timestamp) = timestamp {
        let Some(budget) = remaining(state, deadline) else {
            return;
        };
        let mut query = TopicPartitionList::with_capacity(valid.len());
        for &index in &valid {
            let tp = &tps[index];
            if query
                .add_partition_offset(&tp.topic, tp.partition, Offset::Offset(timestamp))
                .is_err()
            {
                return;
            }
        }
        let result = consumer.offsets_for_times(query, Timeout::After(budget));
        offsets_result(state, tps, &valid, result, Field::Timestamp, deadline);
    }
}

/// 业务作用：在同一总预算内处理组协调器发现和逐分区提交证据，不加入消费组。
/// 参数说明：consumer 为查询客户端，state 为报告，tps/valid 为本轮有存在证据的目标，deadline 为总期限。
/// 返回：确认无历史、有效位置和查询失败分别记录，不把权限拒绝变成 None。
fn committed(
    consumer: &BaseConsumer,
    state: &Mutex<Progress>,
    tps: &[Tp],
    valid: &BTreeSet<usize>,
    deadline: Instant,
) {
    loop {
        let Some(budget) = remaining(state, deadline) else {
            return;
        };
        let mut query = TopicPartitionList::with_capacity(valid.len());
        for &index in valid {
            query.add_partition(&tps[index].topic, tps[index].partition);
        }
        let mut result = consumer.committed_offsets(
            query,
            Timeout::After(budget.min(Duration::from_millis(250))),
        );
        // FindCoordinator 的组拒绝可能只交给消费事件队列；保留该证据，不能把等待切片耗尽报告为无权限以外的结果。
        if let Some(error) = super::admin::poll_query_rejection(consumer) {
            result = Err(error);
        }
        if result
            .as_ref()
            .err()
            .is_some_and(super::admin::is_coordinator_lookup_transient)
        {
            let Some(budget) = remaining(state, deadline) else {
                return;
            };
            // 冷组协调器发现只在剩余预算内退避，不能为每轮重新分配完整请求期限。
            std::thread::sleep(budget.min(Duration::from_millis(100)));
            continue;
        }
        offsets_result(state, tps, valid, result, Field::Committed, deadline);
        return;
    }
}

/// 业务作用：把原生列表中的每个目标独立转换成位置或缺失证据。
/// 参数说明：state 为报告，tps/valid 为请求目标，result 为原生完整列表或调用错误，field 为提交或时间维度，deadline 为总期限。
/// 返回：缺项和资源级失败不覆盖其它目标的有效值，负哨兵按维度区分。
fn offsets_result(
    state: &Mutex<Progress>,
    tps: &[Tp],
    valid: &BTreeSet<usize>,
    result: std::result::Result<TopicPartitionList, KafkaError>,
    field: Field,
    deadline: Instant,
) {
    for &index in valid {
        let value = match &result {
            Ok(list) => match list.find_partition(&tps[index].topic, tps[index].partition) {
                None => ObservedOffset::absent(OffsetMissing::Unavailable),
                Some(element) => match element.error() {
                    Err(error) => ObservedOffset::absent(classify(&error)),
                    Ok(()) => match (element.offset(), field) {
                        (Offset::Offset(value), _) if value >= 0 => ObservedOffset::present(value),
                        (Offset::Invalid, Field::Committed) => {
                            ObservedOffset::absent(OffsetMissing::NoCommittedOffset)
                        }
                        (Offset::End, Field::Timestamp) => {
                            ObservedOffset::absent(OffsetMissing::NoTimestampMatch)
                        }
                        _ => ObservedOffset::absent(OffsetMissing::Inconsistent),
                    },
                },
            },
            Err(error) => ObservedOffset::absent(classify(error)),
        };
        publish(state, index, field, value, deadline);
    }
}

/// 业务作用：只按原生固定错误码区分只读失败，不输出远端正文。
/// 参数说明：error 为当前查询或资源级错误。
/// 返回：确定权限、资源缺失、期限或不可观测；本地 UnknownPartition 不冒充 broker 缺失证据。
fn classify(error: &KafkaError) -> OffsetMissing {
    match super::errors::code(error) {
        Some(code) if super::errors::authorization(code) => OffsetMissing::Authorization,
        Some(RDKafkaErrorCode::UnknownTopicOrPartition) => OffsetMissing::ResourceMissing,
        Some(
            RDKafkaErrorCode::OperationTimedOut
            | RDKafkaErrorCode::RequestTimedOut
            | RDKafkaErrorCode::TimedOutQueue,
        ) => OffsetMissing::Timeout,
        _ => OffsetMissing::Unavailable,
    }
}
