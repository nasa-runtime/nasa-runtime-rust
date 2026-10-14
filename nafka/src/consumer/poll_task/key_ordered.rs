//! 跨 poll 的有界分区与按键调用，以及当前 assignment 内的完成证据。

use super::handler::{user_paused, HandlerWorker};
use super::*;

/// 尚未进入连续安全前缀的业务结论；原输入由有界队列保留，失权后仍依据 broker 位点重投。
struct CompletedKey {
    /// 包含原记录和有界诊断余量的保守额度。
    bytes: usize,
    /// 已执行的业务结论，重复 fetch 时不能重新调用 handler。
    outcome: InvocationOutcome,
    /// 区分按键观测与普通分区调用，避免把不同执行模式的积压混合计数。
    keyed: bool,
    /// 本次实际调用的记录数量，后续取入不能扩大已完成批次的裁决范围。
    count: usize,
}

/// 当前 owner 会话私有的完成证据，不提供跨重启去重承诺。
#[derive(Default)]
pub(super) struct KeyCompletions {
    /// 分区和真实 offset 是当前 assignment 内唯一的责任身份。
    entries: BTreeMap<(Tp, i64), CompletedKey>,
    /// 与每项额度同时释放的字节责任。
    retained_bytes: usize,
    /// 实际调用跨 poll 存活；逻辑取消不能删除其退出责任。
    workers: Vec<Worker>,
}

impl KeyCompletions {
    /// 业务作用：发布按键调用和保留证据的即时容量快照，不在 owner 上调用外部观察器。
    ///
    /// 参数说明：`health` 为当前槽健康状态。
    /// 返回：只更新数量和字节快照，不改变 Ready、暂停原因或提交资格。
    pub(super) fn publish_load(&self, health: &Arc<RwLock<GroupHealth>>) {
        let mut snapshot = health
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        snapshot.active_handlers = self.workers.len();
        snapshot.key_active_handlers = self.workers.iter().filter(|worker| worker.keyed).count();
        snapshot.key_pending_records = self.entries.values().filter(|entry| entry.keyed).count();
        snapshot.key_pending_bytes = self
            .entries
            .values()
            .filter(|entry| entry.keyed)
            .map(|entry| entry.bytes)
            .sum();
    }

    /// 业务作用：提交已经越过压缩空洞后回收无需再次 fetch 的完成证据。
    ///
    /// 参数说明：`progress` 为只在 broker 确认后推进的分区账本。
    /// 返回：保留尚未确认的记录；不会按本地 fetch position 提前释放结果。
    pub(super) fn prune_committed(&mut self, progress: &GroupOffsetState) {
        self.entries.retain(|(tp, offset), _| {
            progress
                .confirmed_offset(tp)
                .is_none_or(|next| *offset >= next)
        });
        self.retained_bytes = self.entries.values().map(|entry| entry.bytes).sum();
    }

    /// 业务作用：失权或人工定位时作废全部旧完成证据。
    ///
    /// 参数说明：无。
    /// 返回：清除完成证据并永久撤销旧调用；实际调用的输入和名额仍保留到退出。
    pub(super) fn clear(&mut self) {
        for worker in &mut self.workers {
            worker.authorized = false;
            worker.invocation.fence(false);
        }
        self.entries.clear();
        self.retained_bytes = 0;
    }

    /// 业务作用：人工定位只作废目标分区的完成证据，避免其它分区重复执行业务。
    /// 参数说明：tp 为显式定位目标。
    /// 返回：仅回收该分区的记录和字节额度，其它空洞后的已完成结果继续保留。
    pub(super) fn clear_partition(&mut self, tp: &Tp) {
        self.entries.retain(|(partition, _), _| partition != tp);
        self.retained_bytes = self.entries.values().map(|entry| entry.bytes).sum();
    }

    /// 业务作用：查询已实际退出的调用结果，允许进度容量不足时保留原证据。
    ///
    /// 参数说明：`tp`、`offset` 为当前交付记录的分区与坐标。
    /// 返回：已完成结果的副本；未执行的记录返回 None。
    pub(super) fn outcome(&self, tp: &Tp, offset: i64) -> Option<InvocationOutcome> {
        self.entries
            .get(&(tp.clone(), offset))
            .map(|entry| entry.outcome.clone())
    }

    /// 业务作用：结果进入提交或失败治理状态机后释放其独立完成额度。
    ///
    /// 参数说明：`tp`、`offset` 为已经被 owner 接管的责任身份。
    /// 返回：移除对应证据；不存在时不改变其它额度。
    pub(super) fn remove(&mut self, tp: &Tp, offset: i64) {
        if let Some(entry) = self.entries.remove(&(tp.clone(), offset)) {
            self.retained_bytes -= entry.bytes;
        }
    }

    /// 业务作用：在额度已经预留后发布实际退出的调用结果。
    ///
    /// 参数说明：`identity` 为记录身份；`bytes` 为预留额度；`outcome` 为业务结论；keyed 区分执行模式；count 固定本次实际批次边界。
    /// 返回：保留一份有界证据，直到 owner 接管或 assignment 作废。
    fn insert(
        &mut self,
        identity: (Tp, i64),
        bytes: usize,
        outcome: InvocationOutcome,
        keyed: bool,
        count: usize,
    ) {
        self.remove(&identity.0, identity.1);
        self.retained_bytes += bytes;
        self.entries.insert(
            identity,
            CompletedKey {
                bytes,
                outcome,
                keyed,
                count,
            },
        );
    }
}

/// 一个实际业务调用的退出责任；join 完成前不释放对应 key 或 worker 名额。
struct Worker {
    /// 当前 assignment 内的记录身份。
    identity: (Tp, i64),
    /// 调度时权威；旧调用可继续退出但不得授权新的来源提交。
    assignment_epoch: u64,
    /// 显式失权不可因下一轮 epoch 相同而重新授权。
    authorized: bool,
    /// 仅按键调用计入按键并发观测。
    keyed: bool,
    /// 实际派发范围，独立于后续 poll 追加的输入。
    count: usize,
    /// 实际调用占有的原始 key；null key 独占整个分区。
    key: Option<Vec<u8>>,
    /// 调用与完成证据共享的额度。
    bytes: usize,
    /// 实际业务调用的退出责任与确认权限。
    invocation: HandlerWorker,
    /// 消费 span 随实际调用结束，不随等待者取消而提前结束。
    span: Option<natelemetry::SpanGuard>,
}

/// 业务作用：把可保留的失败说明限制为最多 1024 字节，同时保留 DLT、延后和致命语义。
///
/// 参数说明：`outcome` 为业务调用的原始结论。
/// 返回：可以进入有界完成证据的结果；不会把错误改成成功。
fn bounded_outcome(outcome: InvocationOutcome) -> InvocationOutcome {
    /// 业务作用：按 UTF-8 边界收缩保留的诊断，避免业务错误文本突破完成队列预算。
    ///
    /// 参数说明：`value` 为待保留的失败说明。
    /// 返回：最多 1024 字节且仍合法的 UTF-8 文本。
    fn bounded(mut value: String) -> String {
        let mut end = value.len().min(1024);
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        value.truncate(end);
        value.shrink_to_fit();
        value
    }
    match outcome {
        InvocationOutcome::DecodeFailed { failures } => InvocationOutcome::DecodeFailed {
            failures: failures
                .into_iter()
                .map(|(offset, reason)| (offset, bounded(reason)))
                .collect(),
        },
        InvocationOutcome::Failed(NafkaError::HandlerDeferred(reason)) => {
            InvocationOutcome::Failed(NafkaError::HandlerDeferred(bounded(reason)))
        }
        InvocationOutcome::Failed(NafkaError::HandlerDeadLetter(reason)) => {
            InvocationOutcome::Failed(NafkaError::HandlerDeadLetter(bounded(reason)))
        }
        InvocationOutcome::Failed(error) => {
            InvocationOutcome::Failed(NafkaError::Broker(bounded(error.to_string())))
        }
        InvocationOutcome::Fatal(error) => {
            InvocationOutcome::Fatal(NafkaError::Lifecycle(bounded(error.to_string())))
        }
        completed => completed,
    }
}

impl KeyCompletions {
    /// 业务作用：在主循环收割真实完成并即时撤销过期调用，不等待慢分区退出。
    /// 参数说明：consumer/epoch 为当前分配事实；stopping 为停止准入；health 为本槽观测。
    /// 返回：仅有效代次的结论进入待裁决表；旧调用仍持有资源直到实际完成。
    pub(super) fn collect_finished(
        &mut self,
        consumer: &ConsumerClient,
        epoch: u64,
        stopping: bool,
        health: &Arc<RwLock<GroupHealth>>,
    ) {
        let authorized = !stopping && !consumer.has_rebalance() && !consumer.assignment_lost();
        let mut index = 0;
        while index < self.workers.len() {
            let valid = authorized
                && self.workers[index].authorized
                && self.workers[index].assignment_epoch == epoch;
            if !valid {
                self.workers[index].authorized = false;
            }
            self.workers[index].invocation.fence(valid);
            if self.workers[index].invocation.is_finished() {
                let worker = self.workers.swap_remove(index);
                let outcome = bounded_outcome(worker.invocation.finish());
                if let Some(span) = worker.span {
                    let _ = span.finish(None);
                }
                // 失权后的实际退出只能释放责任，不能把旧结果当成新分配已经完成的业务。
                if valid {
                    self.insert(
                        worker.identity,
                        worker.bytes,
                        outcome,
                        worker.keyed,
                        worker.count,
                    );
                }
            } else {
                index += 1;
            }
        }
        self.publish_load(health);
    }

    /// 业务作用：识别停止和定位必须继续等待的真实业务责任。
    /// 参数说明：无。
    /// 返回：仍有实际调用时为true，不把超时或确认权撤销当作完成。
    pub(super) fn has_active(&self) -> bool {
        !self.workers.is_empty()
    }

    /// 业务作用：在 owner 进入本地等待前复验实际完成，避免已经退出的调用继续承担 poll 等待。
    /// 参数说明：无。
    /// 返回：至少一个业务调用已实际结束时为 true；不提前收回责任或授权提交。
    pub(super) fn has_finished(&self) -> bool {
        self.workers
            .iter()
            .any(|worker| worker.invocation.is_finished())
    }

    /// 业务作用：立即发现已经实际退出的框架致命结论，禁止继续派发其它顺序域。
    /// 参数说明：无。
    /// 返回：存在致命结论时给出有界原因；业务失败、未确认和普通超时不触发全槽停止。
    pub(super) fn fatal_error(&self) -> Option<String> {
        self.entries
            .values()
            .find_map(|entry| match &entry.outcome {
                InvocationOutcome::Fatal(error) => Some(error.to_string()),
                _ => None,
            })
    }

    /// 业务作用：让失败回退等待本分区实际调用，避免同坐标重读与旧副本重叠。
    /// 参数说明：tp为需要回退或定位的分区。
    /// 返回：该分区仍有真实调用时为true，其它分区不受影响。
    pub(super) fn active_partition(&self, tp: &Tp) -> bool {
        self.workers.iter().any(|worker| worker.identity.0 == *tp)
    }

    /// 业务作用：固定已执行批次的真实边界，后续poll不能把新输入并入旧业务结果。
    /// 参数说明：tp/offset为已执行单元的首坐标。
    /// 返回：已完成单元的原记录数；尚无结果为None。
    pub(super) fn completed_count(&self, tp: &Tp, offset: i64) -> Option<usize> {
        self.entries
            .get(&(tp.clone(), offset))
            .map(|entry| entry.count)
    }

    /// 业务作用：在失败分区的旧调用全部退出后解除执行保护，其它暂停原因仍独立生效。
    /// 参数说明：consumer/health为当前native归属与暂停事实。
    /// 返回：只恢复没有实际调用的分区，不改变任何来源位点。
    pub(super) fn resume_idle(&self, consumer: &ConsumerClient, health: &Arc<RwLock<GroupHealth>>) {
        let targets = health
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .paused
            .iter()
            .filter(|(tp, reason)| {
                *reason == PauseReason::HandlerInFlight && !self.active_partition(tp)
            })
            .map(|(tp, _)| tp.clone())
            .collect::<Vec<_>>();
        let resumable = clear_pause_reason(health, &targets, &PauseReason::HandlerInFlight);
        let _ = consumer.resume(&resumable);
    }

    /// 业务作用：判定本地重读坐标是否已有实际调用，防止对同一输入重复派发。
    /// 参数说明：tp/offset为输入身份。
    /// 返回：仍有对应调用时为true，包括正在失权收尾的调用。
    pub(super) fn active_at(&self, tp: &Tp, offset: i64) -> bool {
        self.workers
            .iter()
            .any(|worker| worker.identity.0 == *tp && worker.identity.1 == offset)
    }

    /// 业务作用：让一个分区的single/batch调用进入持久执行集合，主循环立即回到其它分区。
    /// 参数说明：runtime/route为冻结执行资源；records为连续输入。
    /// 返回：true表示已启动，false表示等待额度；启动失败交回明确框架结论。
    pub(super) fn start_partition(
        &mut self,
        runtime: &Arc<OwnerRuntime>,
        route: &Arc<dyn crate::consumer::erased::ErasedConsumer>,
        records: Vec<RawRecord>,
    ) -> std::result::Result<bool, InvocationOutcome> {
        // Stop 的源事实可能先于控制邮箱到达，准入点必须再次复验。
        if runtime.stop_requested.load(Ordering::Acquire) {
            return Ok(false);
        }
        let first = records.first().expect("分区调用非空");
        let identity = (first.ctx.topic_partition(), first.ctx.offset);
        if self.active_at(&identity.0, identity.1) {
            return Ok(false);
        }
        let Some(permit) = runtime.handlers.reserve(Some((&identity.0, None))) else {
            return Ok(false);
        };
        let assignment_epoch = first.assignment_epoch;
        let count = records.len();
        let bytes = records
            .iter()
            .map(RawRecord::retained_bytes)
            .sum::<usize>()
            .saturating_add(2048);
        // 只有取得实际调用额度才建立消费 span，等待额度或应用旧结果不产生虚假调用。
        for record in &records {
            if let Err(reason) = crate::extract_trace_context(&record.ctx.headers) {
                runtime.counter(
                    "trace_invalid_total",
                    1,
                    &[("group", &record.ctx.group), ("reason", reason.as_str())],
                );
            }
        }
        // 批调用使用独立根，逐输入父关系由有限links作用域提供，首条不代表整批因果关系。
        let (guard, context) = crate::trace::consume_span(
            runtime.span_recorder.as_ref(),
            (records.len() == 1)
                .then(|| first.ctx.trace_context())
                .flatten(),
        );
        let ambient = Some(context);
        let span = tracing::info_span!("kafka.consume", group = %first.ctx.group, consumer_id = route.meta().id,
            topic = %first.ctx.topic, partition = first.ctx.partition, first_offset = first.ctx.offset,
            last_offset = records.last().expect("分区调用非空").ctx.offset, count,
            event = %route.meta().event, delivery_attempt = first.ctx.delivery_attempt,
            retry_attempt = first.ctx.retry_attempt, trace_id = %context.trace_id_hex());
        let invocation = HandlerWorker::spawn(runtime, route, records, ambient, span, permit)?;
        self.workers.push(Worker {
            identity,
            assignment_epoch,
            authorized: true,
            keyed: false,
            count,
            key: None,
            bytes,
            invocation,
            span: guard,
        });
        Ok(true)
    }

    /// 业务作用：为一个分区派发至多一个可执行key，轮转让其它分区取得新归还的调用额度。
    /// 参数说明：runtime/attempts为冻结资源与可信次数；prepared为保序输入；health用于复验用户暂停；retry_offset 为尚在退避的失败前缀。
    /// 返回：只创建有额度且不跨越同key或null屏障的调用，未执行输入继续留在队列。
    pub(super) fn launch_round(
        &mut self,
        runtime: &Arc<OwnerRuntime>,
        attempts: &BTreeMap<(Tp, i64), RetryAttemptState>,
        prepared: &mut VecDeque<PreparedTypedItem>,
        health: &Arc<RwLock<GroupHealth>>,
        retry_offset: Option<i64>,
    ) {
        if runtime.stop_requested.load(Ordering::Acquire) {
            return;
        }
        let Some(first) = prepared.front() else {
            return;
        };
        let identity = first.location();
        if user_paused(health, &identity.0) {
            return;
        }
        let mut workers = std::mem::take(&mut self.workers);
        launch_available(
            runtime,
            attempts,
            prepared,
            self,
            &identity,
            &mut workers,
            retry_offset,
        );
        self.workers = workers;
        self.publish_load(health);
    }
}

/// 业务作用：复用已经退出的 worker 名额，让慢 key 之外的同 key 后继继续推进。
///
/// 参数说明：`runtime` 为冻结运行资源；`attempts` 为可信尝试表；`prepared` 为待派发记录；
/// `completed` 为保留证据；`first_identity` 为本次必须预留容量的最早责任；`workers` 为实际调用集合；
/// retry_offset 为仍在退避的失败前缀，其 key 在本轮保持关闭。
/// 返回：在数量、字节、顺序和 null 屏障均允许时启动调用，否则保留记录等待 owner 裁决。
fn launch_available(
    runtime: &Arc<OwnerRuntime>,
    attempts: &BTreeMap<(Tp, i64), RetryAttemptState>,
    prepared: &mut VecDeque<PreparedTypedItem>,
    completed: &mut KeyCompletions,
    first_identity: &(Tp, i64),
    workers: &mut Vec<Worker>,
    retry_offset: Option<i64>,
) {
    if workers
        .iter()
        .any(|worker| worker.identity.0 == first_identity.0 && worker.key.is_none())
    {
        return;
    }
    let config = &runtime.config.behavior;
    let mut keys: BTreeSet<_> = workers
        .iter()
        .filter(|worker| worker.identity.0 == first_identity.0)
        .map(|worker| worker.key.clone())
        .collect();
    let mut prior_safe = true;
    let reserved_bytes = completed
        .retained_bytes
        .saturating_add(workers.iter().map(|worker| worker.bytes).sum::<usize>());
    let reserved_records = completed.entries.len() + workers.len();
    for item in prepared.iter_mut() {
        let PreparedTypedItem::Routed { source, route } = item else {
            break;
        };
        let identity = (source.ctx.topic_partition(), source.ctx.offset);
        if retry_offset == Some(identity.1) {
            // 用户 resume 只能解除用户暂停，不能提前开放尚未到期的失败 key。
            prior_safe = false;
            keys.insert(source.ctx.raw_key.clone());
            if source.ctx.raw_key.is_none() {
                break;
            }
            continue;
        }
        if let Some(entry) = completed.entries.get(&identity) {
            if matches!(
                entry.outcome,
                InvocationOutcome::Completed {
                    acked_prefix: 1,
                    total: 1,
                    ..
                }
            ) {
                continue;
            }
            // 未确认、失败或 DLT 请求仍占有顺序域，只有 owner 完成裁决后才能放行同 key 后继。
            prior_safe = false;
            keys.insert(source.ctx.raw_key.clone());
            if source.ctx.raw_key.is_none() {
                break;
            }
            continue;
        }
        // null key 必须等待此前全部实际调用得到安全结果，后面的 key 同样不能越过它。
        if source.ctx.raw_key.is_none() && !prior_safe {
            break;
        }
        prior_safe = false;
        if !keys.insert(source.ctx.raw_key.clone()) {
            continue;
        }
        if workers.len() >= config.max_in_flight_handlers {
            break;
        }
        // 原始输入已经通过单记录上限；物化后增加的可信投递头由诊断余量承担，
        // 不能让这部分有界增量超过为最早空洞保留的最大记录名额。
        let bytes = source
            .retained_bytes()
            .min(config.max_record_bytes)
            .saturating_add(2048);
        // 为前缀重试保留一个最大记录的名额；高 offset 结果不能占尽填补空洞的容量。
        let first_reserved = completed
            .entries
            .get(first_identity)
            .map(|entry| entry.bytes)
            .or_else(|| {
                workers
                    .iter()
                    .find(|worker| &worker.identity == first_identity)
                    .map(|worker| worker.bytes)
            });
        let reserve = if &identity == first_identity {
            0
        } else {
            // 最早责任已经占有的额度也属于恢复预留，不能再重复扣除一个完整名额。
            config
                .max_record_bytes
                .saturating_add(2048)
                .saturating_sub(first_reserved.unwrap_or(0))
        };
        let record_reserve = usize::from(&identity != first_identity && first_reserved.is_none());
        if reserved_records.saturating_add(1 + record_reserve) > config.max_group_progress_records
            || reserved_bytes.saturating_add(bytes).saturating_add(reserve)
                > config.max_group_pending_bytes
        {
            break;
        }
        let (failed_deliveries, bounded_failures) = retry_counts(attempts, &identity);
        let Some(permit) = runtime
            .handlers
            .reserve(Some((&identity.0, source.ctx.raw_key.as_deref())))
        else {
            continue;
        };
        annotate_delivery_attempt(
            std::slice::from_mut(source),
            failed_deliveries.saturating_add(1),
            bounded_failures.saturating_add(1),
        );
        if let Err(reason) = crate::extract_trace_context(&source.ctx.headers) {
            runtime.counter(
                "trace_invalid_total",
                1,
                &[("group", &source.ctx.group), ("reason", reason.as_str())],
            );
        }
        let (span, context) =
            crate::trace::consume_span(runtime.span_recorder.as_ref(), source.ctx.trace_context());
        let ambient = Some(context);
        let launched = HandlerWorker::spawn(
            runtime,
            route,
            vec![source.clone()],
            ambient,
            tracing::Span::current(),
            permit,
        );
        match launched {
            Ok(invocation) => workers.push(Worker {
                identity,
                assignment_epoch: source.assignment_epoch,
                authorized: true,
                keyed: true,
                count: 1,
                key: source.ctx.raw_key.clone(),
                bytes,
                invocation,
                span,
            }),
            Err(outcome) => {
                if let Some(span) = span {
                    let _ = span.finish(None);
                }
                completed.insert(identity, bytes, outcome, true, 1);
            }
        }
        // 每轮每分区只启动一个顺序域，下一次 owner 轮转再使用其余空闲名额。
        break;
    }
}
