//! 订阅组的 owner 集合、组级控制路由与聚合健康。

use super::supervisor::{
    CommandReply, GroupPlan, OwnerCommandKind, OwnerHandle, OwnerMode, StartupGate,
};
use crate::{
    config::KafkaConfig,
    error::{NafkaError, Result},
    health::{GroupHealth, GroupState},
    types::Tp,
    KafkaProxyInner,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

/// owner 的本地恢复状态、共享组输入与执行池以及源资源；业务发布使用源的原有 producer。
pub(crate) struct OwnerRuntime {
    inner: Arc<KafkaProxyInner>,
    pub(crate) config: KafkaConfig,
    pub(crate) slot: usize,
    /// owner 已认领或源所有者已发布的永久停止意图；跨会话保留，不能被健康刷新或自动重建撤销。
    pub(crate) stop_requested: Arc<AtomicBool>,
    /// 实际业务执行额度随 owner 保持，不因单次超时或业务结果发布而提前释放。
    pub(crate) handlers: super::worker::HandlerPool,
    /// 本槽输入身份持有全组责任额度，实际提交确认前不因队列阶段变化归还。
    pub(crate) records: Arc<super::capacity::RecordLedger>,
    /// 分配范围与失败记录元数据由全部槽共享，不按并发数切分或放大。
    pub(crate) metadata: Arc<super::metadata_capacity::MetadataCapacity>,
    gauges: Arc<Mutex<Vec<Option<GroupHealth>>>>,
}
impl std::ops::Deref for OwnerRuntime {
    type Target = KafkaProxyInner;
    /// 业务作用：复用源的发布、遥测和关闭资源，不为消费槽创建独立源。
    /// 参数说明：无。
    /// 返回：源资源借用；消费输入和执行额度由全组共享凭证裁决。
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl OwnerRuntime {
    /// 业务作用：将每槽身份与恢复范围绑定到共享输入、执行池，防止增加 owner 时放大实际责任上限。
    /// 参数说明：inner 为源；slot/count 为稳定槽序号和组内数量，gauges 为同组采样槽，handlers 为组共享执行入口，capacity/metadata 为全组输入与元数据池。
    /// 返回：有效的槽配置；原生身份配置不合法时在创建线程前拒绝。
    fn new(
        inner: Arc<KafkaProxyInner>,
        slot: usize,
        count: usize,
        gauges: Arc<Mutex<Vec<Option<GroupHealth>>>>,
        handlers: super::worker::HandlerPool,
        capacity: Arc<super::capacity::GroupCapacity>,
        metadata: Arc<super::metadata_capacity::MetadataCapacity>,
    ) -> Result<Arc<Self>> {
        let mut config = inner.config.clone();
        if count > 1 {
            // 身份后缀在全部强类型配置冻结后生成，静态成员不能让多个槽互相驱逐。
            config.client_id_prefix = Some(format!(
                "{}-slot-{slot}",
                config.client_id_prefix.as_deref().unwrap_or("nafka")
            ));
            if let Some(instance) = config
                .consumer
                .properties
                .get("group.instance.id")
                .or_else(|| config.properties.get("group.instance.id"))
                .cloned()
            {
                config.consumer.properties.insert(
                    "group.instance.id".into(),
                    format!("{instance}-slot-{slot}"),
                );
            }
            config.validate()?;
        }
        Ok(Arc::new(Self {
            inner,
            handlers,
            records: Arc::new(super::capacity::RecordLedger::new(capacity)),
            metadata,
            config,
            slot,
            stop_requested: Arc::new(AtomicBool::new(false)),
            gauges,
        }))
    }
    /// 业务作用：把各槽最近采样合成组指标，防止多个 owner 相互覆盖同一 gauge。
    /// 参数说明：group 为最终组名；snapshot 为本槽已脱离健康锁的观测。
    /// 返回：发布组累计数量和全槽 Ready；指标仅供观测，不构成分区租约。
    pub(crate) fn publish_health(&self, group: &str, mut snapshot: GroupHealth) {
        (snapshot.retained_records, snapshot.retained_bytes) = self.records.usage();
        // 逻辑指标统计完整输入责任；公共健康字段仍保留按键完成证据的原有诊断含义。
        (snapshot.key_pending_records, snapshot.key_pending_bytes) = self.records.key_usage();
        // 交付完成可能刷新内部诊断状态，但不能把已受理停止的槽重新公布为 Ready。
        if self.stop_requested.load(Ordering::Acquire) {
            snapshot.ready_assignment_epoch = None;
        }
        let values = {
            let mut slots = self
                .gauges
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            slots[self.slot] = Some(snapshot);
            let mut values = [0i64; 9];
            values[0] = 1;
            let mut assignment = BTreeSet::new();
            for slot in slots.iter() {
                let Some(slot) = slot else {
                    values[0] = 0;
                    continue;
                };
                if slot.ready_assignment_epoch.is_none() || slot.state != GroupState::Running {
                    values[0] = 0;
                }
                for tp in &slot.assignment {
                    if !assignment.insert(tp) {
                        values[0] = 0;
                    }
                }
                for (index, count) in [
                    slot.assignment.len(),
                    slot.paused
                        .iter()
                        .map(|(tp, _)| tp)
                        .collect::<BTreeSet<_>>()
                        .len(),
                    slot.active_handlers,
                    slot.key_active_handlers,
                    slot.key_pending_records,
                    slot.key_pending_bytes,
                ]
                .into_iter()
                .enumerate()
                {
                    values[index + 1] =
                        values[index + 1].saturating_add(i64::try_from(count).unwrap_or(i64::MAX));
                }
            }
            let (records, bytes) = self.records.group_usage();
            values[7] = i64::try_from(records).unwrap_or(i64::MAX);
            values[8] = i64::try_from(bytes).unwrap_or(i64::MAX);
            values
        };
        // 聚合锁只保护槽快照，释放后再进行有界观测准入；用户代码只在导出线程运行。
        for (name, value) in [
            "group_ready",
            "assigned_partitions",
            "paused_partitions",
            "active_handlers",
            "key_active_handlers",
            "key_pending_records",
            "key_pending_bytes",
            "retained_records",
            "retained_bytes",
        ]
        .into_iter()
        .zip(values)
        {
            self.inner.gauge(name, value, &[("group", group)]);
        }
    }
}

/// 同一最终 group.id 的控制入口；槽本身继续独占 native consumer。
pub(crate) struct GroupHandle {
    owners: Vec<Arc<OwnerHandle>>,
    route_count: usize,
    capacity: Arc<super::capacity::GroupCapacity>,
    controls: tokio::sync::Mutex<()>,
    control_timeout: Duration,
    epoch: Mutex<(Vec<Option<u64>>, u64)>,
}
impl GroupHandle {
    /// 业务作用：提供冻结路由三元组数量，owner并发不会放大注册规模。
    /// 参数说明：无。返回：该组实际登记的路由数。
    pub(crate) fn route_count(&self) -> usize {
        self.route_count
    }

    /// 业务作用：合并组内实际 owner 关闭责任，不持有或复制业务原文。
    /// 参数说明：无。
    /// 返回：所有槽的数值汇总；停止后的未核实交回不会被健康归零覆盖。
    pub(crate) fn close_report(&self) -> crate::CloseConsumers {
        let mut report = crate::CloseConsumers::default();
        for owner in &self.owners {
            report.add(&owner.close_report());
        }
        report
    }
    /// 业务作用：冻结全部目标归属，并在共同预算内逐目标保留执行证据。
    /// 参数说明：actions 为已验证的显式分区意图。
    /// 返回：每个目标都有结局；缺少唯一归属时整批不派发，未知结果后停止派发后继。
    pub(crate) async fn control_partitions(
        &self,
        actions: BTreeMap<Tp, crate::PartitionControl>,
    ) -> BTreeMap<Tp, crate::ControlOutcome> {
        use crate::ControlOutcome;
        let deadline = Instant::now() + self.control_timeout;
        let mut outcomes = actions
            .keys()
            .cloned()
            .map(|tp| (tp, ControlOutcome::NotApplied))
            .collect::<BTreeMap<_, _>>();
        let Ok(_control) = tokio::time::timeout(self.control_timeout, self.controls.lock()).await
        else {
            return outcomes;
        };
        let health = self.slots_health();
        let mut routed = BTreeMap::new();
        for tp in actions.keys() {
            let candidates = health
                .iter()
                .enumerate()
                .filter(|(_, slot)| {
                    slot.ready_assignment_epoch.is_some() && slot.assignment.contains(tp)
                })
                .collect::<Vec<_>>();
            if candidates.len() == 1 {
                routed.insert(
                    tp.clone(),
                    (candidates[0].0, candidates[0].1.ready_assignment_epoch),
                );
            } else {
                outcomes.insert(tp.clone(), ControlOutcome::OwnerChanged);
            }
        }
        // 在任何副作用之前完成全部目标的归属门禁，不能把缺席目标变成部分已生效操作。
        if routed.len() != actions.len() {
            return outcomes;
        }
        for (tp, action) in actions {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let (slot, epoch) = routed[&tp];
            // 始终携带调用开始时的 owner 代次，排队或前序等待不能授权新分配。
            let result = self.owners[slot]
                .command_at(action.command(tp.clone()), remaining, epoch)
                .await;
            let outcome = match result {
                Ok(_) => ControlOutcome::Applied,
                Err(NafkaError::ControlOwnerChanged(_)) => ControlOutcome::OwnerChanged,
                Err(
                    NafkaError::ControlNotApplied(_)
                    | NafkaError::CommandQueueFull(_)
                    | NafkaError::NoSuchGroup(_),
                ) => ControlOutcome::NotApplied,
                Err(_) => ControlOutcome::Unknown,
            };
            outcomes.insert(tp, outcome);
            // 已开始且无终态的命令仍可能在 owner 内运行，不能继续叠加后继副作用。
            if outcome == ControlOutcome::Unknown {
                break;
            }
        }
        outcomes
    }

    /// 业务作用：在共同启动门后构造全部槽，只有全部本地构造成功才允许上层开放消费。
    /// 参数说明：runtime 为源；plan 为冻结路由；startup_gate 为本批次共同准入门。
    /// 返回：组句柄与汇总构造结果；失败时关闭门并等待本次已创建槽退出。
    pub(crate) async fn start(
        runtime: Arc<KafkaProxyInner>,
        plan: GroupPlan,
        startup_gate: Arc<StartupGate>,
    ) -> Result<(Arc<Self>, oneshot::Receiver<Result<()>>)> {
        let count = if matches!(plan.mode, OwnerMode::Assign { .. }) {
            1
        } else {
            runtime
                .config
                .consumer
                .group_concurrency
                .get(&plan.group)
                .copied()
                .unwrap_or(runtime.config.consumer.concurrency)
        };
        let gauges = Arc::new(Mutex::new(vec![None; count]));
        let handlers =
            super::worker::HandlerPool::new(runtime.config.behavior.max_in_flight_handlers);
        let capacity = super::capacity::GroupCapacity::new(
            Arc::clone(&runtime.input_capacity),
            runtime.config.behavior.max_group_progress_records,
            runtime.config.behavior.max_group_pending_bytes,
            runtime.config.behavior.max_progress_records,
            runtime.config.behavior.max_partition_pending_bytes,
            runtime.config.behavior.max_key_pending_records,
            runtime.config.behavior.max_key_pending_bytes,
        );
        let metadata = super::metadata_capacity::MetadataCapacity::new(
            runtime.config.consumer.max_assignment_partitions,
            runtime.config.behavior.max_retry_entries,
        );
        let configurations = (0..count)
            .map(|slot| {
                OwnerRuntime::new(
                    Arc::clone(&runtime),
                    slot,
                    count,
                    Arc::clone(&gauges),
                    handlers.owner(),
                    Arc::clone(&capacity),
                    Arc::clone(&metadata),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let mut owners = Vec::new();
        let mut results = Vec::new();
        for config in configurations {
            match OwnerHandle::start(config, plan.clone(), Arc::clone(&startup_gate)) {
                Ok((owner, result)) => {
                    owners.push(owner);
                    results.push(result);
                }
                Err(error) => {
                    // 任一槽构造失败时整体保持关闭，已创建槽不能留下未登记的消费副作用。
                    startup_gate.abort();
                    let deadline = Instant::now()
                        + Duration::from_millis(runtime.config.behavior.shutdown_timeout_ms);
                    futures::future::join_all(
                        owners.iter().map(|owner| owner.stop_until(deadline)),
                    )
                    .await;
                    return Err(error);
                }
            }
        }
        let route_count = plan
            .typed
            .iter()
            .map(|route| route.meta().topics.len())
            .sum::<usize>()
            + plan
                .passthrough
                .iter()
                .map(|route| route.meta.topics.len())
                .sum::<usize>();
        let handle = Arc::new(Self {
            route_count,
            owners,
            capacity,
            controls: tokio::sync::Mutex::new(()),
            control_timeout: Duration::from_millis(runtime.config.behavior.control_timeout_ms),
            epoch: Mutex::new((Vec::new(), 0)),
        });
        let (reply, result) = oneshot::channel();
        runtime.runtime.spawn(async move {
            let results = futures::future::join_all(results).await;
            let error = results.into_iter().find_map(|value| match value {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(_) => Some(NafkaError::Lifecycle("owner 在本地构造期间退出".into())),
            });
            let _ = reply.send(error.map_or(Ok(()), Err));
        });
        Ok((handle, result))
    }

    /// 业务作用：按稳定槽顺序返回独立健康观测，空 assignment 槽仍保留诊断。
    /// 参数说明：无。
    /// 返回：下标即槽号的快照；快照不是跨槽原子租约。
    pub(crate) fn slots_health(&self) -> Vec<GroupHealth> {
        self.owners.iter().map(|owner| owner.health()).collect()
    }

    /// 业务作用：聚合组内状态与资源计数，允许已 join 的空闲槽满足组级 Ready。
    /// 参数说明：无。
    /// 返回：任一槽失权或观测到重复归属时撤销 Ready；不同槽的计数相加。
    pub(crate) fn health(&self) -> GroupHealth {
        let slots = self.slots_health();
        if slots.len() == 1 {
            return slots[0].clone();
        }
        let mut combined = slots[0].clone();
        combined.assignment.clear();
        combined.paused.clear();
        combined.owner_session_started = 0;
        combined.owner_session_restarted = 0;
        combined.commit_attempted = 0;
        combined.commit_succeeded = 0;
        combined.commit_failed = 0;
        combined.active_handlers = 0;
        combined.key_active_handlers = 0;
        combined.key_pending_records = 0;
        combined.key_pending_bytes = 0;
        combined.last_poll_at = slots.iter().map(|slot| slot.last_poll_at).min().flatten();
        combined.last_success_at = slots.iter().filter_map(|slot| slot.last_success_at).max();
        combined.last_assignment_at = slots
            .iter()
            .filter_map(|slot| slot.last_assignment_at)
            .max();
        combined.state = slots
            .iter()
            .max_by_key(|slot| state_rank(slot.state))
            .expect("组至少一个槽")
            .state;
        combined.last_error = slots.iter().find_map(|slot| slot.last_error.clone());
        let mut assignment = BTreeSet::new();
        let mut unique = true;
        for slot in &slots {
            for tp in &slot.assignment {
                unique &= assignment.insert(tp.clone());
            }
            combined.paused.extend(slot.paused.clone());
            combined.owner_session_started = combined
                .owner_session_started
                .saturating_add(slot.owner_session_started);
            combined.owner_session_restarted = combined
                .owner_session_restarted
                .saturating_add(slot.owner_session_restarted);
            combined.commit_attempted = combined
                .commit_attempted
                .saturating_add(slot.commit_attempted);
            combined.commit_succeeded = combined
                .commit_succeeded
                .saturating_add(slot.commit_succeeded);
            combined.commit_failed = combined.commit_failed.saturating_add(slot.commit_failed);
            combined.active_handlers += slot.active_handlers;
            combined.key_active_handlers += slot.key_active_handlers;
            combined.key_pending_records += slot.key_pending_records;
            combined.key_pending_bytes += slot.key_pending_bytes;
        }
        // 输入额度从共享账本一次读取，不能把归还前的旧槽快照与借额后的新槽快照相加。
        (combined.retained_records, combined.retained_bytes) = self.capacity.usage();
        combined.assignment = assignment.into_iter().collect();
        let signature = slots
            .iter()
            .map(|slot| slot.ready_assignment_epoch)
            .collect::<Vec<_>>();
        let mut epoch = self
            .epoch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if epoch.0 != signature {
            epoch.0 = signature;
            epoch.1 = epoch.1.saturating_add(1);
        }
        // 各槽 epoch 不能取最大值或求和当作组权威；必须观察整个向量的变化。
        combined.ready_assignment_epoch =
            (unique && epoch.0.iter().all(Option::is_some)).then_some(epoch.1);
        combined
    }

    /// 业务作用：按当前归属拆分分区控制，广播组控制并在每个 owner 内复验入队代次。
    /// 参数说明：kind 为调用方完整操作意图。
    /// 返回：全部槽成功才成功；多槽操作部分失败统一报告结果未知，不声称跨 consumer 原子。
    pub(crate) async fn command(&self, kind: OwnerCommandKind) -> Result<CommandReply> {
        let _control = tokio::time::timeout(self.control_timeout, self.controls.lock())
            .await
            .map_err(|_| NafkaError::ControlNotApplied(0))?;
        if self.owners.len() == 1 {
            return self.owners[0].command(kind).await;
        }
        let health = self.slots_health();
        let targets = partition_targets(&kind);
        let mut routed = BTreeMap::<usize, Vec<Tp>>::new();
        if let Some(targets) = targets.filter(|targets| !targets.is_empty()) {
            // 先解析全部目标再入队，缺席或双重归属不能造成前几个槽已经执行的半完成状态。
            for tp in targets {
                let owners = health
                    .iter()
                    .enumerate()
                    .filter(|(_, slot)| {
                        slot.ready_assignment_epoch.is_some() && slot.assignment.contains(&tp)
                    })
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>();
                if owners.len() != 1 {
                    return Err(NafkaError::Lifecycle("分区归属正在变化或尚未分配".into()));
                }
                routed.entry(owners[0]).or_default().push(tp);
            }
        } else {
            for (index, slot) in health.iter().enumerate() {
                if !matches!(kind, OwnerCommandKind::Restart)
                    || slot.state.permits_operator_restart()
                {
                    routed.insert(index, Vec::new());
                }
            }
        }
        if routed.is_empty() {
            return Err(NafkaError::Lifecycle("没有允许该操作的 owner".into()));
        }
        let replies =
            futures::future::join_all(routed.into_iter().map(|(index, targets)| {
                self.owners[index].command(select_targets(&kind, targets))
            }))
            .await;
        let mut positions = BTreeMap::new();
        let mut assignment = BTreeSet::new();
        for reply in replies {
            match reply.map_err(|_| NafkaError::ControlOutcomeUnknown(0))? {
                CommandReply::Unit => {}
                CommandReply::Positions(found) => positions.extend(found),
                CommandReply::Assignment(found) => {
                    for tp in found {
                        if !assignment.insert(tp) {
                            return Err(NafkaError::Lifecycle("assignment 正在转移".into()));
                        }
                    }
                }
            }
        }
        Ok(match kind {
            OwnerCommandKind::Position { .. } => CommandReply::Positions(positions),
            OwnerCommandKind::Assignment => {
                CommandReply::Assignment(assignment.into_iter().collect())
            }
            _ => CommandReply::Unit,
        })
    }

    /// 业务作用：在共同 deadline 内同时停止所有槽，单槽超时不阻止其它槽收口。
    /// 参数说明：deadline 为整个关闭流程的绝对期限。
    /// 返回：全部实际退出才成功；任一未退出仍保留本组句柄。
    pub(crate) async fn stop_until(&self, deadline: Instant) -> Result<()> {
        let results =
            futures::future::join_all(self.owners.iter().map(|owner| owner.stop_until(deadline)))
                .await;
        results.into_iter().collect::<Result<Vec<_>>>().map(|_| ())
    }
    /// 业务作用：释放外层运行句柄时向每槽发送非阻塞停止通知。
    /// 参数说明：无。
    /// 返回：全部槽接受通知时为 true；正式关闭仍须等待 stop_until。
    pub(crate) fn request_stop_best_effort(&self) -> bool {
        self.owners
            .iter()
            .map(|owner| owner.request_stop_best_effort())
            .fold(true, |all, accepted| all & accepted)
    }

    /// 业务作用：向全部槽同步发布源退出的永久停止意图，避免单槽等待阻止其它槽失权。
    /// 参数说明：无。
    /// 返回：只撤销准入并通知，不清除责任或等待线程退出。
    pub(crate) fn request_permanent_stop(&self) {
        for owner in &self.owners {
            owner.request_permanent_stop();
        }
    }
}

/// 业务作用：定义聚合状态的保护优先级，避免健康槽掩盖已停止或失败的成员。
/// 参数说明：state 为单槽状态。
/// 返回：优先级数值，越大越优先对外呈现。
fn state_rank(state: GroupState) -> u8 {
    match state {
        GroupState::Running => 0,
        GroupState::Retrying => 1,
        GroupState::ProgressBackpressure => 2,
        GroupState::DltBackpressure => 3,
        GroupState::CommitBlocked => 4,
        GroupState::Degraded => 5,
        GroupState::Starting => 6,
        GroupState::Joining => 7,
        GroupState::Recovering => 8,
        GroupState::Stopped => 9,
        GroupState::Stopping => 10,
        GroupState::Crashed => 11,
    }
}
/// 业务作用：提取需要按 assignment 路由的命令范围。
/// 参数说明：kind 为完整命令。
/// 返回：Some 空集合表示所有当前分区，None 表示非分区命令。
pub(crate) fn partition_targets(kind: &OwnerCommandKind) -> Option<Vec<Tp>> {
    match kind {
        OwnerCommandKind::Seek { offsets } => Some(offsets.keys().cloned().collect()),
        OwnerCommandKind::SeekTimestamp { times } => Some(times.keys().cloned().collect()),
        OwnerCommandKind::SeekBeginning { tps }
        | OwnerCommandKind::SeekEnd { tps }
        | OwnerCommandKind::SeekCommitted { tps }
        | OwnerCommandKind::Pause { tps }
        | OwnerCommandKind::Resume { tps }
        | OwnerCommandKind::Position { tps } => Some(tps.clone()),
        _ => None,
    }
}
/// 业务作用：生成单槽负责的子命令，保持原操作与所有目标值。
/// 参数说明：kind 为完整操作；targets 为本槽分区，空时保持广播语义。
/// 返回：不持有其它槽分区的独立命令。
fn select_targets(kind: &OwnerCommandKind, targets: Vec<Tp>) -> OwnerCommandKind {
    if targets.is_empty() {
        return kind.clone();
    }
    match kind {
        OwnerCommandKind::Seek { offsets } => OwnerCommandKind::Seek {
            offsets: targets
                .into_iter()
                .map(|tp| {
                    let value = offsets[&tp];
                    (tp, value)
                })
                .collect(),
        },
        OwnerCommandKind::SeekTimestamp { times } => OwnerCommandKind::SeekTimestamp {
            times: targets
                .into_iter()
                .map(|tp| {
                    let value = times[&tp];
                    (tp, value)
                })
                .collect(),
        },
        OwnerCommandKind::SeekBeginning { .. } => OwnerCommandKind::SeekBeginning { tps: targets },
        OwnerCommandKind::SeekEnd { .. } => OwnerCommandKind::SeekEnd { tps: targets },
        OwnerCommandKind::SeekCommitted { .. } => OwnerCommandKind::SeekCommitted { tps: targets },
        OwnerCommandKind::Pause { .. } => OwnerCommandKind::Pause { tps: targets },
        OwnerCommandKind::Resume { .. } => OwnerCommandKind::Resume { tps: targets },
        OwnerCommandKind::Position { .. } => OwnerCommandKind::Position { tps: targets },
        _ => kind.clone(),
    }
}
