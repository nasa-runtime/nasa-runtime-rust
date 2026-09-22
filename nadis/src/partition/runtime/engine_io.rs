//! 每个执行域单飞推进提交与精确重投，共享账本保持跨域业务键顺序。

use super::super::{disposition, retryop, wire};
use super::*;

pub(super) struct IoAction {
    coordinate: Coordinate,
    pub(super) source: Arc<SourceAuthority>,
    revision: u64,
    kind: IoKind,
}

enum IoKind {
    Commit {
        unknown: bool,
        confirmed: bool,
    },
    Retry {
        operation: String,
        executed: bool,
        attempts: u32,
    },
    Poison {
        attempts: u32,
    },
}
enum IoResult {
    Confirmed,
    Unknown,
    Moved,
    Body(Vec<u8>),
    Tombstone,
    Blocked,
    Parked,
    Retry,
}

struct IoLease<'a> {
    core: &'a RedisPartitionRuntime,
    source: Arc<SourceAuthority>,
    bytes: usize,
}

impl Drop for IoLease<'_> {
    /// 业务作用：I/O 正常结束或显式中止后归还正文预留和来源引用，取消不遗失计数。
    /// 参数说明: 无。
    /// 返回：释放本地 I/O 责任，服务端已发送操作的结果仍由原记录票据归约。
    fn drop(&mut self) {
        let mut state = self.core.state.lock().expect("ledger");
        state.bytes -= self.bytes;
        state.domains[self.source.domain].bytes -= self.bytes;
        self.source.io.fetch_sub(1, Ordering::AcqRel);
        self.core.changed.notify_waiters();
    }
}

/// 业务作用：从账本选择一个已到期提交或精确重试，提交优先且同 key 重试必须等待全部提交收口。
/// 参数说明：`state` 为一致账本视图；`domain` 为本监督者唯一负责的执行域。
/// 返回：携带坐标与 revision 的唯一 I/O 快照；无合法动作时返回 None。
pub(super) fn next_io(state: &State, domain: usize) -> Option<IoAction> {
    let now = Instant::now();
    let mut ordered: Vec<_> = state
        .records
        .iter()
        .filter(|(_, record)| record.source.domain == domain)
        .collect();
    ordered.sort_by(|(left, a), (right, b)| {
        a.batch
            .cmp(&b.batch)
            .then_with(|| stream_id_cmp(&left.identity.id, &right.identity.id))
    });
    for (coordinate, record) in &ordered {
        if let RecordOwner::Commit {
            unknown,
            confirmed,
            next,
        } = record.owner
        {
            if next <= now && record.source.can_commit() {
                return Some(IoAction {
                    coordinate: (*coordinate).clone(),
                    source: record.source.clone(),
                    revision: record.revision,
                    kind: IoKind::Commit { unknown, confirmed },
                });
            }
        }
    }
    for (coordinate, record) in ordered {
        if !record.source.can_start() {
            continue;
        }
        let kind = match &record.owner {
            RecordOwner::Retry {
                next,
                operation,
                executed,
            } if *next <= now => {
                if let Some(key) = record.gate {
                    let gate = state.gates.get(&key)?;
                    let same_legacy_bucket = record.plan.as_ref().is_some_and(|plan| plan.legacy)
                        && gate
                            .queue
                            .front()
                            .and_then(|head| state.records.get(head))
                            .is_some_and(|head| {
                                head.batch == record.batch
                                    && head.source.epoch == record.source.epoch
                            });
                    if gate.executing.is_some()
                        || (gate.queue.front() != Some(coordinate) && !same_legacy_bucket)
                        || gate.queue.iter().any(|c| {
                            state
                                .records
                                .get(c)
                                .is_some_and(|r| matches!(r.owner, RecordOwner::Commit { .. }))
                        })
                    {
                        continue;
                    }
                }
                IoKind::Retry {
                    operation: operation.clone(),
                    executed: *executed,
                    attempts: record.attempts,
                }
            }
            RecordOwner::Unroutable {
                malformed: true,
                next,
            } if *next <= now => IoKind::Poison {
                attempts: record.attempts,
            },
            _ => continue,
        };
        return Some(IoAction {
            coordinate: coordinate.clone(),
            source: record.source.clone(),
            revision: record.revision,
            kind,
        });
    }
    None
}

impl RedisPartitionRuntime {
    /// 业务作用：执行票据的唯一网络步骤，并仅向原始坐标和 revision 发布结论。
    /// 参数说明：`action` 为账本生成且尚未执行的 I/O 快照。
    /// 返回：成功提交、未知提交或精确正文分别归约；网络失败不会重放成功 handler。
    pub(super) async fn run_io(&self, action: IoAction) {
        let Some(rt) = action.source.group.upgrade() else {
            return;
        };
        let reserved_bytes = if matches!(action.kind, IoKind::Commit { .. }) {
            0
        } else {
            self.limits.max_record_bytes
        };
        {
            let mut state = self.state.lock().expect("ledger");
            // 精确重投同样先预留正文容量；未取得预算时不向 Redis 请求消息正文。
            if state
                .bytes
                .checked_add(reserved_bytes)
                .is_none_or(|n| n > self.limits.max_inflight_payload_bytes)
                || state.domains[action.source.domain]
                    .bytes
                    .checked_add(reserved_bytes)
                    .is_none_or(|n| {
                        n > self.executions.domains[action.source.domain]
                            .spec
                            .quota
                            .bytes
                    })
            {
                return;
            }
            state.bytes += reserved_bytes;
            state.domains[action.source.domain].bytes += reserved_bytes;
        }
        action.source.io.fetch_add(1, Ordering::AcqRel);
        let mut lease = IoLease {
            core: self,
            source: action.source.clone(),
            bytes: reserved_bytes,
        };
        let result = match &action.kind {
            IoKind::Commit { unknown, confirmed } => {
                commit(&rt, &action, *unknown, *confirmed).await
            }
            IoKind::Retry {
                operation,
                executed,
                attempts,
            } => retry(self, &rt, &action, operation, *executed, *attempts).await,
            IoKind::Poison { attempts } => {
                retry(
                    self,
                    &rt,
                    &action,
                    &format!(
                        "malformed:{}:{}",
                        action.source.epoch, action.coordinate.identity.id
                    ),
                    true,
                    *attempts,
                )
                .await
            }
        };
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(partition = action.source.partition, error = %error, "分区 I/O 未取得确定结果，保留原责任");
                let invalid = matches!(
                    error,
                    NasaRedisError::Parsing(_)
                        | NasaRedisError::Codec(_)
                        | NasaRedisError::ProtocolMarker(_)
                );
                if invalid {
                    action.source.blocked.store(true, Ordering::Release);
                    action.source.protocol_error.store(true, Ordering::Release);
                }
                if matches!(action.kind, IoKind::Commit { .. }) {
                    IoResult::Unknown
                } else if invalid {
                    IoResult::Blocked
                } else {
                    IoResult::Retry
                }
            }
        };
        // 解码和业务回调可以读取运行状态，必须在账本锁外执行；正文预留一直持有到原子交接。
        let decoded = if let IoResult::Body(body) = &result {
            if body.len() > self.limits.max_record_bytes {
                action.source.oversized.store(true, Ordering::Release);
                Err(DecodeFailure::Internal)
            } else {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let env = serde_json::from_slice::<Envelope>(body)
                        .map_err(|_| DecodeFailure::Malformed)?;
                    let plan = rt
                        .plans
                        .get(&(env.topic.clone(), env.event.clone()))
                        .ok_or(DecodeFailure::Internal)?
                        .clone();
                    let value = (plan.decode)(action.coordinate.identity.clone(), env)?;
                    Ok((plan, value))
                }))
                .unwrap_or(Err(DecodeFailure::Internal))
            }
        } else {
            Err(DecodeFailure::Internal)
        };
        let failure = decoded.as_ref().err().copied();
        // 未被接管的业务对象在账本锁释放后析构，避免析构中的应用逻辑重入账本。
        let mut prepared = decoded.ok();
        let mut retired_payload = None;
        let mut state = self.state.lock().expect("ledger");
        if !state
            .records
            .get(&action.coordinate)
            .is_some_and(|r| r.revision == action.revision && r.source.epoch == action.source.epoch)
        {
            return;
        }
        match result {
            IoResult::Confirmed => {
                // 确认轴先收口；删除队列满时原 Commit 继续持有容量，但不再阻塞同 key 的业务顺序。
                detach_gate(&mut state, &action.coordinate);
                if rt.try_enqueue_async_delete(
                    action.source.partition,
                    &action.coordinate.identity.id,
                ) {
                    retired_payload = retire(self, &mut state, &action.coordinate, true);
                } else {
                    state
                        .records
                        .get_mut(&action.coordinate)
                        .expect("record")
                        .owner = RecordOwner::Commit {
                        unknown: false,
                        confirmed: true,
                        next: Instant::now() + Duration::from_millis(50),
                    };
                }
            }
            IoResult::Moved => {
                retired_payload = retire(self, &mut state, &action.coordinate, false);
            }
            IoResult::Unknown => {
                state
                    .records
                    .get_mut(&action.coordinate)
                    .expect("record")
                    .owner = RecordOwner::Commit {
                    unknown: true,
                    confirmed: false,
                    next: Instant::now() + Duration::from_millis(100),
                };
            }
            IoResult::Tombstone => {
                state
                    .records
                    .get_mut(&action.coordinate)
                    .expect("record")
                    .owner = RecordOwner::Commit {
                    unknown: false,
                    confirmed: false,
                    next: Instant::now(),
                };
            }
            IoResult::Body(body) => {
                match prepared.as_ref() {
                    Some((plan, value)) => {
                        let bytes = body.len().saturating_add(value.weight);
                        let old_gate = state.records.get(&action.coordinate).expect("record").gate;
                        let route = if plan.legacy {
                            Some(napart::RouteHash::from_key(&action.source.epoch))
                        } else {
                            value.route
                        };
                        let new_gate = route.map(|route| GateKey {
                            plan: plan.id,
                            route,
                        });
                        // 重投正文必须仍属于原业务键；路由回调不稳定时禁止从旧队头跳入另一顺序域。
                        if old_gate.is_some() && old_gate != new_gate {
                            action.source.blocked.store(true, Ordering::Release);
                            state
                                .records
                                .get_mut(&action.coordinate)
                                .expect("record")
                                .plan = None;
                            state
                                .records
                                .get_mut(&action.coordinate)
                                .expect("record")
                                .owner = RecordOwner::Unroutable {
                                malformed: false,
                                next: Instant::now(),
                            };
                        } else if body.len().checked_add(value.weight).is_some()
                            && (state.bytes - lease.bytes)
                                .checked_add(bytes)
                                .is_some_and(|total| {
                                    total <= self.limits.max_inflight_payload_bytes
                                })
                            && (state.domains[action.source.domain].bytes - lease.bytes)
                                .checked_add(bytes)
                                .is_some_and(|total| {
                                    total
                                        <= self.executions.domains[action.source.domain]
                                            .spec
                                            .quota
                                            .bytes
                                })
                        {
                            if old_gate.is_none() {
                                if let Some(key) = new_gate {
                                    if !state.gates.contains_key(&key) {
                                        let Some(token) = issue(&mut state) else {
                                            return;
                                        };
                                        state.gates.insert(
                                            key,
                                            OrderedGate {
                                                token,
                                                business: BusinessState::Open,
                                                queue: VecDeque::new(),
                                                executing: None,
                                            },
                                        );
                                    }
                                    state
                                        .gates
                                        .get_mut(&key)
                                        .expect("gate")
                                        .queue
                                        .push_back(action.coordinate.clone());
                                }
                            }
                            // 已预留的正文预算直接转给记录，中间不出现可被其它来源抢占的容量窗口。
                            state.bytes = state.bytes - lease.bytes + bytes;
                            state.domains[action.source.domain].bytes =
                                state.domains[action.source.domain].bytes - lease.bytes + bytes;
                            lease.bytes = 0;
                            let (plan, value) = prepared.take().expect("prepared retry");
                            let record = state.records.get_mut(&action.coordinate).expect("record");
                            record.plan = Some(plan);
                            record.bytes = bytes;
                            record.gate = new_gate;
                            record.owner = RecordOwner::Ready;
                            record.retry_intent = None;
                            state
                                .prepared
                                .insert(action.coordinate.clone(), value.payload);
                        } else {
                            // 扩容失败保留同一重投意图与队头，释放临时对象后退避；不增加 delivery 或 poison 次数。
                            action.source.blocked.store(true, Ordering::Release);
                            let record = state.records.get_mut(&action.coordinate).expect("record");
                            if let RecordOwner::Retry { next, .. } = &mut record.owner {
                                *next = Instant::now() + Duration::from_millis(200);
                            }
                        }
                    }
                    None => {
                        let malformed = matches!(failure, Some(DecodeFailure::Malformed));
                        let record = state.records.get_mut(&action.coordinate).expect("record");
                        record.attempts = record.attempts.saturating_add(1);
                        if let Some(revision) = record.revision.checked_add(1) {
                            record.revision = revision;
                            record.retry_intent = None;
                        } else {
                            record.source.lose();
                        }
                        record.owner = RecordOwner::Unroutable {
                            malformed,
                            next: Instant::now() + Duration::from_millis(200),
                        };
                        if !malformed {
                            record.plan = None;
                            action.source.blocked.store(true, Ordering::Release);
                        }
                    }
                }
            }
            IoResult::Parked => {
                action.source.parked.store(true, Ordering::Release);
                // Park 保留 PEL，也保留本地顺序头；提前退账会让其它物理来源的同 key 后继越过它。
                let record = state.records.get_mut(&action.coordinate).expect("record");
                record.owner = RecordOwner::Parked;
                record.retry_intent = None;
            }
            IoResult::Blocked => {
                action.source.blocked.store(true, Ordering::Release);
                let record = state.records.get_mut(&action.coordinate).expect("record");
                record.plan = None;
                record.owner = RecordOwner::Unroutable {
                    malformed: false,
                    next: Instant::now(),
                };
            }
            IoResult::Retry => {
                match &mut state
                    .records
                    .get_mut(&action.coordinate)
                    .expect("record")
                    .owner
                {
                    RecordOwner::Retry { next, .. } | RecordOwner::Unroutable { next, .. } => {
                        *next = Instant::now() + Duration::from_millis(200)
                    }
                    _ => {}
                }
            }
        }
        let unresolved = state.records.values().any(|r| {
            r.source.epoch == action.source.epoch
                && ((action.source.blocked.load(Ordering::Acquire)
                    && matches!(
                        r.owner,
                        RecordOwner::Commit {
                            confirmed: false,
                            ..
                        }
                    ))
                    || matches!(
                        r.owner,
                        RecordOwner::Unroutable {
                            malformed: true,
                            ..
                        }
                    )
                    || matches!(r.owner, RecordOwner::Unroutable { .. }) && r.plan.is_none())
        });
        if !unresolved && !action.source.parked.load(Ordering::Acquire) {
            for (coordinate, record) in state
                .records
                .iter_mut()
                .filter(|(_, r)| r.source.epoch == action.source.epoch)
            {
                if matches!(
                    record.owner,
                    RecordOwner::Unroutable {
                        malformed: false,
                        ..
                    }
                ) && record.plan.is_some()
                {
                    record.owner = RecordOwner::Retry {
                        next: Instant::now(),
                        operation: format!(
                            "deferred:{}:{}",
                            action.source.epoch, coordinate.identity.id
                        ),
                        executed: false,
                    };
                }
            }
            // 整批保护期间先排干全部已读后缀，再开放新读取，避免新批次越过尚未重建 gate 的旧坐标。
            if !action.source.protocol_error.load(Ordering::Acquire)
                && !action.source.oversized.load(Ordering::Acquire)
                && !state
                    .records
                    .values()
                    .any(|record| record.source.epoch == action.source.epoch)
            {
                action.source.blocked.store(false, Ordering::Release);
            }
        }
        drop(state);
        drop(retired_payload);
        self.changed.notify_waiters();
    }
}

/// 业务作用：只推进已成功记录的提交事实；未知响应通过精确 PEL 查询或幂等重 ACK 收口。
/// 参数说明：`rt` 为组；`action` 为原始提交坐标；`unknown` 表示上次结果未知；`confirmed` 表示仅剩删除交接。
/// 返回：每条记录独立的确认、未知或 owner 转移结论。
async fn commit(
    rt: &GroupRuntime,
    action: &IoAction,
    _unknown: bool,
    confirmed: bool,
) -> Result<IoResult> {
    if confirmed {
        return Ok(IoResult::Confirmed);
    }
    if !action.source.can_commit() {
        return Ok(IoResult::Moved);
    }
    let source = &action.source;
    let id = action.coordinate.identity.id.as_ref();
    {
        match wire::exact_pending(rt, source.partition, id).await? {
            None => return Ok(IoResult::Confirmed),
            Some(row) if row.consumer != rt.node_id => return Ok(IoResult::Moved),
            Some(_) => {}
        }
    }
    // 后继发布窗口仍开放不等于拥有服务端修改权，ACK 前必须再次复验同一任期 holder。
    match rt.lock.holds_status(&source.lock_key, &source.holder).await {
        HoldStatus::Held if source.can_commit() => {}
        HoldStatus::Lost => {
            source.lose();
            return Ok(IoResult::Moved);
        }
        _ => return Ok(IoResult::Unknown),
    }
    let count = if let (Some(meta), Some(stamp)) = (&rt.fence_meta, &source.stamp) {
        match fencing::fenced_ack(
            &rt.client,
            &rt.layout,
            meta,
            stamp,
            &source.lock_key,
            &source.holder,
            &[id.to_string()],
        )
        .await?
        {
            fencing::FencedAck::Acked(n) => n,
            fencing::FencedAck::Rejected(_) => {
                source.lose();
                return Ok(IoResult::Moved);
            }
        }
    } else {
        redis::cmd("XACK")
            .arg(&*action.coordinate.identity.stream)
            .arg(&*action.coordinate.group)
            .arg(id)
            .query_async::<i64>(&mut rt.client.conn())
            .await?
    };
    match count {
        1 => Ok(IoResult::Confirmed),
        0 => match wire::exact_pending(rt, source.partition, id).await? {
            None => Ok(IoResult::Confirmed),
            Some(row) if row.consumer != rt.node_id => Ok(IoResult::Moved),
            _ => Ok(IoResult::Unknown),
        },
        _ => Err(NasaRedisError::Parsing(
            "single record XACK count invalid".into(),
        )),
    }
}

/// 业务作用：按原始坐标查询 PEL、稳定 retry-op 和正文，绝不直接调用业务 handler。
/// 参数说明：`core` 保存同一票据的重投意图；`rt` 为组；`action` 为记录票据；`operation` 为网络重放身份；`executed` 区分失败与从未执行；`attempts` 为本地失败次数。
/// 返回：正文回到统一 dispatcher，毒消息仅处置本记录，未知结果继续保留原票据。
async fn retry(
    core: &RedisPartitionRuntime,
    rt: &GroupRuntime,
    action: &IoAction,
    operation: &str,
    executed: bool,
    attempts: u32,
) -> Result<IoResult> {
    let source = &action.source;
    if !source.can_start() {
        return Ok(IoResult::Moved);
    }
    let id = action.coordinate.identity.id.as_ref();
    let Some(pending) = wire::exact_pending(rt, source.partition, id).await? else {
        return Ok(IoResult::Moved);
    };
    if pending.consumer != rt.node_id {
        return Ok(IoResult::Moved);
    }
    if rt.lock.holds_status(&source.lock_key, &source.holder).await != HoldStatus::Held
        || !source.can_start()
    {
        return Ok(IoResult::Retry);
    }
    let intent = {
        let state = core.state.lock().expect("ledger");
        let Some(record) = state.records.get(&action.coordinate).filter(|record| {
            record.revision == action.revision && record.source.epoch == source.epoch
        }) else {
            return Ok(IoResult::Moved);
        };
        record.retry_intent.clone()
    };
    // 已登记的 CAS 可能已经递增 delivery；重放必须先归约原意图，不能把响应丢失解释为新一次失败。
    if intent.is_none()
        && executed
        && pending.deliveries.max(attempts as u64) > rt.cfg.max_redeliver as u64
    {
        return poison(rt, action).await;
    }
    if !executed {
        return Ok(match wire::exact_body(rt, source.partition, id).await? {
            Some(body) => IoResult::Body(body),
            None => IoResult::Tombstone,
        });
    }
    let ids = [id.to_string()];
    let operation = retryop::operation_id(
        &rt.layout,
        &format!("{}:{}:{operation}", rt.node_id, source.epoch),
        action.revision,
        &ids,
    );
    let intents = if let Some(intent) = intent {
        vec![intent]
    } else {
        let intents =
            retryop::ensure_pending(&rt.client, &rt.layout, source.partition, &operation, &ids)
                .await?;
        if intents.len() != 1 || intents[0].id != id || intents[0].desired == 0 {
            return Err(NasaRedisError::ProtocolMarker(
                "single record retry intent mismatch".into(),
            ));
        }
        // 首次修改 PEL 前冻结原意图；marker 只承担远端持久化，TTL 不控制本地未决票据的寿命。
        let mut state = core.state.lock().expect("ledger");
        let Some(record) = state.records.get_mut(&action.coordinate).filter(|record| {
            record.revision == action.revision && record.source.epoch == source.epoch
        }) else {
            return Ok(IoResult::Moved);
        };
        record.retry_intent = Some(intents[0].clone());
        intents
    };
    // marker 登记跨越网络等待，原任期失效后不得继续执行 XCLAIM。
    if !source.can_start() {
        return Ok(IoResult::Moved);
    }
    let fence_key = rt
        .fence_meta
        .as_ref()
        .map(|_| fencing::fence_key(&rt.layout))
        .unwrap_or_default();
    let fence = retryop::FenceArgs {
        holder: &source.holder,
        lock_key: &source.lock_key,
        fence_key: &fence_key,
        round: rt.fence_meta.as_ref().map_or(0, |m| m.round),
        nonce: rt.fence_meta.as_ref().map_or("", |m| m.nonce.as_str()),
        counter: source.stamp.as_ref().map_or(0, |s| s.counter),
    };
    let outcomes = retryop::execute(
        &rt.client,
        &rt.layout,
        source.partition,
        &rt.node_id,
        &intents,
        &fence,
    )
    .await?;
    let result =
        match outcomes.as_slice() {
            [(
                returned,
                retryop::RetryOutcome::Claimed(body) | retryop::RetryOutcome::Have(body),
            )] if returned == id => IoResult::Body(body.clone()),
            [(returned, retryop::RetryOutcome::Resolved)] if returned == id => {
                // retry-op 的 RESOLVED 不能独自证明 entry 已删除，精确正文仍存在时必须按实际内容处理。
                match wire::exact_body(rt, source.partition, id).await? {
                    Some(body) => IoResult::Body(body),
                    None => IoResult::Tombstone,
                }
            }
            [(
                returned,
                retryop::RetryOutcome::OwnershipChanged | retryop::RetryOutcome::Superseded,
            )] if returned == id => IoResult::Moved,
            _ => IoResult::Blocked,
        };
    // 正文尚未归入本地后继前保留稳定 marker，容量退避或持锁复验不确定不能递增第二次。
    if matches!(result, IoResult::Moved | IoResult::Tombstone) && source.can_commit() {
        let _ = retryop::finish(&rt.client, &rt.layout, source.partition, &operation).await;
    }
    if !source.can_start()
        || rt.lock.holds_status(&source.lock_key, &source.holder).await != HoldStatus::Held
    {
        return Ok(IoResult::Retry);
    }
    Ok(result)
}

/// 业务作用：沿用现有 Park/DLQ 协议隔离一条确证超限记录，Drop 交给独立提交责任。
/// 参数说明：`rt` 为组；`action` 为超限记录与原始 owner 权威。
/// 返回：Drop 生成提交、Park 冻结物理来源、DLQ 完成后清理本地责任；未知副作用保留重试。
async fn poison(rt: &GroupRuntime, action: &IoAction) -> Result<IoResult> {
    use crate::config::PoisonPolicy;
    if matches!(rt.cfg.poison_policy, PoisonPolicy::Drop) {
        return Ok(IoResult::Tombstone);
    }
    let source = &action.source;
    let Some(body) = wire::exact_body(rt, source.partition, &action.coordinate.identity.id).await?
    else {
        return Ok(IoResult::Tombstone);
    };
    let records = [(action.coordinate.identity.id.to_string(), body)];
    // 正文查询不赋予修改权限，Park 前仍必须持有当前来源的业务权威。
    if !source.can_start() {
        return Ok(IoResult::Moved);
    }
    let park_id = disposition::park(
        &rt.client,
        &rt.layout,
        source.partition,
        &source.lock_key,
        &source.holder,
        &records,
    )
    .await?;
    if !source.can_start() {
        return Ok(IoResult::Moved);
    }
    if matches!(rt.cfg.poison_policy, PoisonPolicy::Park) {
        return Ok(IoResult::Parked);
    }
    let operation = disposition::auto_op_id(source.partition, &park_id);
    let Some(fence) = rt.owner_fence(source.partition, operation) else {
        return Ok(IoResult::Parked);
    };
    match disposition::dlq_from_parked(&rt.client, &rt.layout, source.partition, &fence.lease())
        .await
    {
        Ok(_) => Ok(IoResult::Confirmed),
        Err(_) => Ok(IoResult::Parked),
    }
}
