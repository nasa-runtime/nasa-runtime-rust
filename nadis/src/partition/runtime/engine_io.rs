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
    ///
    /// 参数说明: 无。
    ///
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
///
/// 参数说明：
/// - `state`: 在账本锁内读取的记录、业务键队列及来源权威。
/// - `domain`: 本监督者负责的执行域编号，只选择该域的记录。
///
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
                    kind: IoKind::Commit {
                        unknown,
                        confirmed,
                    },
                });
            }
        }
    }
    for (coordinate, record) in ordered {
        // 停止业务准入的来源仍可能提交既有结果，但不能再认领重投或毒消息隔离动作。
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
                    // 在途业务、不同队头或未完成提交仍占有同一顺序域，重投不得越过这些责任。
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
    ///
    /// 参数说明：
    /// - `action`: 账本生成的 I/O 快照，携带原始来源任期、记录坐标、revision 和动作类型。
    ///
    /// 返回：无返回值；按确定性结果更新仍匹配的记录责任，过期快照不修改后继记录。
    /// 未获容量或组已释放时不发送请求；网络结果未知时保留重试责任，不重放已成功的 handler。
    pub(super) async fn run_io(&self, action: IoAction) {
        // 组已经释放时没有可持续持有的 Redis 与调度资源，不能凭孤立票据启动新 I/O。
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
            // I/O 登记与账本预算同锁发布，恢复通知不能在已准备处置却尚未计入 I/O 的间隙解除保护。
            action.source.io.fetch_add(1, Ordering::AcqRel);
        }
        let mut lease = IoLease {
            core: self,
            source: action.source.clone(),
            bytes: reserved_bytes,
        };
        let result = match &action.kind {
            IoKind::Commit {
                unknown,
                confirmed,
            } => commit(&rt, &action, *unknown, *confirmed).await,
            IoKind::Retry {
                operation,
                executed,
                attempts,
            } => retry(self, &rt, &action, operation, *executed, *attempts).await,
            IoKind::Poison {
                attempts,
            } => {
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
                // 协议或解码证据损坏时关闭该来源，避免持续请求把不确定记录当成合法消息推进。
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
        // 网络等待期间记录可能已换任期或推进 revision，旧结果不能覆盖新的责任持有者。
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
                            if let RecordOwner::Retry {
                                next,
                                ..
                            } = &mut record.owner
                            {
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
                // 隔离修订与 Parked 责任在同一账本锁下发布，较早的 Redis 复验不能清除这次隔离。
                if action
                    .source
                    .park_revision
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |revision| {
                        revision.checked_add(1)
                    })
                    .is_err()
                {
                    // 修订不能回绕复用；无法分辨先后时关闭来源，保留恢复记录给下一任期。
                    action.source.lose();
                }
                // Park 已将原 PEL 转存并确认，但仍保留本地顺序头；提前退账会让同 key 后继越过隔离责任。
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
                    RecordOwner::Retry {
                        next,
                        ..
                    }
                    | RecordOwner::Unroutable {
                        next,
                        ..
                    } => *next = Instant::now() + Duration::from_millis(200),
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
///
/// 参数说明：
/// - `rt`: 原记录所属的物理组及 Redis、锁资源。
/// - `action`: 原始提交坐标与消息来源任期，用于复验 PEL 和持有权。
/// - `_unknown`: 上次提交结果是否未知；当前两种情况均重新读取精确 PEL，不据此跳过校验。
/// - `confirmed`: 源记录已经确认，只需继续交接正文删除责任。
///
/// 返回：已确认或 PEL 消失时返回 Confirmed；来源或 PEL 持有者变化时返回 Moved。
/// 锁状态或 ACK 结果不能确定时返回 Unknown；读取失败或不合法协议结果返回错误。
/// 两种模式都在 ACK 脚本内复验 holder，启用 fencing 时还要求原分区任期完整一致。
async fn commit(
    rt: &GroupRuntime,
    action: &IoAction,
    _unknown: bool,
    confirmed: bool,
) -> Result<IoResult> {
    if confirmed {
        return Ok(IoResult::Confirmed);
    }
    // 正常排干仍可确认既有结果，失权或释放中的来源不得再提交原消息。
    if !action.source.can_commit() {
        return Ok(IoResult::Moved);
    }
    let source = &action.source;
    let id = action.coordinate.identity.id.as_ref();
    {
        // 不再属于本消费者的 PEL 责任由其新持有者推进，旧业务结果不能替它执行确认。
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
    let count = match (&rt.fence_meta, &source.stamp) {
        (Some(meta), Some(stamp)) if stamp.p == source.partition && stamp.epoch == meta.epoch => {
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
                    // 服务端已拒绝原任期，立即关闭该来源并保留 PEL 给当前持有者。
                    source.lose();
                    return Ok(IoResult::Moved);
                }
            }
        }
        (None, None) => {
            // 先前持锁检查的成功回包可能已经过时，Legacy ACK 必须与 holder 复验原子执行。
            const ACK_LUA: &str = r#"
local lock_type = redis.call('TYPE', KEYS[1])['ok']
local stream_type = redis.call('TYPE', KEYS[2])['ok']
if lock_type ~= 'none' and lock_type ~= 'hash' then
    return {'PROTOCOL', 0}
end
if stream_type ~= 'none' and stream_type ~= 'stream' then
    return {'PROTOCOL', 0}
end
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 then
    return {'NOT_HOLDER', 0}
end
return {'OK', redis.call('XACK', KEYS[2], ARGV[2], ARGV[3])}
"#;
            let (status, count): (String, i64) = redis::Script::new(ACK_LUA)
                .key(&source.lock_key)
                .key(&*action.coordinate.identity.stream)
                .arg(&source.holder)
                .arg(&*action.coordinate.group)
                .arg(id)
                .invoke_async(&mut rt.client.conn())
                .await?;
            match status.as_str() {
                "OK" => count,
                "NOT_HOLDER" => {
                    // 原 holder 已失权，不能在该来源继续业务或重试确认。
                    source.lose();
                    return Ok(IoResult::Moved);
                }
                _ => {
                    return Err(NasaRedisError::ProtocolMarker(
                        "Legacy ACK 键类型或响应标签不合法".into(),
                    ));
                }
            }
        }
        _ => {
            // fencing 证据缺失或坐标不一致不能降为仅 holder 校验，原责任交当前持有者恢复。
            source.lose();
            return Ok(IoResult::Moved);
        }
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
///
/// 参数说明：
/// - `core`: 保存原票据与已捕获重投意图的共享账本。
/// - `rt`: 提供 Redis、分区策略及锁操作的物理组。
/// - `action`: 原始来源、坐标及 revision 的 I/O 快照。
/// - `operation`: 调度时保存的重放身份，结合来源任期与 revision 派生持久操作键。
/// - `executed`: 是否已经尝试过业务或畸形消息处置；未尝试时只重新读取正文，不增加投递计数。
/// - `attempts`: 本地记录的失败次数，与精确 PEL 投递次数共同判断是否超出重试上限。
///
/// 返回：正文回到统一 dispatcher，毒消息仅处置本记录，未知结果继续保留原票据；
/// 来源凭据与当前组身份不一致时在读取 PEL 或登记意图前返回协议错误。
async fn retry(
    core: &RedisPartitionRuntime,
    rt: &GroupRuntime,
    action: &IoAction,
    operation: &str,
    executed: bool,
    attempts: u32,
) -> Result<IoResult> {
    let source = &action.source;
    // 重投会重新获得业务执行机会，只有仍开放根业务的原来源才能推进。
    if !source.can_start() {
        return Ok(IoResult::Moved);
    }
    // 重投次数也是持久副作用，不能将其它分区或初始化 epoch 的计数拼成当前来源权威。
    let fence_epoch = if let Some(meta) = rt.fence_meta.as_ref() {
        let Some(stamp) = source.stamp.as_ref() else {
            return Err(NasaRedisError::ProtocolMarker(
                "retry 来源缺少 fencing 凭据".into(),
            ));
        };
        if stamp.p != source.partition
            || stamp.epoch != meta.epoch
            || stamp.counter == 0
            || stamp.counter > i64::MAX as u64
        {
            return Err(NasaRedisError::ProtocolMarker(
                "retry 来源 fencing 身份不一致".into(),
            ));
        }
        stamp.epoch
    } else {
        0
    };
    let id = action.coordinate.identity.id.as_ref();
    // PEL 已消失或交给其它消费者时，旧票据只能退账，不能重新制造一条投递。
    let Some(pending) = wire::exact_pending(rt, source.partition, id).await? else {
        return Ok(IoResult::Moved);
    };
    if pending.consumer != rt.node_id {
        return Ok(IoResult::Moved);
    }
    // 持锁证据不确定时保留原票据退避，不能把暂时读失败解释为获得处置权限。
    if rt.lock.holds_status(&source.lock_key, &source.holder).await != HoldStatus::Held
        || !source.can_start()
    {
        return Ok(IoResult::Retry);
    }
    let intent = {
        let state = core.state.lock().expect("ledger");
        // 网络查询可能晚于记录迁移；只从同一 revision 和任期读取原重投意图。
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
        epoch: fence_epoch,
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
///
/// 参数说明：
/// - `rt`: 执行隔离策略的物理消费组。
/// - `action`: 超限记录及其原始 owner 权威。
///
/// 返回：Drop 生成提交、Park 冻结物理来源、DLQ 完成后清理本地责任；任期不完整时拒绝，未知副作用保留重试。
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
    // 只能用原消息来源的任期授权 ACK；后来 owner_ctx 的新凭据不能接替旧记录的处置权限。
    let (fence_key, round, nonce, counter) = match (&rt.fence_meta, &source.stamp) {
        (Some(meta), Some(stamp)) if stamp.p == source.partition && stamp.epoch == meta.epoch => (
            fencing::fence_key(&rt.layout),
            meta.round,
            meta.nonce.as_str(),
            stamp.counter,
        ),
        (None, None) => (String::new(), 0, "", 0),
        _ => {
            // 缺失或不一致的 stamp 不能降为仅 holder 校验，否则会绕过启用中的 fencing。
            return Err(NasaRedisError::OwnershipChanged(format!(
                "分区 {} 的 Park 来源任期不完整或坐标不一致",
                source.partition
            )));
        }
    };
    let lease = disposition::OwnerLease {
        partition: source.partition,
        operation_id: &action.coordinate.identity.id,
        expected_park_id: None,
        holder: &source.holder,
        lock_key: &source.lock_key,
        fence_key: &fence_key,
        round,
        nonce,
        counter,
    };
    let park_id =
        disposition::park(&rt.client, &rt.layout, source.partition, &lease, &records).await?;
    // 已完成 Park 不延续本地业务权限；等待期间失权时不再发起 DLQ 转换。
    if !source.can_start() {
        return Ok(IoResult::Moved);
    }
    if matches!(rt.cfg.poison_policy, PoisonPolicy::Park) {
        return Ok(IoResult::Parked);
    }
    let operation = disposition::auto_op_id(source.partition, &park_id);
    // Park 的成功回包可能晚于下一批隔离事实，自动 DLQ 必须保留原来源任期及本次保存的批次。
    let continuation = disposition::OwnerLease {
        operation_id: &operation,
        expected_park_id: Some(&park_id),
        ..lease
    };
    match disposition::dlq_from_parked(&rt.client, &rt.layout, source.partition, &continuation)
        .await
    {
        Ok(_) => Ok(IoResult::Confirmed),
        Err(_) => Ok(IoResult::Parked),
    }
}
