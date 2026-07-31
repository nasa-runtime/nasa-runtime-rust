use super::*;
use crate::config::PoisonPolicy;
use std::collections::HashMap;
use std::time::Duration;

/// RetryBackoff 到期重投:逐 ID `XPENDING 精确查 + XCLAIM 0` 取回 payload(PEL 路径,
/// 不变量 5;ExactTargetRetry 的无 marker 简化版——XCLAIM Unknown 的 retry-op 对账 = R4.2)。
pub(super) async fn retry_redeliver(
    rt: &Arc<GroupRuntime>,
    slots: &mut HashMap<u32, ClaimSlot>,
    p: u32,
    permit: tokio::sync::OwnedSemaphorePermit,
) {
    let Some(slot) = slots.get_mut(&p) else {
        return;
    };
    let ClaimState::RetryBackoff { ids, attempt, .. } = &slot.state else {
        return;
    };
    let (ids, attempt) = (ids.clone(), *attempt);
    // operation_id:按 (sorted ids + group + node_id + claim_term=generation) 派生,
    // 同批重试稳定、跨 dispatch 周期新建 → retry-op marker 按 op_id 键控,消除串扰。
    let claim_term = slot.generation;
    let op_id = super::retryop::operation_id(&rt.layout, &rt.node_id, claim_term, &ids);

    // ── attempt 权威值(R4.2c):意图先落盘(ensure_pending 顺带取回 PEL 真实
    // delivery count = desired-1),阈值比较取 max(本地 attempt, PEL count)——
    // 进程重启/owner 转移后本地计数清零,PEL count 是跨生命周期的事实源。
    let intents =
        match super::retryop::ensure_pending(&rt.client, &rt.layout, p, &op_id, &ids).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(p, err = %e, "retry-op 意图落盘失败,延后");
                slot.state = ClaimState::RetryBackoff {
                    ids,
                    attempt,
                    until: tokio::time::Instant::now() + Duration::from_millis(500),
                };
                drop(permit);
                return;
            }
        };
    if intents.is_empty() {
        // 全部已出 PEL(AlreadyResolved/EntryDeleted)→ 无宗可立,回 Ready
        slot.retry_attempt = 0;
        slot.state = ClaimState::Ready;
        drop(permit);
        return;
    }
    // ── 逐 ID 毒消息判定──
    //   · 不用批次最大 count 连坐(R5);每 ID 独立判定;
    //   · **以实际 PEL delivery count 为准,不信 marker desired**(:损坏 desired 会
    //     绕过 CAS 五规则直接 Drop 正常消息)。actual_count < desired-1 = marker 损坏
    //     (CAS 的 CORRUPT 规则)→ **冻结整分区重试,绝不 Drop/Park/DLQ**,等人工/受控修复。
    let max_redeliver = rt.cfg.max_redeliver;
    let mut poison: Vec<super::retryop::RetryIntent> = Vec::new();
    let mut retryable: Vec<super::retryop::RetryIntent> = Vec::new();
    let mut corrupt = 0u32;
    for it in intents {
        match super::retryop::actual_count(&rt.client, &rt.layout, p, &it.id).await {
            Ok(None) => {} // 已不在 PEL(Resolved):跳过
            Ok(Some(count)) => {
                let floor = it.desired.saturating_sub(1);
                if count < floor {
                    // marker desired 远大于实际 count+1 = 损坏:不处置(留 PEL),告警
                    tracing::error!(
                        p,
                        id = %it.id,
                        count,
                        desired = it.desired,
                        "retry-op marker desired 损坏(count<desired-1),冻结分区(不 Drop/不处置,等修复)"
                    );
                    corrupt += 1;
                } else {
                    // count 与 desired 一致(∈{desired-1, desired})或更大:用**实际 count** 判毒。
                    //**接管首轮宽限**——`attempt==0` = 本 owner 刚接管、本地还没重投
                    // 过,此时**不单凭上一个 owner 的 PEL count 判毒**(旧节点 GC/网络等环境问题会把
                    // 正常消息的 delivery count 抬高;接管不该据此直接 Park/Drop)。先本地投递一次,
                    // attempt→1,下轮 attempt>0 才据 effective 判毒。正常消息在宽限轮被 handler 消费
                    // ACK,真毒消息宽限一轮后照常 poison。
                    let effective = attempt.max(count as u32);
                    if effective > max_redeliver && attempt > 0 {
                        poison.push(it);
                    } else {
                        retryable.push(it);
                    }
                }
            }
            Err(e) => {
                // 网络读失败:保守当 retryable(下轮再判),绝不据残缺信息 Drop
                tracing::warn!(p, id = %it.id, err = %e, "retry-op 读实际 count 失败,本轮当 retryable");
                retryable.push(it);
            }
        }
    }
    // 检出损坏 → 冻结整分区重试(长退避 + 告警;CAS 损坏不自动修改,fail-closed,绝不丢消息)
    if corrupt > 0 {
        slot.state = ClaimState::RetryBackoff {
            ids,
            attempt,
            until: tokio::time::Instant::now() + Duration::from_millis(30_000),
        };
        drop(permit);
        return;
    }

    // 1. 毒消息子集:**只处置 poison_ids**(绝不连坐 retryable)
    if !poison.is_empty() {
        let poison_ids: Vec<String> = poison.iter().map(|i| i.id.clone()).collect();
        match handle_poison(rt, slot, p, &poison_ids).await {
            PoisonOutcome::Frozen { park_id } => {
                // Park 冻结整分区:retryable 留 PEL,待 resume 后由 recovery/sweep 收编
                let _ = super::retryop::finish(&rt.client, &rt.layout, p, &op_id).await;
                slot.retry_attempt = 0;
                slot.state = ClaimState::Parked { park_id };
                drop(permit);
                return;
            }
            PoisonOutcome::Retry => {
                // 处置未确认(ACK/park 失败):保留**全部** ids 下轮重试
                // (marker 不收宗,Pending 重放幂等;retryable 一并延后一拍无害)
                slot.state = ClaimState::RetryBackoff {
                    ids,
                    attempt,
                    until: tokio::time::Instant::now() + Duration::from_millis(500),
                };
                drop(permit);
                return;
            }
            PoisonOutcome::Cleared => { /* Drop/Dlq 已清 poison 子集,继续派发 retryable */ }
        }
    }

    // 2. retryable 子集:retry-op CAS → 派发 InFlight(无 retryable 则收宗回 Ready)
    if retryable.is_empty() {
        let _ = super::retryop::finish(&rt.client, &rt.layout, p, &op_id).await;
        slot.retry_attempt = 0;
        slot.state = ClaimState::Ready;
        drop(permit);
        return;
    }
    let retryable_ids: Vec<String> = retryable.iter().map(|i| i.id.clone()).collect();

    // ── retry-op CAS 执行(仅 retryable 子集;意图已落盘;R4.2b 五规则)──
    // count==desired-1 才 XCLAIM RETRYCOUNT(唯一递增点),==desired 只 XRANGE 取 payload
    // (不重复递增),owner 变/Superseded/Corrupt 放弃。
    let mut records: Vec<(String, Vec<u8>)> = Vec::new();
    let mut resolved_ids: Vec<String> = Vec::new(); // 墓碑/已出 PEL:fenced ACK 清残留
    {
        // N2:retry-op CAS 现 owner-fenced——构造本任期凭据(holder + fence,无 fence_meta=原实现V1)
        let fence_key = rt
            .fence_meta
            .as_ref()
            .map(|_| super::fencing::fence_key(&rt.layout))
            .unwrap_or_default();
        let (round, nonce) = rt
            .fence_meta
            .as_ref()
            .map(|m| (m.round, m.nonce.clone()))
            .unwrap_or((0, String::new()));
        let holder = slot.guard.holder().to_string();
        let lock_key = slot.guard.lock_key().to_string();
        let counter = slot.stamp.as_ref().map(|s| s.counter).unwrap_or(0);
        let fence = super::retryop::FenceArgs {
            holder: &holder,
            lock_key: &lock_key,
            fence_key: &fence_key,
            round,
            nonce: &nonce,
            counter,
        };
        match super::retryop::execute(&rt.client, &rt.layout, p, &rt.node_id, &retryable, &fence)
            .await
        {
            Ok(outcomes) => {
                let mut ownership_lost = false;
                for (id, oc) in outcomes {
                    use super::retryop::RetryOutcome as O;
                    match oc {
                        // Claimed/Have 都拿到 payload:进重投批次(Have = 上次已 claim 过,不再递增)
                        O::Claimed(pl) | O::Have(pl) => records.push((id, pl)),
                        // EntryDeleted 墓碑(entry 已 XDEL/trim 但 PEL 引用残留)或已出 PEL:
                        // 收集后 fenced ACK 清理(文档 EntryDeleted 墓碑终态——不无限残留;
                        // ACK 不在 PEL 的 ID 是 no-op,对两种 RESOLVED 都安全)
                        O::Resolved => resolved_ids.push(id),
                        O::OwnershipChanged => {
                            //owner 已变 → **不能继续以本 owner 拉 `>`**
                            tracing::error!(p, id, "retry-op:owner 已变,停止以本 owner 续作");
                            ownership_lost = true;
                        }
                        O::Superseded => tracing::warn!(p, id, "retry-op:已被更新重投取代,跳过"),
                        O::Corrupt => {} // execute 内已 error 日志;不自动修改
                    }
                }
                // owner 变更:**不回 Ready、不 finish**(marker 留给新 owner);退避等看门狗释锁
                // → 移除 slot(避免 stale owner 在 watchdog 触发前的窗口继续拉 `>`/改 PEL)
                if ownership_lost {
                    slot.state = ClaimState::RetryBackoff {
                        ids: retryable_ids,
                        attempt,
                        until: tokio::time::Instant::now() + Duration::from_millis(1_000),
                    };
                    drop(permit);
                    return;
                }
                let _ = super::retryop::finish(&rt.client, &rt.layout, p, &op_id).await;
                // 墓碑清理:fenced ACK 把 entry 已删但 PEL 仍残留的 ID 清出 PEL(best-effort)
                if !resolved_ids.is_empty() {
                    fenced_ack_best_effort(rt, slot, p, &resolved_ids).await;
                }
            }
            Err(e) => {
                // CAS 执行失败(网络):仅保留 retryable 子集下轮重放(poison 已在上方处置;
                // marker 仍 Pending,下轮 ensure_pending 的 ID 集比对会自洽重算)
                tracing::warn!(p, err = %e, "retry-op CAS 执行失败,Pending 保留待重放");
                slot.state = ClaimState::RetryBackoff {
                    ids: retryable_ids,
                    attempt,
                    until: tokio::time::Instant::now() + Duration::from_millis(500),
                };
                drop(permit);
                return;
            }
        }
    }

    if records.is_empty() {
        // 全部已不在 PEL / 被放弃 → 回 Ready
        slot.retry_attempt = 0;
        slot.state = ClaimState::Ready;
        drop(permit);
        return;
    }
    // 重投批次进 InFlight(attempt 透传:worker 失败后由 coordinator 以 attempt+1 退避)
    slot.generation += 1;
    slot.state = ClaimState::InFlight {
        generation: slot.generation,
    };
    let batch = WorkBatch {
        generation: slot.generation,
        records,
        _permit: permit,
    };
    if let Err(e) = slot.work_tx.try_send(batch) {
        // 同主路径:投递失败必须保留 ID 回 RetryBackoff,不得静默丢
        let ids: Vec<String> = match e {
            mpsc::error::TrySendError::Full(b) | mpsc::error::TrySendError::Closed(b) => {
                b.records.iter().map(|(id, _)| id.clone()).collect()
            }
        };
        tracing::error!(p, ?ids, "重投批次 mailbox 投递失败,保留 RetryBackoff");
        slot.state = ClaimState::RetryBackoff {
            ids,
            attempt: slot.retry_attempt,
            until: tokio::time::Instant::now() + Duration::from_millis(500),
        };
    }
    let _ = attempt; // attempt 已由 slot.retry_attempt 权威维护(此处仅日志可用)
}

/// 对一组 ID 做 best-effort fenced ACK(墓碑清理用;V2 走 fenced Lua,V1 裸 XACK)。
/// ACK 不在 PEL 的 ID 是 no-op,故对"墓碑残留"与"已出 PEL"两种情况都安全。
///
/// # 参数
/// - `rt`: 分区消费组运行时状态。
/// - `slot`: 分区执行器或 Redis partition 的槽位编号。
/// - `p`: 分区编号或当前协议步骤中的短名参数。
/// - `ids`: Redis stream entry id 或业务记录 id 列表。
async fn fenced_ack_best_effort(rt: &Arc<GroupRuntime>, slot: &ClaimSlot, p: u32, ids: &[String]) {
    if ids.is_empty() {
        return;
    }
    let acked = if let (Some(meta), Some(st)) = (&rt.fence_meta, slot.stamp.as_ref()) {
        matches!(
            super::fencing::fenced_ack(
                &rt.client,
                &rt.layout,
                meta,
                st,
                lock_key_full(rt, p).as_str(),
                slot.guard.holder(),
                ids,
            )
            .await,
            Ok(super::fencing::FencedAck::Acked(_))
        )
    } else {
        // 原实现V1:裸 XACK 前补 holder 二次校验
        legacy_v1_guarded_xack(rt, p, slot.guard.holder(), ids)
            .await
            .is_ok()
    };
    if acked {
        rt.enqueue_async_delete(p, ids).await;
    }
}

/// 原实现V1(无 fence)裸 XACK 前补 holder 二次校验(与 worker `process_batch`
/// 的 XACK-前 holds_status 复核对齐)——`holds_status` 非 `Held`(Lost/Unknown)一律不 ACK,杜绝
/// "本轮判 not-lost、执行 XACK 时锁恰被新 owner 接管,旧 owner 仍把已被接管的消息清出 PEL"丢消息窗口。
/// RustV2 走 `fenced_ack` 原子校验(已安全),本路径仅 原实现V1(fence_meta=None,与 原实现 节点互通)。
///
/// # 参数
/// - `rt`: 分区消费组运行时状态。
/// - `p`: 分区编号或当前协议步骤中的短名参数。
/// - `holder`: 当前分布式锁持有者 token。
/// - `ids`: Redis stream entry id 或业务记录 id 列表。
async fn legacy_v1_guarded_xack(
    rt: &Arc<GroupRuntime>,
    p: u32,
    holder: &str,
    ids: &[String],
) -> std::result::Result<(), String> {
    use crate::lock::HoldStatus;
    if ids.is_empty() {
        return Ok(());
    }
    let lk = lock_key_full(rt, p);
    match rt.lock.holds_status(&lk, holder).await {
        HoldStatus::Held => {}
        HoldStatus::Lost => return Err("锁已失(Lost),跳过 LegacyV1 XACK".into()),
        HoldStatus::Unknown => return Err("锁状态未知(Unknown),保守跳过 LegacyV1 XACK".into()),
    }
    let mut ack = redis::cmd("XACK");
    ack.arg(rt.layout.stream(p)).arg(rt.layout.group());
    for id in ids {
        ack.arg(id);
    }
    ack.query_async::<i64>(&mut rt.client.conn())
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// 毒消息处置结果(只作用于传入的 poison 子集,绝不连坐 retryable)。
enum PoisonOutcome {
    /// Drop/Dlq 已把 poison 子集清出 PEL,分区继续消费 retryable。
    Cleared,
    /// Park(或 Dlq 发布失败回退)冻结整分区:retryable 留 PEL 待 resume 后由 recovery 收编。
    Frozen { park_id: String },
    /// 处置未确认(fenced ACK/park 失败):调用方保留全部 ids 下轮重试。
    Retry,
}

/// 处置毒消息**子集**(`poison_ids`):Drop = fenced ACK 清 PEL;Dlq = park 转存 + 发 DLQ;
/// Park = park 转存并冻结分区。不设置 slot.state(调用方按 PoisonOutcome 决定),
/// 仅读取 slot 的 guard/stamp。
///
/// # 参数
/// - `rt`: 分区消费组运行时状态。
/// - `slot`: 分区执行器或 Redis partition 的槽位编号。
/// - `p`: 分区编号或当前协议步骤中的短名参数。
/// - `poison_ids`: 需要标记为异常并隔离的分区消息 ID 列表。
async fn handle_poison(
    rt: &Arc<GroupRuntime>,
    slot: &ClaimSlot,
    p: u32,
    poison_ids: &[String],
) -> PoisonOutcome {
    match rt.cfg.poison_policy {
        PoisonPolicy::Drop => {
            tracing::error!(
                p,
                ?poison_ids,
                "毒消息超重投上限,按 Drop 策略 ACK 丢弃(仅 poison 子集)"
            );
            // V2:fenced ACK(原子校验 holder+fence+任期);V1:裸 XACK(holds 双检查由调用链保证)
            let acked: std::result::Result<(), String> =
                if let (Some(meta), Some(st)) = (&rt.fence_meta, slot.stamp.as_ref()) {
                    match super::fencing::fenced_ack(
                        &rt.client,
                        &rt.layout,
                        meta,
                        st,
                        lock_key_full(rt, p).as_str(),
                        slot.guard.holder(),
                        poison_ids,
                    )
                    .await
                    {
                        Ok(super::fencing::FencedAck::Acked(_)) => Ok(()),
                        Ok(super::fencing::FencedAck::Rejected(tag)) => {
                            Err(format!("fenced 拒绝: {tag}"))
                        }
                        Err(e) => Err(e.to_string()),
                    }
                } else {
                    // 原实现V1:裸 XACK 前补 holder 二次校验
                    legacy_v1_guarded_xack(rt, p, slot.guard.holder(), poison_ids).await
                };
            match acked {
                Ok(_) => {
                    rt.enqueue_async_delete(p, poison_ids).await;
                    PoisonOutcome::Cleared
                }
                Err(e) => {
                    tracing::warn!(p, err = %e, "Drop 的 ACK 未确认,保留重试(不回 Ready)");
                    PoisonOutcome::Retry
                }
            }
        }
        PoisonPolicy::Dlq => {
            // Dlq = fenced park 转存(payload 落 quarantine + 源 XACK)+ 立即发 DLQ stream
            let records = match fetch_pel_records(rt, p, poison_ids).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(p, err = %e, "取 PEL 原文失败,保留重试(不当已清理)");
                    return PoisonOutcome::Retry;
                }
            };
            if records.is_empty() {
                return PoisonOutcome::Cleared; // 确证已不在 PEL
            }
            match super::disposition::park(
                &rt.client,
                &rt.layout,
                p,
                slot.guard.lock_key(),
                slot.guard.holder(),
                &records,
            )
            .await
            {
                Ok(park_id) => {
                    let parked_ids: Vec<String> =
                        records.iter().map(|(id, _)| id.clone()).collect();
                    rt.enqueue_async_delete(p, &parked_ids).await;
                    // owner-fenced(R4.2f-A):worker 持 slot.guard + slot.stamp,直接构造 owner 凭据。
                    // ①.1:operation_id 用**可识别稳定** `auto:{p}:{park_id}`(非一次性 park_id)——
                    // auto-DLQ 是 owner 内部触发、无外部重提主体,publish 中途崩溃后接管方据 `auto:` 前缀
                    // 识别并自动续作到终态(见 takeover_disposition 的 AutoResume + coordinator)。
                    let auto_op = super::disposition::auto_op_id(p, &park_id);
                    let fence_key = rt
                        .fence_meta
                        .as_ref()
                        .map(|_| super::fencing::fence_key(&rt.layout))
                        .unwrap_or_default();
                    let (round, nonce) = rt
                        .fence_meta
                        .as_ref()
                        .map(|m| (m.round, m.nonce.clone()))
                        .unwrap_or((0, String::new()));
                    let lease = super::disposition::OwnerLease {
                        partition: p,
                        operation_id: &auto_op,
                        holder: slot.guard.holder(),
                        lock_key: slot.guard.lock_key(),
                        fence_key: &fence_key,
                        round,
                        nonce: &nonce,
                        counter: slot.stamp.as_ref().map(|s| s.counter).unwrap_or(0),
                    };
                    match super::disposition::dlq_from_parked(&rt.client, &rt.layout, p, &lease)
                        .await
                    {
                        Ok(dlq_ids) => {
                            tracing::error!(p, ?dlq_ids, "毒消息已入 DLQ,分区继续消费");
                            PoisonOutcome::Cleared
                        }
                        Err(e) => {
                            // 发布失败:已 park,回退冻结(可经管理 API 续作/resume/drop)
                            tracing::warn!(p, err = %e, "dlq 发布失败,回退 Parked 等管理处置");
                            PoisonOutcome::Frozen { park_id }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(p, err = %e, "dlq 前置 park 失败,保持重投退避");
                    PoisonOutcome::Retry
                }
            }
        }
        PoisonPolicy::Park => {
            let records = match fetch_pel_records(rt, p, poison_ids).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(p, err = %e, "取 PEL 原文失败,保留重试(不当已清理)");
                    return PoisonOutcome::Retry;
                }
            };
            if records.is_empty() {
                return PoisonOutcome::Cleared; // 确证已不在 PEL,无可 Park
            }
            match super::disposition::park(
                &rt.client,
                &rt.layout,
                p,
                slot.guard.lock_key(),
                slot.guard.holder(),
                &records,
            )
            .await
            {
                Ok(park_id) => {
                    let parked_ids: Vec<String> =
                        records.iter().map(|(id, _)| id.clone()).collect();
                    rt.enqueue_async_delete(p, &parked_ids).await;
                    tracing::error!(
                        p,
                        park_id,
                        n = records.len(),
                        "毒消息已 Park,分区停拉等待管理处置"
                    );
                    PoisonOutcome::Frozen { park_id }
                }
                Err(e) => {
                    tracing::warn!(p, err = %e, "park 失败,保持重投退避");
                    PoisonOutcome::Retry
                }
            }
        }
    }
}

/// 新 owner 接管恢复:`XAUTOCLAIM min_idle=0` 分页收编本组**全部**残留
/// PEL 到本 consumer——旧 owner 已被分区锁 + fencing 排除,min_idle=0 立即收编,不像
/// 运行期孤儿巡检那样制造一段不可消费窗口。收编到的 ID 交 coordinator 转 RetryBackoff,
/// 走 permit 门控的重投/毒消息路径(payload 在重投路径再取)。
///
/// 完成前 Claim 保持 `Recovering`(禁 `>`);XAUTOCLAIM 失败则退避重试同游标(不切 Ready),
/// 直到扫完或被 cancel(分区让出/停机)。含游标分页 + 停滞保护 + deleted/tombstone ID。
pub(super) async fn recover_pel(
    rt: &Arc<GroupRuntime>,
    p: u32,
    cancel: &CancellationToken,
) -> Vec<String> {
    // 外层:一次扫描失败(transport/协议异常/游标停滞)整轮重来——**绝不报假完成**
    // (parser/停滞 fail-open 会被归约成"空 PEL,切 Ready",必须 fail-closed
    // 保持 Recovering 退避重试)。仅 cancel(让出/停机)才结束。
    loop {
        if cancel.is_cancelled() {
            return Vec::new();
        }
        match scan_pel_once(rt, p, cancel).await {
            Ok(ids) => return ids,
            Err(e) => {
                tracing::warn!(p, err = %e, "PEL recovery 扫描异常,退避后整轮重试(保持 Recovering)");
                tokio::select! {
                    _ = cancel.cancelled() => return Vec::new(),
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                }
            }
        }
    }
}

/// 一次完整的 PEL 收编扫描:`XAUTOCLAIM min_idle=0 ... JUSTID` 游标分页,直到游标归 0。
/// 返回收编到的 **claimed** ID(进重投);deleted ID(Redis 已自动移出 PEL,实测)
/// 仅计数告警,**不进 retry-op**。transport/协议/停滞异常一律上抛由外层重试。
///
/// # 参数
/// - `rt`: 分区消费组运行时状态。
/// - `p`: 分区编号或当前协议步骤中的短名参数。
/// - `cancel`: 后台任务使用的取消信号。
async fn scan_pel_once(
    rt: &Arc<GroupRuntime>,
    p: u32,
    cancel: &CancellationToken,
) -> Result<Vec<String>> {
    let stream = rt.layout.stream(p);
    let group = rt.layout.group().to_string();
    let count = rt.cfg_batch();
    let mut claimed_ids: Vec<String> = Vec::new();
    let mut deleted_total = 0usize;
    let mut cursor = "0-0".to_string();
    let mut stall = 0u32;
    loop {
        if cancel.is_cancelled() {
            return Ok(claimed_ids);
        }
        // XAUTOCLAIM <stream> <group> <consumer> 0 <cursor> COUNT n JUSTID
        // (JUSTID:只要 ID 且**不增 delivery count**——实测确认;payload 在重投路径再取;
        //  min_idle=0:旧 owner 已被锁+fencing 排除)
        let v: redis::Value = redis::cmd("XAUTOCLAIM")
            .arg(&stream)
            .arg(&group)
            .arg(&rt.node_id)
            .arg(0)
            .arg(&cursor)
            .arg("COUNT")
            .arg(count)
            .arg("JUSTID")
            .query_async(&mut rt.client.conn())
            .await?; // transport 错误上抛 → 外层整轮重试
        let (next, claimed, deleted) = parse_xautoclaim_justid(v)?; // 协议异常上抛
        let progressed = !claimed.is_empty() || !deleted.is_empty();
        deleted_total += deleted.len();
        claimed_ids.extend(claimed);
        // 游标回到 0(0-0/0)= 一轮扫完
        if next == "0-0" || next == "0" {
            if deleted_total > 0 {
                tracing::warn!(
                    p,
                    deleted = deleted_total,
                    "PEL recovery:遇 tombstone(已被 Redis 移出 PEL),仅计数"
                );
            }
            return Ok(claimed_ids);
        }
        // 游标停滞保护:连续无进展且游标不前进 → 报错(由外层退避重试,**不当作完成**)
        if !progressed && next == cursor {
            stall += 1;
            if stall >= 3 {
                return Err(NasaRedisError::ProtocolMarker(format!(
                    "PEL recovery 游标停滞(p={p}, cursor={cursor})"
                )));
            }
        } else {
            stall = 0;
        }
        cursor = next;
    }
}

/// 运行期 orphan sweep 单分区:对已持有分区
/// `XAUTOCLAIM min_idle ... JUSTID` **游标分页**收编滞留过久的 PEL——兜住"旧 owner 丢锁后、
/// 其看门狗尚未判定 Lost 前的迟到 `>` 把消息拉进死 consumer PEL"这种一次性 takeover recovery
/// 覆盖不到的残留(min_idle 过滤本方刚拉取的新条目,不误抢)。
/// 改造点:游标分页清完 backlog(单页改多页);**错误不再静默吞成空**——记录 warn(诊断上
/// 不把"扫描失败"伪装成"没有孤儿");单分区 page 数有上限防卡死。
pub(super) async fn sweep_partition(
    rt: &Arc<GroupRuntime>,
    p: u32,
    min_idle_ms: u64,
) -> Vec<String> {
    const MAX_PAGES: u32 = 64; // 单轮单分区页上限(剩余 backlog 下一轮再收)
    let stream = rt.layout.stream(p);
    let group = rt.layout.group().to_string();
    let mut out: Vec<String> = Vec::new();
    let mut cursor = "0-0".to_string();
    for _ in 0..MAX_PAGES {
        // 停机/取消即止(单轮 page 预算 + cancel 检查)
        if rt.bg_cancel.is_cancelled() {
            break;
        }
        //**每页前校验本节点仍持有该分区**(owner_ctx 在失锁/让出/停机时移除)
        // ——一旦失去所有权立即停,不再对它的 PEL 做 XAUTOCLAIM 改 owner/idle(缩窄迟到副作用窗口)。
        if !rt.owner_ctx.lock().expect("owner_ctx").contains_key(&p) {
            break;
        }
        let v: std::result::Result<redis::Value, redis::RedisError> = redis::cmd("XAUTOCLAIM")
            .arg(&stream)
            .arg(&group)
            .arg(&rt.node_id)
            .arg(min_idle_ms)
            .arg(&cursor)
            .arg("COUNT")
            .arg(rt.cfg_batch())
            .arg("JUSTID")
            .query_async(&mut rt.client.conn())
            .await;
        let (next, claimed, _deleted) = match v {
            Ok(v) => match parse_xautoclaim_justid(v) {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(p, err = %e, "orphan sweep:XAUTOCLAIM 响应异常,本轮中止(非'无孤儿')");
                    break;
                }
            },
            Err(e) => {
                tracing::warn!(p, err = %e, "orphan sweep:XAUTOCLAIM 失败,本轮中止(非'无孤儿')");
                break;
            }
        };
        out.extend(claimed);
        if next == "0-0" || next == "0" {
            break; // 扫完
        }
        cursor = next;
    }
    out
}

/// 解析 `XAUTOCLAIM ... JUSTID` 响应:`[next-cursor, [id...], [deleted-id...]]`。
/// 响应形态异常即 Err(协议错误,**不猜成空**,R4)。
///
/// # 参数
/// - `v`: 待转换的值。
fn parse_xautoclaim_justid(v: redis::Value) -> Result<(String, Vec<String>, Vec<String>)> {
    let redis::Value::Array(parts) = v else {
        return Err(NasaRedisError::ProtocolMarker(
            "XAUTOCLAIM 响应非数组".into(),
        ));
    };
    let mut it = parts.into_iter();
    let next = match it.next() {
        Some(c) => String::from_utf8_lossy(&value_bytes(c)).into_owned(),
        None => {
            return Err(NasaRedisError::ProtocolMarker(
                "XAUTOCLAIM 响应缺游标段".into(),
            ))
        }
    };
    let claimed = match it.next() {
        Some(redis::Value::Array(items)) => items
            .into_iter()
            .map(|i| String::from_utf8_lossy(&value_bytes(i)).into_owned())
            .collect(),
        Some(redis::Value::Nil) | None => Vec::new(),
        Some(other) => {
            return Err(NasaRedisError::ProtocolMarker(format!(
                "XAUTOCLAIM claimed 段形态异常: {other:?}"
            )))
        }
    };
    // 第三段(Redis 7+)= tombstone ID;缺失容忍(旧版本无此段)
    let deleted = match it.next() {
        Some(redis::Value::Array(items)) => items
            .into_iter()
            .map(|i| String::from_utf8_lossy(&value_bytes(i)).into_owned())
            .collect(),
        _ => Vec::new(),
    };
    Ok((next, claimed, deleted))
}

/// 取回指定 ID 的 entry 原文。改用 **XRANGE**(只读 stream entry,**不需要 PEL
/// 所有权、不带外递增 delivery count**)——此前 `XCLAIM idle=0` 既要求本节点是 PEL owner(与
/// 锁/fence 所有权不一致窗口下会跨 owner 改 PEL),又会把 delivery count +1(带外污染毒消息判定)。
/// fail-closed:Redis 错误**上抛**(调用方 → `PoisonOutcome::Retry`);entry 不存在
/// (已 XDEL/trim)= 自然缺席,不算错误。
pub(super) async fn fetch_pel_records(
    rt: &Arc<GroupRuntime>,
    p: u32,
    ids: &[String],
) -> crate::error::Result<Vec<(String, Vec<u8>)>> {
    let stream = rt.layout.stream(p);
    let mut records: Vec<(String, Vec<u8>)> = Vec::new();
    for id in ids {
        // XRANGE stream id id COUNT 1 → [[id, [f1,v1,...]]];entry 不存在 = 空数组(已删/trim)
        let v: redis::Value = redis::cmd("XRANGE")
            .arg(&stream)
            .arg(id)
            .arg(id)
            .arg("COUNT")
            .arg(1)
            .query_async(&mut rt.client.conn())
            .await?; // 错误上抛,不吞
                     // XRANGE 返回 entries 数组;复用 XREADGROUP 解析:伪装成单 stream 响应形态
        let mut tmp: HashMap<u32, Vec<(String, Vec<u8>)>> = HashMap::new();
        let wrapped = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::BulkString(stream.clone().into_bytes()),
            v,
        ])]);
        let mut perr = std::collections::HashSet::new();
        if !parse_xreadgroup(&rt.layout, wrapped, &mut tmp, &mut perr) || !perr.is_empty() {
            return Err(NasaRedisError::ProtocolMarker(format!(
                "fetch_pel_records:XRANGE 响应形态异常(分区 {p} id={id})"
            )));
        }
        if let Some(mut rs) = tmp.remove(&p) {
            records.append(&mut rs);
        }
    }
    Ok(records)
}

// ── 毒消息管理入口(单 owner 进程内直调;跨节点路由 = command outbox,R4.2b)──
