use super::*;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::Semaphore;

/// Coordinates ownership events and worker wakeups.
pub(super) async fn coordinator_loop(rt: Arc<GroupRuntime>, mut event_rx: mpsc::Receiver<Event>) {
    let mut slots: HashMap<u32, ClaimSlot> = HashMap::new();
    // 全局批次预算 = max_inflight_batches(接线 stream.inflight_max,
    // 不再固定 = 分区数;业务饱和时拿不到 permit 的 Ready 分区本轮不拉,消息留 Redis)。
    let budget = Arc::new(Semaphore::new(rt.stream_cfg.inflight_max.max(1)));
    // 空轮冷流再 poll 间隔 = stream.poll_timeout_ms(NOBLOCK 托管模式语义,
    //接线,不再借 holds_check_interval)。
    let poll_timeout = Duration::from_millis(rt.stream_cfg.poll_timeout_ms.max(1));

    let mut shutdown_ack: Option<oneshot::Sender<()>> = None;
    // 停机 drain 的**绝对** deadline(首次观测到 cancel 时锁定一次,
    // 不逐轮重置;到期即使 in-flight 未归零也强制取消 worker + 释锁退出,
    // 否则挂死的 handler 会让停机永久等待)。
    let mut drain_deadline: Option<tokio::time::Instant> = None;

    loop {
        // ── 1. 处理积压事件(非阻塞排干;事件可重复,按 generation 幂等)──
        while let Ok(ev) = event_rx.try_recv() {
            handle_event(&rt, &mut slots, ev, &mut shutdown_ack).await;
        }

        // ── 2. 停机 drain:admission 已关,等全部 InFlight 归零 / drain_timeout 到期后退出 ──
        if rt.cancel.is_cancelled() {
            // 优先用 shutdown 入口生成的统一 deadline;未设置(直接 cancel
            // 而非经 shutdown())才以 cfg.drain_timeout_ms 起算
            let deadline = *drain_deadline.get_or_insert_with(|| {
                rt.shutdown_deadline
                    .lock()
                    .ok()
                    .and_then(|g| *g)
                    .unwrap_or_else(|| {
                        tokio::time::Instant::now()
                            + std::time::Duration::from_millis(rt.cfg.drain_timeout_ms)
                    })
            });
            let inflight = slots
                .values()
                .filter(|s| matches!(s.state, ClaimState::InFlight { .. }))
                .count();
            let timed_out = tokio::time::Instant::now() >= deadline;
            if inflight == 0 || timed_out {
                if timed_out && inflight > 0 {
                    // 超时强制退出:取消在途 worker(其批次不 ACK,留 PEL 交新 owner recovery)
                    tracing::warn!(
                        inflight,
                        drain_timeout_ms = rt.cfg.drain_timeout_ms,
                        "停机 drain 超时,强制取消在途 worker 并释锁(未完成批次留 PEL)"
                    );
                }
                // 清理:**单一绝对 deadline**——
                // ① 一次性 cancel 全部 worker(不再逐个串行等 200ms);
                // ② 在 deadline 内**并发** join 所有 worker,到期 abort 剩余;
                // ③ unlock 用剩余预算,耗尽即返回靠 lease(超时丢弃 guard 仍触发 Drop best-effort)。
                let drained: Vec<(u32, ClaimSlot)> = slots.drain().collect();
                rt.owner_ctx.lock().expect("owner_ctx").clear(); // 停机:清空 owner 凭据
                let mut handles = Vec::new();
                let mut guards = Vec::new();
                for (p, slot) in drained {
                    let ClaimSlot {
                        worker_cancel,
                        mut worker_handle,
                        guard,
                        mut recovery_handle,
                        ..
                    } = slot;
                    worker_cancel.cancel();
                    handles.push(worker_handle.take());
                    //:recovery 任务也纳入 drain join——停机必须等其退出(或 deadline abort)再
                    // 释锁,避免迟到 XAUTOCLAIM 在解锁后改 PEL
                    if let Some(mut h) = recovery_handle.take() {
                        handles.push(h.take());
                    }
                    guards.push((p, guard));
                }
                //**严格用绝对 deadline,不再 .max(200ms/50ms) 隐式延长**。
                // worker 在 deadline 内并发 join,到期 abort;guard unlock 用到 deadline 的剩余
                // 预算,预算耗尽即跳过、靠 lease + Drop best-effort 释放(不再额外加宽限)。
                let abort_handles: Vec<_> = handles.iter().map(|h| h.abort_handle()).collect();
                let join_fut = futures::future::join_all(handles);
                if tokio::time::timeout_at(deadline, join_fut).await.is_err() {
                    tracing::warn!(
                        "停机 drain 到绝对 deadline,abort 剩余 worker(未退出的 handler 强制中断)"
                    );
                    for a in abort_handles {
                        a.abort();
                    }
                }
                for (p, guard) in guards {
                    if tokio::time::Instant::now() >= deadline {
                        tracing::warn!(
                            p,
                            "停机已过 deadline,跳过 unlock(靠 lease + Drop best-effort)"
                        );
                        drop(guard); // Drop:取消看门狗 + spawn UNLOCK(best-effort)
                        continue;
                    }
                    match tokio::time::timeout_at(deadline, guard.unlock()).await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => tracing::warn!(p, err = %e, "停机释锁失败(将由 lease 兜底)"),
                        Err(_) => {
                            tracing::warn!(p, "停机释锁超时(预算耗尽,靠 lease + Drop best-effort)")
                        }
                    }
                }
                rt.claimed.write().expect("claimed").clear();
                if let Some(ack) = shutdown_ack.take() {
                    let _ = ack.send(());
                }
                return;
            }
            // 还有在途批次且未到 deadline:等事件**或** deadline,不再发起新 poll
            tokio::select! {
                ev = event_rx.recv() => {
                    if let Some(ev) = ev {
                        handle_event(&rt, &mut slots, ev, &mut shutdown_ack).await;
                    }
                }
                _ = tokio::time::sleep_until(deadline) => { /* 下一轮命中 timed_out 强制退出 */ }
            }
            continue;
        }

        // ── 2.5 丢锁检测:看门狗判定锁丢失 → 立即移除该 Claim、停止
        //    一切 poll。否则旧 owner 的 Ready/Backoff 到期后仍会 `>` XREADGROUP,把消息
        //    拉进**死 consumer 的 PEL**,新 owner 一次性 recovery 之后只读 `>` 永不再见。
        //    本地 watch 检测(看门狗 RENEW 明确返 0 即置 lost),零额外 Redis RTT──
        let lost: Vec<u32> = slots
            .iter()
            .filter(|(_, s)| *s.guard.lost().borrow())
            .map(|(p, _)| *p)
            .collect();
        for p in lost {
            if let Some(mut slot) = slots.remove(&p) {
                slot.worker_cancel.cancel();
                // 失锁:立即撤销 owner 凭据——此后任何 direct/control 管理操作都因
                // owner_ctx 无该分区被拒(fenced Lua 的 holder/counter 校验是二次兜底)
                rt.owner_ctx.lock().expect("owner_ctx").remove(&p);
                // 锁已丢且无法等待优雅退出，必须立即中止 recovery 和 worker，避免失权任务继续写 Redis。
                if let Some(mut h) = slot.recovery_handle.take() {
                    h.abort();
                }
                slot.worker_handle.abort();
                tracing::warn!(p, node = %rt.node_id, "看门狗判定分区锁丢失,移除 Claim 并停止 poll");
                drop(slot.guard); // best-effort(锁多半已被他人持有/过期)
                rt.claimed.write().expect("claimed").retain(|x| *x != p);
            }
        }

        // ── 2.7 发布 sweep 票据:**只列 Ready/Backoff 分区 + 其 claim
        //    generation**,sweep_loop 据此扫描,不再扫 InFlight/Recovering/RetryBackoff/Parked
        //    (避免对它们 XAUTOCLAIM 改 PEL owner/idle);回灌时带 generation 校验。
        {
            let tickets: Vec<(u32, u64)> = slots
                .iter()
                .filter(|(_, s)| matches!(s.state, ClaimState::Ready | ClaimState::Backoff { .. }))
                .map(|(p, s)| (*p, s.generation))
                .collect();
            *rt.sweepable.lock().expect("sweepable") = tickets;
        }
        // (orphan sweep 已移出本循环 → 后台 sweep_loop,经 Event::OrphansClaimed 回灌,
        // 避免串行 Redis I/O 阻塞 coordinator 处理 shutdown、lost 和 event。

        // ── 3. RetryBackoff 到期:先拿 permit,再走 PEL 精确重投(禁 `>`,不变量 5)──
        //(架构观察,**非正确性缺陷,已评审决定维持**):retry_redeliver 改 slots 状态,
        // **只能内联在串行 coordinator**(不像 sweep 是只读+经 OrphansClaimed 回灌才得以移出循环)。其内部对每个
        // RetryBackoff-due 分区做 ensure_pending(1 Lua)+ N×actual_count(XPENDING)+ N×CAS(EVAL)+ poison 处置,
        // 全部内联 await。**degraded 场景**(下游瞬断致本节点几十分区在途批次同时失败 → 同时 due + 高 RTT)下,
        // step 3 串行内联 I/O 可达数百~上万 RTT,**暂时拖慢**主循环到达 step 4/5(permit 回收、丢锁检测、shutdown)。
        // **维持理由**:① 仅 degraded-mode 延迟、非正确性(coordinator 必恢复、非死锁);② 每轮 retry 分区数已被
        // permit(inflight_max)+ 本节点 owned 分区数双重限幅;③ 移出需把 retry-op I/O spawn 出去、只把 records/处置
        // 结论经事件回灌改状态(类比 sweep→OrphansClaimed),是对调度中枢的较大重构,无实测生产问题驱动不做。
        // **复议触发**:生产实测到 mass-retry 主循环停顿导致丢锁检测/shutdown 超 SLA 时,再按 ③ 重构。
        let now = tokio::time::Instant::now();
        let retry_due: Vec<u32> = slots
            .iter()
            .filter(|(_, s)| matches!(&s.state, ClaimState::RetryBackoff { until, .. } if *until <= now))
            .map(|(p, _)| *p)
            .collect();
        for p in retry_due {
            let Ok(permit) = Arc::clone(&budget).try_acquire_owned() else {
                continue;
            }; // 拿不到→保持 RetryBackoff
            retry_redeliver(&rt, &mut slots, p, permit).await;
        }

        // ── 4. Ready 且拿到 permit 的分区 → 合批 NOBLOCK XREADGROUP(不变量 2/3)──
        // **OverloadPolicy = SkipPoll**：预算 inflight_max
        // semaphore)耗尽时**不拉取**那些分区,消息**留在 Redis**(可靠队列,无丢失)、下轮预算回收后再拉。
        // 原实现 的 {Inline/Wait/DropOnOverload} 多态对应**executor 架构**,在 Rust 的 async coordinator-loop +
        // 独立 worker 模型里都不适用:Wait=在 coordinator 循环阻塞等 permit 会卡死所有分区进度(workers 靠
        // coordinator 推进才能释放 permit → 死锁);DropOnOverload=丢消息违背可靠队列;Inline=取消并行。
        // 故 SkipPoll 是唯一安全策略,无需多态枚举(配 inflight_max 调节背压深度即可)。
        let mut polled: Vec<(u32, tokio::sync::OwnedSemaphorePermit)> = Vec::new();
        for (p, slot) in slots.iter() {
            if matches!(slot.state, ClaimState::Ready) {
                match Arc::clone(&budget).try_acquire_owned() {
                    Ok(permit) => polled.push((*p, permit)),
                    Err(_) => break, // 预算耗尽:其余 Ready 保持原状,消息留在 Redis(SkipPoll)
                }
            }
        }
        if !polled.is_empty() {
            poll_and_dispatch(&rt, &mut slots, polled).await;
        }

        // ── 5. 等待:事件 / 最近的 Backoff 到期 / orphan sweep 到期(select!+sleep)──
        let next_deadline = slots
            .values()
            .filter_map(|s| match &s.state {
                ClaimState::Backoff { until } => Some(*until),
                ClaimState::RetryBackoff { until, .. } => Some(*until),
                _ => None,
            })
            .min()
            .unwrap_or_else(|| tokio::time::Instant::now() + poll_timeout);
        tokio::select! {
            ev = event_rx.recv() => {
                if let Some(ev) = ev { handle_event(&rt, &mut slots, ev, &mut shutdown_ack).await; }
            }
            _ = tokio::time::sleep_until(next_deadline) => {
                // 冷流到期:统一翻回 Ready(下轮进 poll)
                let now = tokio::time::Instant::now();
                for s in slots.values_mut() {
                    if let ClaimState::Backoff { until } = s.state {
                        if until <= now { s.state = ClaimState::Ready; }
                    }
                }
            }
            _ = rt.cancel.cancelled() => { /* 进入下轮循环的 drain 分支 */ }
        }
    }
}

/// 事件归约(coordinator 独占写 slots;WorkFinished 按 generation 幂等, CAS 语义)。
pub(super) async fn handle_event(
    rt: &Arc<GroupRuntime>,
    slots: &mut HashMap<u32, ClaimSlot>,
    ev: Event,
    shutdown_ack: &mut Option<oneshot::Sender<()>>,
) {
    match ev {
        Event::WorkFinished {
            p,
            generation,
            outcome,
        } => {
            let Some(slot) = slots.get_mut(&p) else {
                return;
            }; // 分区已释放:迟到事件,忽略
            let ClaimState::InFlight { generation: cur } = slot.state else {
                return;
            };
            if cur != generation {
                return; // 旧代完成事件(rebalance 后),忽略——generation 过滤
            }
            match outcome {
                WorkOutcome::Acked => {
                    slot.retry_attempt = 0; // 成功:重投计数清零
                    slot.state = ClaimState::Ready;
                }
                WorkOutcome::Failed { ids } => {
                    // 失败 → RetryBackoff:attempt 累计,指数退避(200ms × 2^(n-1),封顶 5s)。
                    //`clamp(1,6)` 而非 `min(6)`——保证移位幂 ≥0,杜绝 attempt=0 时
                    //  `min(6)-1` u32 下溢 panic(当前 attempt 已先 +1 ≥1 不可达,显式防御未来误用)。
                    slot.retry_attempt += 1;
                    let backoff = Duration::from_millis(
                        (200u64 << (slot.retry_attempt.clamp(1, 6) - 1)).min(5_000),
                    );
                    slot.state = ClaimState::RetryBackoff {
                        ids,
                        attempt: slot.retry_attempt,
                        until: tokio::time::Instant::now() + backoff,
                    };
                }
                WorkOutcome::LostOwnership => {
                    // 所有权丢失:本地退场(锁已不属于我们,不执行任何 Redis 修改)
                    tracing::warn!(p, "fencing 判定所有权丢失,移除本地 claim");
                    let mut slot = slots.remove(&p).expect("刚 get 过");
                    slot.worker_cancel.cancel();
                    rt.owner_ctx.lock().expect("owner_ctx").remove(&p); // 撤销 owner 凭据
                    if let Some(mut h) = slot.recovery_handle.take() {
                        h.abort(); //:所有权已失,abort recovery 杜绝迟到 XAUTOCLAIM
                    }
                    slot.worker_handle.abort(); // 立即中止 worker，杜绝失锁后继续操作 Redis。
                    drop(slot.guard); // Drop best-effort(锁多半已被他人持有/过期)
                    rt.claimed.write().expect("claimed").retain(|x| *x != p);
                }
            }
        }
        Event::ClaimAcquired { p, guard } => {
            if slots.contains_key(&p) {
                // 重复抢占(理论不可达:锁互斥);释放多余 guard。
                //冷路径**直接 await unlock**(不再 detached spawn 游离 task)——
                // 满足"无未 join 游离 task",且 response_timeout 兜底 unlock 不会无限挂。
                let _ = guard.unlock().await;
                return;
            }
            // V2:获取本分区任期 stamp(新任期使旧 owner 的迟到 ACK 被 STALE 拒绝)
            let stamp = match &rt.fence_meta {
                Some(meta) => {
                    match super::fencing::acquire_stamp(&rt.client, &rt.layout, meta, p).await {
                        Ok(st) => Some(st),
                        Err(e) => {
                            // stamp 获取失败:不接管(fence 未就绪即 fail-closed)
                            tracing::error!(p, err = %e, "acquire_stamp 失败,放弃接管");
                            let _ = guard.unlock().await; // 冷路径直接 await(不留游离 task)
                            return;
                        }
                    }
                }
                None => None, // 原实现V1:无 stamp
            };
            // 归约 disposition marker + parked index(**在 spawn worker 之前**,便于 fail-closed
            // 直接放弃接管)。**marker 是权威事实源**,不能只信 parked index——
            // marker=Parked 而 index 缺失会被误判为“未 Park”并恢复消费，因此必须 fail-closed：
            // 读失败(Redis/WRONGTYPE/损坏)→ fail-closed 放弃接管、释锁退避(不静默消费)。
            let dispo = match super::disposition::takeover_disposition(&rt.client, &rt.layout, p)
                .await
            {
                Ok(d) => d,
                Err(e) => {
                    tracing::error!(p, err = %e, "接管时读 disposition 失败,放弃接管(fail-closed,不消费)");
                    let _ = guard.unlock().await; // 冷路径直接 await(不留游离 task)
                    return;
                }
            };
            let initial_state = match dispo {
                super::disposition::TakeoverDisposition::NotParked => {
                    ClaimState::Recovering { generation: 0 }
                }
                super::disposition::TakeoverDisposition::Frozen { park_id } => {
                    tracing::warn!(
                        p,
                        park_id,
                        "接管的分区处于处置在途态(marker 非终态),冻结停拉"
                    );
                    ClaimState::Parked { park_id }
                }
                // 接管到 auto-DLQ 在途时，因其由 owner 内部触发且无外部重提主体，必须自动续作 DLQ。
                // 发布到终态,消除原"publish 中途崩溃 → 分区永久冻结、admin 异 op 也续不了"的确定性死结。
                super::disposition::TakeoverDisposition::AutoResumeDlq {
                    park_id,
                    operation_id,
                } => {
                    tracing::warn!(
                        p,
                        park_id,
                        operation_id,
                        "接管到 auto-DLQ 在途态,自动续作 DLQ 发布"
                    );
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
                        operation_id: &operation_id,
                        holder: guard.holder(),
                        lock_key: guard.lock_key(),
                        fence_key: &fence_key,
                        round,
                        nonce: &nonce,
                        counter: stamp.as_ref().map(|s| s.counter).unwrap_or(0),
                    };
                    match super::disposition::dlq_from_parked(&rt.client, &rt.layout, p, &lease)
                        .await
                    {
                        Ok(ids) => {
                            tracing::error!(
                                p,
                                ?ids,
                                "接管自动续作 auto-DLQ 完成(已终态 Dlqed),恢复消费"
                            );
                            ClaimState::Recovering { generation: 0 }
                        }
                        Err(e) => {
                            // 基础设施错误(网络/Redis 瞬时):暂冻结,下次接管/再平衡读到仍 DlqPublishing+auto
                            // 会再次 AutoResume(marker 未推进终态,续作幂等安全)。
                            tracing::warn!(p, err = %e, "接管自动续作 auto-DLQ 失败,暂冻结(下轮再续)");
                            ClaimState::Parked { park_id }
                        }
                    }
                }
                super::disposition::TakeoverDisposition::Inconsistent { reason } => {
                    // marker/index 不一致或损坏:fail-closed,放弃接管(交下轮再平衡 + 告警)
                    tracing::error!(
                        p,
                        reason,
                        "disposition 状态不一致,放弃接管(fail-closed,不消费)"
                    );
                    let _ = guard.unlock().await; // 冷路径直接 await(不留游离 task)
                    return;
                }
            };
            // owner 凭据登记:管理路径据此构造 owner-fenced 转换凭据
            let holder = guard.holder().to_string();
            rt.owner_ctx.lock().expect("owner_ctx").insert(
                p,
                super::OwnerCtx {
                    holder: holder.clone(),
                    counter: stamp.as_ref().map(|s| s.counter).unwrap_or(0),
                },
            );
            // 建串行 worker(mailbox 容量 1,不变量 1)
            let (work_tx, work_rx) = mpsc::channel::<WorkBatch>(1);
            let worker_cancel = CancellationToken::new();
            let worker_handle = tokio::spawn(worker_loop(
                Arc::clone(rt),
                p,
                holder,
                stamp.clone(),
                work_rx,
                worker_cancel.clone(),
            ));
            let need_recover = matches!(initial_state, ClaimState::Recovering { .. });
            // 收编任务复用 worker_cancel:分区让出/停机时一并取消
            let rec_cancel = worker_cancel.clone();
            slots.insert(
                p,
                ClaimSlot {
                    state: initial_state,
                    guard,
                    work_tx,
                    worker_cancel,
                    worker_handle: AbortOnDropTask::new(worker_handle),
                    generation: 0,
                    stamp,
                    retry_attempt: 0,
                    recovery_handle: None,
                },
            );
            rt.claimed.write().expect("claimed").push(p);
            if need_recover {
                let rt2 = Arc::clone(rt);
                //**存 JoinHandle 进 slot**(不再 detached),drain/release 据此等退出
                let h = tokio::spawn(async move {
                    let ids = super::retry::recover_pel(&rt2, p, &rec_cancel).await;
                    if !rec_cancel.is_cancelled() {
                        let _ = rt2
                            .event_tx
                            .send(Event::RecoveryFinished {
                                p,
                                generation: 0,
                                ids,
                            })
                            .await;
                    }
                });
                if let Some(slot) = slots.get_mut(&p) {
                    slot.recovery_handle = Some(AbortOnDropTask::new(h));
                }
                tracing::info!(p, node = %rt.node_id, "分区已接管,开始收编残留 PEL(Recovering)");
            } else {
                tracing::info!(p, node = %rt.node_id, "分区已接管(Parked,恢复停拉)");
            }
        }
        Event::RecoveryFinished { p, generation, ids } => {
            let Some(slot) = slots.get_mut(&p) else {
                return; // 分区已释放:收编结果作废(消息留 PEL 交下一任 owner)
            };
            let ClaimState::Recovering { generation: cur } = slot.state else {
                return; // 已不在 Recovering:迟到事件,忽略
            };
            if cur != generation {
                return;
            }
            if ids.is_empty() {
                slot.state = ClaimState::Ready;
                tracing::info!(p, "PEL 接管完成:无残留,进入正常消费");
            } else {
                // 收编到的 PEL 交 RetryBackoff:走 permit 门控的重投/毒消息路径
                // (retry-op owner==self,逐条取 payload 重投;超限按 PoisonPolicy 处置)
                let n = ids.len();
                slot.state = ClaimState::RetryBackoff {
                    ids,
                    attempt: 0,
                    //接管首轮不立即重投(`now`)——大量滞留 PEL 会一次性抢 permit
                    // 重投风暴、抖动并加速撞毒。给 1s 平滑(后续按指数退避)。
                    until: tokio::time::Instant::now() + Duration::from_millis(1_000),
                };
                tracing::warn!(p, n, "PEL 接管完成:收编残留进重投路径(1s 后首投)");
            }
        }
        Event::ReleaseExcess { n } => {
            // 超额释放:**只挑明确可安全迁移的 Ready/Backoff**(
            // RetryBackoff 有未处置 PEL、新 owner 当前还没有通用 PEL recovery 会放大
            //;Parked 是待人工介入态;Recovering
            // 正在收编 PEL;InFlight 在途——这些都不可让出)。
            let victims: Vec<u32> = slots
                .iter()
                .filter(|(_, s)| matches!(s.state, ClaimState::Ready | ClaimState::Backoff { .. }))
                .map(|(p, _)| *p)
                .take(n)
                .collect();
            //让出改用与停机 drain 同款「同步摘除 → 并发 drain + 单一 deadline」范式,
            // 不再逐 victim 串行 await(k 个分区最坏 k×1s 卡死 coordinator 主循环 → permit 不归还、背压假饱和)。
            // ① 同步快速摘除(cancel + 撤 owner_ctx),不 await;
            let mut draining: Vec<(u32, ClaimSlot)> = Vec::with_capacity(victims.len());
            for p in victims {
                let slot = slots.remove(&p).expect("victims 来自 slots");
                slot.worker_cancel.cancel();
                rt.owner_ctx.lock().expect("owner_ctx").remove(&p); // 撤销 owner 凭据
                draining.push((p, slot));
            }
            let released = !draining.is_empty();
            // ② 并发 drain:**单一 deadline**,各 victim 独立「等 recovery → 等 worker → unlock」(互不阻塞)。
            // 不变量保持:每个分区都先等 recovery/worker 退出再 unlock(解锁后无迟到 XAUTOCLAIM 改 PEL)。
            let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
            futures::future::join_all(draining.into_iter().map(|(p, mut slot)| {
                let rt = Arc::clone(rt);
                async move {
                    if let Some(mut task) = slot.recovery_handle.take() {
                        let h = task.take();
                        let ah = h.abort_handle();
                        if tokio::time::timeout_at(deadline, h).await.is_err() {
                            ah.abort();
                            tracing::warn!(p, "让出时 recovery 未在 deadline 内退出,已 abort");
                        }
                    }
                    let worker_handle = slot.worker_handle.take();
                    let ah = worker_handle.abort_handle();
                    if tokio::time::timeout_at(deadline, worker_handle)
                        .await
                        .is_err()
                    {
                        ah.abort();
                        tracing::warn!(p, "让出时 worker 未在 deadline 内退出,已 abort");
                    }
                    if let Err(e) = slot.guard.unlock().await {
                        tracing::warn!(p, err = %e, "释放分区锁失败(lease 兜底)");
                    }
                    rt.claimed.write().expect("claimed").retain(|x| *x != p);
                    tracing::info!(p, "分区已让出(再平衡)");
                }
            }))
            .await;
            //**锁真正释放之后**才 pub wake(让兄弟节点立刻抢到)。
            // 此前 wake 在 rebalance 任务里 ReleaseExcess 入队后立即发,那时锁还没释放,
            // 兄弟抢锁必失败、只能等周期兜底——与"锁真正释放后才 wake"的注释不符。
            if released {
                let _: std::result::Result<i64, _> = redis::cmd("PUBLISH")
                    .arg(rt.layout.wake())
                    .arg("")
                    .query_async(&mut rt.client.conn())
                    .await;
            }
        }
        Event::ParkResolved { p } => {
            // 管理 API 已完成 resume/drop/dlq:**不能直接 Ready**——Park 只隔离
            // 了 poison 子集,retryable 子集仍留在 PEL;Ready 只读 `>` 不读 PEL,会越过这些旧
            // 消息直接消费新消息,违反"失败批次未处置前禁 `>`"。改为先转 **Recovering** 跑
            // 一次 `XAUTOCLAIM min_idle=0` 收编当前 PEL(含 retryable 子集 + resume 重发的新
            // ID 仍走 `>`),收编完才 Ready——同时覆盖进程重启/命令由他节点发起/本地 deferred 丢失。
            if let Some(slot) = slots.get_mut(&p) {
                if matches!(slot.state, ClaimState::Parked { .. }) {
                    slot.generation += 1;
                    let gen = slot.generation;
                    slot.retry_attempt = 0;
                    slot.state = ClaimState::Recovering { generation: gen };
                    let rec_cancel = slot.worker_cancel.clone();
                    let rt2 = Arc::clone(rt);
                    //:存 JoinHandle 进 slot(覆盖上一个已完成的);drain/release 等其退出
                    let h = tokio::spawn(async move {
                        let ids = super::retry::recover_pel(&rt2, p, &rec_cancel).await;
                        if !rec_cancel.is_cancelled() {
                            let _ = rt2
                                .event_tx
                                .send(Event::RecoveryFinished {
                                    p,
                                    generation: gen,
                                    ids,
                                })
                                .await;
                        }
                    });
                    slot.recovery_handle = Some(AbortOnDropTask::new(h));
                    tracing::info!(p, "Park 解除 → Recovering(先收编残留 PEL 再 Ready)");
                }
            }
        }
        Event::OrphansClaimed { p, generation, ids } => {
            // 后台 sweep 收编到滞留 PEL → 仅当 slot 仍 Ready/Backoff **且 generation 匹配**
            // 才转 RetryBackoff;否则(丢锁移除/InFlight/Recovering/Parked/换代)忽略作废。
            if ids.is_empty() {
                return;
            }
            if let Some(slot) = slots.get_mut(&p) {
                if matches!(slot.state, ClaimState::Ready | ClaimState::Backoff { .. })
                    && slot.generation == generation
                {
                    tracing::warn!(
                        p,
                        n = ids.len(),
                        "orphan sweep 收编滞留 PEL(疑旧 owner 丢锁后迟到 poll),转重投"
                    );
                    slot.retry_attempt = 0;
                    slot.state = ClaimState::RetryBackoff {
                        ids,
                        attempt: 0,
                        // sweep 收编同样 1s 平滑,避免一次性重投风暴
                        until: tokio::time::Instant::now() + Duration::from_millis(1_000),
                    };
                }
            }
        }
        Event::Shutdown { done } => {
            *shutdown_ack = Some(done); // drain 完成后在主循环回 ack
        }
    }
}
