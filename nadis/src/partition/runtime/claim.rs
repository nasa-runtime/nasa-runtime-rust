//! Claim 只拥有来源权威、读取监督和锁释放责任。

use super::engine::SourceAuthority;
use super::*;
use std::{collections::HashMap, time::Duration};

/// 业务作用：归约来源取得、失权、让出与停止，不等待或执行业务 handler。
///
/// 参数说明：
/// - `rt`: 持有组配置、来源账本、锁及停机信号的运行时。
/// - `event_rx`: 接收 Claim 取得、份额调整及处置完成线索的有界事件通道。
///
/// 返回：组停止且全部 Claim 已退出时结束；有效租约只在读取与记录责任收口后释放。
/// 释放结果未知或已经失权的来源保留租约对账责任；本循环结束不替代对账收敛证明。
pub(super) async fn coordinator_loop(rt: Arc<GroupRuntime>, mut event_rx: mpsc::Receiver<Event>) {
    let mut slots: HashMap<u32, ClaimSlot> = HashMap::new();
    loop {
        while let Ok(event) = event_rx.try_recv() {
            handle_event(&rt, &mut slots, event).await;
        }
        if rt.cancel.is_cancelled() {
            // 先禁止各来源开始新业务，再等待已有记录与读取任务收口，保留成功业务的提交窗口。
            for slot in slots.values() {
                slot.source.quiesce();
            }
        }
        // 退出来源必须同时没有记录责任和仍运行的读取者，不能只凭停止标记就把锁交给下一 owner。
        let finished: Vec<_> = slots
            .iter()
            .filter_map(|(p, slot)| {
                if slot.source.active()
                    && slot
                        .reader
                        .as_ref()
                        .is_some_and(|reader| reader.is_finished())
                {
                    // 读者异常退出后不能继续宣告来源健康，也不能重新绑定仍有本地责任的坐标。
                    rt.core.degraded.store(true, Ordering::Release);
                    slot.source.quiesce();
                }
                if !slot.source.can_commit() {
                    slot.source.lose();
                }
                (!slot.source.active()
                    && rt.core.drained(&slot.source)
                    && slot.reader.as_ref().is_none_or(|h| h.is_finished()))
                .then_some(*p)
            })
            .collect();
        let mut released = false;
        for partition in finished {
            let mut slot = slots.remove(&partition).expect("claim");
            if let Some(reader) = slot.reader.take() {
                let _ = reader.await;
            }
            rt.owner_ctx
                .lock()
                .expect("owner context")
                .remove(&partition);
            if let Some(guard) = slot.guard.take() {
                // 取得读取与记录退出证明后才允许 unlock，失权来源只停止续租。
                if slot.source.can_commit() {
                    slot.source.begin_release();
                    if guard.unlock().await.is_err() {
                        // unlock 回包未知时保留租约对账责任，不能把本地 guard 析构当作服务端释放证明。
                        rt.core.abandoned_source(&slot.source);
                    } else {
                        slot.source.unreleased.store(false, Ordering::Release);
                        released = true;
                    }
                } else {
                    rt.core.abandoned_source(&slot.source);
                    guard.abandon();
                }
            }
            rt.claimed
                .write()
                .expect("claimed")
                .retain(|p| *p != partition);
        }
        if released {
            // 真实 unlock 与本地持有表更新之后再唤醒竞争者；未知释放和失权不能作为锁已空闲的证据。
            let _: redis::RedisResult<i64> = redis::cmd("PUBLISH")
                .arg(rt.layout.wake())
                .arg("")
                .query_async(&mut rt.client.conn())
                .await;
        }
        if rt.cancel.is_cancelled() && slots.is_empty() {
            return;
        }
        tokio::select! {
            event = event_rx.recv() => {
                if let Some(event) = event {
                    handle_event(&rt, &mut slots, event).await;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {
            }
        }
    }
}

/// 业务作用：复验取得与管理通知，把来源读取监督绑定到不可变任期。
///
/// 参数说明：
/// - `rt`: 分区组运行时与当前控制权威。
/// - `slots`: 持有读取监督与记录排空责任的现役 Claim。
/// - `event`: 待核对来源身份后归约的管理事件。
///
/// 返回：停机事件不会重开来源，fencing/disposition 证据不足时放弃接管。
/// 处置通知仅触发重新核对，持久冻结仍存在、查询失败或本地隔离修订变化时不直接解除保护。
async fn handle_event(rt: &Arc<GroupRuntime>, slots: &mut HashMap<u32, ClaimSlot>, event: Event) {
    match event {
        Event::ClaimAcquired {
            p,
            guard,
        } => {
            // 重复 Claim 或已关闭的组不能发布新的来源，未交接的守卫在此释放。
            if slots.contains_key(&p)
                || rt.cancel.is_cancelled()
                || rt.core.roots_closed.load(Ordering::Acquire)
            {
                let _ = guard.unlock().await;
                return;
            }
            // 本地身份耗尽时禁止复用任期，避免迟到记录被误认成新 Claim 的责任。
            let Some(identity) = BatchIdentity::new_claim() else {
                let _ = guard.unlock().await;
                return;
            };
            // fencing 任期必须先于来源发布，缺少权威时不接管 PEL。
            let stamp = match &rt.fence_meta {
                Some(meta) => match fencing::acquire_stamp(&rt.client, &rt.layout, meta, p).await {
                    Ok(stamp) => Some(stamp),
                    Err(_) => {
                        let _ = guard.unlock().await;
                        return;
                    }
                },
                None => None,
            };
            let parked = match disposition::takeover_disposition(&rt.client, &rt.layout, p).await {
                Ok(disposition::TakeoverDisposition::NotParked) => false,
                Ok(disposition::TakeoverDisposition::Frozen {
                    ..
                }) => true,
                Ok(disposition::TakeoverDisposition::AutoResumeDlq {
                    park_id,
                    operation_id,
                }) => {
                    let fence_key = rt
                        .fence_meta
                        .as_ref()
                        .map(|_| fencing::fence_key(&rt.layout))
                        .unwrap_or_default();
                    // 自动续作依据来自同一次 marker 快照，等待持有权和 Redis 期间不能改为处置新批次。
                    let lease = disposition::OwnerLease {
                        partition: p,
                        operation_id: &operation_id,
                        expected_park_id: Some(&park_id),
                        holder: guard.holder(),
                        lock_key: guard.lock_key(),
                        fence_key: &fence_key,
                        round: rt.fence_meta.as_ref().map_or(0, |m| m.round),
                        nonce: rt.fence_meta.as_ref().map_or("", |m| m.nonce.as_str()),
                        counter: stamp.as_ref().map_or(0, |s| s.counter),
                    };
                    disposition::dlq_from_parked(&rt.client, &rt.layout, p, &lease)
                        .await
                        .is_err()
                }
                _ => {
                    // 持久冻结证据不一致或读取失败时不猜测为可消费状态，释放尚未发布的 Claim。
                    let _ = guard.unlock().await;
                    return;
                }
            };
            let source = SourceAuthority::new(rt, p, identity, &guard, stamp.clone());
            // 先写入冻结状态及 owner 凭据，再登记来源和启动读者，读取者不能看到未经恢复核对的半完成 Claim。
            source.parked.store(parked, Ordering::Release);
            rt.owner_ctx.lock().expect("owner context").insert(
                p,
                OwnerCtx {
                    holder: guard.holder().into(),
                    counter: stamp.as_ref().map_or(0, |s| s.counter),
                },
            );
            rt.core.register_source(&source);
            let reader = tokio::spawn(source::source_loop(rt.clone(), source.clone()));
            slots.insert(
                p,
                ClaimSlot {
                    source,
                    guard: Some(guard),
                    reader: Some(reader),
                },
            );
            rt.claimed.write().expect("claimed").push(p);
        }
        Event::RetainShare {
            target,
        } => {
            // 已进入排干的来源仍持锁，但不再占用要保留的 Active 份额。
            // 按当前状态归约绝对目标，使重复通知及延迟到达的通知不会再次让出同一份超额。
            // Park 仍占持有份额，不能把人工冻结当作可自动释放的来源。
            let active = slots.values().filter(|slot| slot.source.active()).count();
            let excess = active.saturating_sub(target);
            // 让出先关闭下一条业务，提交窗口继续开放到已登记成功事实结清。
            for slot in slots
                .values()
                .filter(|slot| slot.source.can_start())
                .take(excess)
            {
                slot.source.quiesce();
            }
        }
        Event::ParkResolved {
            p,
        } => {
            let Some(source) = slots.get(&p).map(|slot| slot.source.clone()) else {
                return;
            };
            loop {
                // 通知可能来自早已完成的旧批次，失权、让出或停止中的来源不能因通知重新开放。
                if !source.active()
                    || rt.bg_cancel.is_cancelled()
                    || rt.cancel.is_cancelled()
                    || rt.core.roots_closed.load(Ordering::Acquire)
                {
                    return;
                }
                if source.io.load(Ordering::Acquire) == 0 {
                    let revision = source.park_revision.load(Ordering::Acquire);
                    // 持久处置仍冻结或证据不完整时保留本地保护，成功通知本身不是解除权限。
                    if !matches!(
                        disposition::takeover_disposition(&rt.client, &rt.layout, p).await,
                        Ok(disposition::TakeoverDisposition::NotParked)
                    ) {
                        return;
                    }
                    // 查询期间形成了新隔离或开始了 I/O 时不采用旧结果，等待责任收口后重新读取。
                    if rt.core.resume_source(&source, revision) {
                        return;
                    }
                }
                tokio::select! {
                    _ = rt.bg_cancel.cancelled() => return,
                    _ = rt.cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
            }
        }
    }
}
