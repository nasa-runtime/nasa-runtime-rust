use super::*;
use std::time::Duration;

/// 业务作用：周期执行分区再平衡，并在单轮失败后等待下一轮重试。
pub(super) async fn rebalance_loop(rt: Arc<GroupRuntime>) {
    let period = Duration::from_millis(rt.cfg.rebalance_ms);
    // 直接使用配置周期(原 period.min(1s) 把生产默认 10s 反写成
    // 1s,心跳/ZREM/抢锁扫描放大 10 倍;开发想要快节奏就显式配小 rebalance_ms)
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = rt.bg_cancel.cancelled() => return,
            _ = tick.tick() => {}
        }
        if let Err(e) = rebalance_once(&rt).await {
            tracing::warn!(err = %e, "再平衡轮次失败,下轮重试");
        }
    }
}

/// 业务作用：单轮再平衡(也被 wake 触发调用):
/// ①心跳:ZADD nodes score=墙钟 now+3*period(V1 语义,与 原实现 混跑兼容);
/// ②清过期成员(score < now)→ ZCARD = alive;③fair = ceil(count/alive);
/// ④欠额:扫描未持有分区 tryLock 抢占(锁互斥保证全集群唯一 owner);
/// ⑤超额:发 ReleaseExcess 给 coordinator(释放后 pub wake 让兄弟立刻接管)。
pub(super) async fn rebalance_once(rt: &Arc<GroupRuntime>) -> Result<()> {
    //single-flight——周期任务与 wake 任务都会调本函数,拿不到守卫说明
    // 已有一轮在跑,本次合并跳过(避免并发心跳/抢锁/重复 ReleaseExcess)。
    let _guard = match rt.rebalance_lock.try_lock() {
        Ok(g) => g,
        Err(_) => return Ok(()),
    };
    let nodes_key = rt.layout.nodes();
    // 心跳时钟按 profile 分流
    //   原实现V1 = 墙钟(与 原实现 节点混跑时 score 语义必须一致);
    //   RustV2 = Redis TIME(全集群同一时钟源,节点墙钟漂移不再误判存活——
    //            每轮多 1 个 TIME RTT,rebalance 周期 10s 量级可忽略)。
    let now_ms: f64 = if rt.client.profile() == crate::config::CompatibilityProfile::RustV2 {
        super::command::redis_now(&rt.client).await? as f64
    } else {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("墙钟早于 epoch")
            .as_millis() as f64
    };
    let expire_at = now_ms + (3 * rt.cfg.rebalance_ms) as f64;

    // ① 心跳 + ② 清过期 + alive
    rt.client
        .z_add(&nodes_key, expire_at, rt.node_id.as_str())
        .await?;
    rt.client
        .z_rem_range_by_score(&nodes_key, f64::NEG_INFINITY, now_ms)
        .await?;
    let alive = rt.client.z_card(&nodes_key).await?.max(1);

    // ③ fair(ceil 除法)
    let fair = rt.count.div_ceil(alive as u32).max(1);
    let owned: Vec<u32> = rt.claimed.read().expect("claimed").clone();

    if (owned.len() as u32) < fair {
        // ④ 欠额抢占:从 0..count 顺扫未持有的分区(锁互斥天然防双持)
        let mut acquired = 0u32;
        let need = fair - owned.len() as u32;
        for p in 0..rt.count {
            if acquired >= need || rt.bg_cancel.is_cancelled() {
                break;
            }
            if owned.contains(&p) {
                continue;
            }
            if let Some(guard) = rt.lock.try_lock(&rt.layout.lock_business_key(p)).await? {
                acquired += 1;
                let _ = rt.event_tx.send(Event::ClaimAcquired { p, guard }).await;
            }
        }
    } else if (owned.len() as u32) > fair {
        // ⑤ 超额让出:只把 ReleaseExcess 交给 coordinator。**wake 由 coordinator 在
        //   victim 真正 unlock 之后发布**,本任务不再在此 pub wake
        //   ——否则锁还没释放就唤醒兄弟,抢锁必失败。
        let excess = owned.len() as u32 - fair;
        let _ = rt
            .event_tx
            .send(Event::ReleaseExcess { n: excess as usize })
            .await;
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// 管理命令 control loop(协议见 command.rs 文件头)
// ─────────────────────────────────────────────────────────────────────────
