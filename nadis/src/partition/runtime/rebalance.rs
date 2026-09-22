use super::*;
use std::time::Duration;

/// 业务作用：周期执行分区再平衡，并在单轮失败后等待下一轮重试。
/// 参数说明：`rt` 为物理分区组的运行状态。
/// 返回：后台停止时退出；失败轮次不改变已持有来源的提交责任。
pub(super) async fn rebalance_loop(rt: Arc<GroupRuntime>) {
    let period = Duration::from_millis(rt.cfg.rebalance_ms);
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = rt.bg_cancel.cancelled() => {
                return;
            }
            _ = tick.tick() => {
            }
        }
        if let Err(e) = rebalance_once(&rt).await {
            tracing::warn!(err = %e, "再平衡轮次失败,下轮重试");
        }
    }
}

/// 业务作用：刷新组心跳并按存活节点分配份额，取得来源或提交幂等的保留目标。
/// 参数说明：`rt` 为周期或 wake 触发再平衡的物理分区组。
/// 返回：Redis 失败时上抛；锁释放由 coordinator 在来源排干后完成。
pub(super) async fn rebalance_once(rt: &Arc<GroupRuntime>) -> Result<()> {
    // 周期与 wake 共用单轮互斥，合并并发触发，避免同时发布相互竞争的持有决策。
    let _guard = match rt.rebalance_lock.try_lock() {
        Ok(g) => g,
        Err(_) => return Ok(()),
    };
    let nodes_key = rt.layout.nodes();
    // LegacyV1 与 Java 节点共享墙钟 score；RustV2 使用 Redis TIME 避免节点时钟漂移影响存活判断。
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
        // claimed 包含仍在排干的持锁来源，只能用来触发份额归约，不能据此重复指定让出数量。
        // coordinator 以当前 Active 数量执行保留目标，并在真实 unlock 后发布 wake。
        let _ = rt
            .event_tx
            .send(Event::RetainShare {
                target: fair as usize,
            })
            .await;
    }
    Ok(())
}
