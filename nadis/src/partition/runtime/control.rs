use super::*;
use futures::StreamExt;
use std::time::Duration;

/// 周期消费本节点持有分区的 command stream:
/// · admin group(nasa-park-admin)与业务 group 分离,consumer = node_id;
/// · **不经业务 ClaimState**:Parked 分区的 Resume 命令照常被读取(修自锁);
/// · 旧 owner 未 ACK 的命令经 XAUTOCLAIM 接管(Partition 持锁语义下 admin group
///   同样单 owner,无 owner 混淆——XAUTOCLAIM 在 Partition 协议内允许);
/// · 执行序:execute_one(去重/deadline/动作/先写 result)→ XACK+XDEL 命令 entry。
pub(super) async fn control_loop(rt: Arc<GroupRuntime>) {
    use std::collections::HashSet;
    let mut ensured: HashSet<u32> = HashSet::new(); // 已建 admin group 的分区
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = rt.bg_cancel.cancelled() => return, // 停机:未读命令留 stream 交新 owner
            _ = tick.tick() => {}
        }
        let owned: Vec<u32> = rt.claimed.read().expect("claimed").clone();
        for p in owned {
            let cstream = super::command::cmd_stream_key(&rt.layout, p);
            // 建 admin group(from 0:接管前已写入的命令也要消费;BUSYGROUP 幂等)
            if !ensured.contains(&p) {
                let r: std::result::Result<String, redis::RedisError> = redis::cmd("XGROUP")
                    .arg("CREATE")
                    .arg(&cstream)
                    .arg(super::command::ADMIN_GROUP)
                    .arg("0")
                    .arg("MKSTREAM")
                    .query_async(&mut rt.client.conn())
                    .await;
                match r {
                    Ok(_) => {
                        ensured.insert(p);
                    }
                    Err(e) if e.to_string().contains("BUSYGROUP") => {
                        ensured.insert(p);
                    }
                    Err(e) => {
                        tracing::warn!(p, err = %e, "admin group 创建失败");
                        continue;
                    }
                }
            }
            // ①新命令(>)②旧 owner 残留(XAUTOCLAIM idle>15s)——两路合并处理
            let mut entries: Vec<(String, String)> = Vec::new(); // (entry_id, op_id)
            let fresh: std::result::Result<redis::Value, _> = redis::cmd("XREADGROUP")
                .arg("GROUP")
                .arg(super::command::ADMIN_GROUP)
                .arg(&rt.node_id)
                .arg("COUNT")
                .arg(8)
                .arg("STREAMS")
                .arg(&cstream)
                .arg(">")
                .query_async(&mut rt.client.conn())
                .await;
            match fresh {
                Ok(v) => collect_cmd_entries(v, &mut entries),
                Err(e) => {
                    //**NOGROUP 自愈**——admin group 被外部清理后,从 ensured 移除
                    // p,下一轮重建(否则缓存命中、永不重建,该分区管理命令永久读不到)。
                    if e.to_string().contains("NOGROUP") {
                        ensured.remove(&p);
                        tracing::warn!(p, "control admin group 丢失(NOGROUP),下轮重建");
                    } else {
                        tracing::warn!(p, err = %e, "control 读新命令(>)失败,下轮重试");
                    }
                }
            }
            //XAUTOCLAIM **cursor 分页**(每轮最多 PAGE_BUDGET 页)——长 PEL
            // 不再"每轮只处理一页且每轮从 0 重扫",显著缩短旧命令恢复延迟;预算上限防饿死新命令。
            const PAGE_BUDGET: u32 = 4;
            let mut cursor = "0".to_string();
            let mut pages = 0u32;
            loop {
                let claimed: std::result::Result<redis::Value, _> = redis::cmd("XAUTOCLAIM")
                    .arg(&cstream)
                    .arg(super::command::ADMIN_GROUP)
                    .arg(&rt.node_id)
                    .arg(15_000)
                    .arg(&cursor)
                    .arg("COUNT")
                    .arg(8)
                    .query_async(&mut rt.client.conn())
                    .await;
                let parts = match claimed {
                    Ok(redis::Value::Array(parts)) => parts,
                    Ok(_) => break,
                    Err(e) => {
                        if e.to_string().contains("NOGROUP") {
                            ensured.remove(&p); //:NOGROUP 自愈,下轮重建 admin group
                            tracing::warn!(p, "control XAUTOCLAIM 遇 NOGROUP,下轮重建 admin group");
                        } else {
                            tracing::warn!(p, err = %e, "control XAUTOCLAIM 失败,下轮重试");
                        }
                        break;
                    }
                };
                // XAUTOCLAIM 返回 [next-cursor, entries(, deleted-ids)]
                let next = match parts.first() {
                    Some(redis::Value::BulkString(b)) => String::from_utf8_lossy(b).into_owned(),
                    Some(redis::Value::SimpleString(s)) => s.clone(),
                    _ => "0".to_string(),
                };
                if let Some(ev) = parts.into_iter().nth(1) {
                    collect_cmd_entries(
                        redis::Value::Array(vec![redis::Value::Array(vec![
                            redis::Value::BulkString(cstream.clone().into_bytes()),
                            ev,
                        ])]),
                        &mut entries,
                    );
                }
                pages += 1;
                // 游标回 0 = 一轮扫完;达页预算 → 停(下轮 cursor 重 0 续扫剩余,已处理项已出 PEL)
                if next == "0" || next == "0-0" || pages >= PAGE_BUDGET {
                    break;
                }
                cursor = next;
            }
            for (entry_id, op_id) in entries {
                //run_command 返回是否可终结。基础设施错误(Redis/网络)
                // 时保留 command PEL(不 ACK/XDEL),交下一轮或新 owner 重试,
                // 不把故障误当 Rejected 终态删除——保住 at-least-once。
                let should_finalize = run_command(&rt, p, &op_id).await;
                if !should_finalize {
                    continue;
                }
                // 先 result(run_command 内)后 ACK+XDEL。
                //**观测 XACK/XDEL 结果**——XACK 失败下轮经 PEL 重 finalize;
                // XACK 成功但 XDEL 失败 → entry 永留 stream(auto-trim 未实现)→ 告警留痕。
                let ack: std::result::Result<i64, _> = redis::cmd("XACK")
                    .arg(&cstream)
                    .arg(super::command::ADMIN_GROUP)
                    .arg(&entry_id)
                    .query_async(&mut rt.client.conn())
                    .await;
                if let Err(e) = &ack {
                    tracing::warn!(p, entry_id, err = %e, "control XACK 失败,保留 PEL 下轮重 finalize");
                    continue; // 不 XDEL:entry 仍 pending,下轮经 XAUTOCLAIM 重新收敛
                }
                let del: std::result::Result<i64, _> = redis::cmd("XDEL")
                    .arg(&cstream)
                    .arg(&entry_id)
                    .query_async(&mut rt.client.conn())
                    .await;
                if let Err(e) = &del {
                    tracing::warn!(p, entry_id, err = %e, "control XDEL 失败(已 XACK,entry 残留 stream 待 trim)");
                }
            }
        }
    }
}

/// 解析 command stream 的 XREADGROUP/XAUTOCLAIM 响应,抽取 (entry_id, op 字段)。
/// **RESP2/RESP3 双形态**(RESP3 下命令通道半适配——本函数是第三个
/// XREADGROUP 解析器,第 25 轮只修了 poll.rs/proxy.rs 两处,漏了这里):
/// - XREADGROUP `>` 顶层:RESP2 `[[stream,[entries]],...]`(部分形态 Set)/ RESP3 `{stream:[entries]}`(Map);
/// - XREADGROUP NOBLOCK 无新消息:`Nil`(空轮,非错误);
/// - entry/fields 段:两协议都是 Array(本地 `?protocol=resp3` 实测确认,XAUTOCLAIM 同样顶层仍是 Array)。
///
/// XAUTOCLAIM 路径由调用方包成合成 `Array([[key, entries]])` 走 Array 臂,不受影响。
pub(super) fn collect_cmd_entries(v: redis::Value, out: &mut Vec<(String, String)>) {
    use redis::Value as V;
    match v {
        V::Nil => {} // NOBLOCK 无新消息:正常空轮
        // RESP2:每个 stream 是 [key, entries](部分形态 Set);取第 2 段 entries。
        V::Array(streams) | V::Set(streams) => {
            for sv in streams {
                let (V::Array(pair) | V::Set(pair)) = sv else {
                    continue;
                };
                if let Some(entries_v) = pair.into_iter().nth(1) {
                    collect_cmd_stream_entries(entries_v, out);
                }
            }
        }
        // RESP3(HELLO 3 / ?protocol=resp3):顶层是 {stream: entries} Map。
        V::Map(pairs) => {
            for (_key, entries_v) in pairs {
                collect_cmd_stream_entries(entries_v, out);
            }
        }
        _ => {}
    }
}

/// 从单个 stream 的 entries 段抽取 (entry_id, op 字段)。entries/entry/fields 段两协议均为 Array。
///
/// # 参数
/// - `entries_v`: 从 Redis 读取到的 stream 条目集合。
/// - `out`: 输出缓冲区,用于收集解析结果。
fn collect_cmd_stream_entries(entries_v: redis::Value, out: &mut Vec<(String, String)>) {
    use redis::Value as V;
    let (V::Array(entries) | V::Set(entries)) = entries_v else {
        return;
    };
    for e in entries {
        let (V::Array(ep) | V::Set(ep)) = e else {
            continue;
        };
        let mut it = ep.into_iter();
        let (Some(idv), Some(V::Array(fields) | V::Set(fields))) = (it.next(), it.next()) else {
            continue;
        };
        let id = String::from_utf8_lossy(&value_bytes(idv)).into_owned();
        let mut fit = fields.into_iter();
        while let (Some(f), Some(val)) = (fit.next(), fit.next()) {
            if value_bytes(f) == b"op" {
                out.push((
                    id.clone(),
                    String::from_utf8_lossy(&value_bytes(val)).into_owned(),
                ));
            }
        }
    }
}

/// owner 执行一条命令(动作映射 + 成功后唤醒 coordinator)。
/// 返回 `true` = 可终结(ACK+XDEL command);`false` = 基础设施错误,保留 PEL 重试。
pub(super) async fn run_command(rt: &Arc<GroupRuntime>, p: u32, op_id: &str) -> bool {
    use super::command::{execute_one, query, CmdAck, CmdAction};
    // 预读 result 决定动作(execute_one 内部还会做去重/deadline 的权威判定)
    let pre = match query(&rt.client, &rt.layout, p, op_id).await {
        Ok(Some(pre)) => pre,
        // 无 result:孤儿 command(producer 违规),清理掉(留 PEL 无意义)
        Ok(None) => {
            tracing::warn!(p, op_id, "命令缺 result(producer 违规),清理");
            return true;
        }
        // 查询失败(Redis/网络):保留 PEL 下轮重试
        Err(e) => {
            tracing::warn!(p, op_id, err = %e, "命令 result 查询失败,保留 PEL 重试");
            return false;
        }
    };
    let action = pre.action;
    // owner-fenced:control loop 执行管理动作前必须证明本节点仍是 p 的 owner;
    // owner_ctx 无该分区(失锁/未持有)→ 不执行副作用,保留 PEL 交新 owner(命令幂等)。
    //:用**命令的稳定 operation_id** 构造凭据——同 op_id 重试幂等续作、异 op_id 不接管在途
    let of = match rt.require_owner_for_op(p, op_id) {
        Ok(of) => of,
        Err(_) => {
            tracing::warn!(
                p,
                op_id,
                "control 执行命令时本节点非 owner(owner_ctx 缺),保留 PEL"
            );
            return false;
        }
    };
    let fc = of.lease();
    let fut = async {
        match action {
            CmdAction::Resume => super::disposition::resume(&rt.client, &rt.layout, p, &fc).await,
            CmdAction::Force => {
                super::disposition::force_republish(&rt.client, &rt.layout, p, &fc).await
            }
            CmdAction::Drop => super::disposition::drop_parked(&rt.client, &rt.layout, p, &fc)
                .await
                .map(|_| Vec::new()),
            CmdAction::Dlq => {
                super::disposition::dlq_from_parked(&rt.client, &rt.layout, p, &fc).await
            }
        }
    };
    match execute_one(&rt.client, &rt.layout, p, op_id, fut, action).await {
        Ok(CmdAck::Finalize) => {
            // 终态已写;若动作成功(Succeeded)→ 通知 coordinator 解除 Parked
            if let Ok(Some(r)) = query(&rt.client, &rt.layout, p, op_id).await {
                if matches!(r.state, super::command::CmdState::Succeeded) {
                    let _ = rt.event_tx.send(Event::ParkResolved { p }).await;
                }
            }
            true
        }
        // 基础设施错误:保留 PEL,不 ACK
        Ok(CmdAck::Retry) => false,
        // execute_one 自身编排出错(Redis 写 result 失败等):同样保留 PEL
        Err(e) => {
            tracing::warn!(p, op_id, err = %e, "命令执行编排异常(result 留 Executing),保留 PEL 重试");
            false
        }
    }
}

/// 后台 orphan sweep actor:周期对本节点持有分区做
/// `XAUTOCLAIM min_idle` 收编滞留 PEL,经 `Event::OrphansClaimed` 回灌 coordinator——
/// **Redis I/O 不在 coordinator 主循环上**,不阻塞 shutdown/lost/事件处理。
/// 串行扫描(每分区一次,backlog 多页);独立 cancel 跟随 rt.cancel。
pub(super) async fn sweep_loop(rt: Arc<GroupRuntime>) {
    let interval = Duration::from_millis(rt.cfg.min_idle_ms.max(1_000));
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = rt.bg_cancel.cancelled() => return,
            _ = tick.tick() => {}
        }
        //**只扫 coordinator 发布的 Ready/Backoff 票据**(带 claim generation),
        // 不再扫全部 claimed——避免对 InFlight/Recovering/RetryBackoff/Parked 分区 XAUTOCLAIM
        // 改 PEL owner/idle;收编结果带 generation 回灌,coordinator 换代/换态即作废。
        let tickets: Vec<(u32, u64)> = rt.sweepable.lock().expect("sweepable").clone();
        for (p, generation) in tickets {
            if rt.bg_cancel.is_cancelled() {
                return;
            }
            //执行前**重校验 ticket 仍有效**——slot 仍在 sweepable 同代(未转
            // InFlight/Recovering/Parked/换代)且本节点仍持有(owner_ctx)。否则跳过,不动其 PEL。
            let valid = rt
                .sweepable
                .lock()
                .expect("sweepable")
                .iter()
                .any(|&(q, g)| q == p && g == generation)
                && rt.owner_ctx.lock().expect("owner_ctx").contains_key(&p);
            if !valid {
                continue;
            }
            let ids = super::retry::sweep_partition(&rt, p, rt.cfg.min_idle_ms).await;
            // 回灌前再确认本节点仍持有(缩窄 XAUTOCLAIM 后→回灌前的窗口;generation 仍由 coordinator 二次校验)
            if !ids.is_empty() && rt.owner_ctx.lock().expect("owner_ctx").contains_key(&p) {
                let _ = rt
                    .event_tx
                    .send(Event::OrphansClaimed { p, generation, ids })
                    .await;
            }
        }
    }
}

/// wake 订阅:收到信号立即补一轮再平衡(加速通道;正确性靠周期兜底)。
pub(super) async fn wake_loop(rt: Arc<GroupRuntime>) {
    loop {
        if rt.bg_cancel.is_cancelled() {
            return;
        }
        // 独立 Pub/Sub 连接;断开后循环重建
        let Ok(mut pubsub) = rt.client.raw_client().get_async_pubsub().await else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        if pubsub.subscribe(rt.layout.wake()).await.is_err() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        let mut msgs = pubsub.on_message();
        loop {
            tokio::select! {
                _ = rt.bg_cancel.cancelled() => return,
                m = msgs.next() => {
                    if m.is_none() { break; } // 连接断:重建订阅
                    let _ = rebalance_once(&rt).await;
                }
            }
        }
    }
}
