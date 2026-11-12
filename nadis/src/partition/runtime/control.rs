use super::*;
use futures::StreamExt;
use std::time::Duration;

/// 业务作用：从独立管理消费组读取本节点持有分区的处置命令，包含仍在隔离中的来源。
///
/// 参数说明：
/// - `rt`: 提供持有分区快照、节点身份、管理命令布局和后台停止信号的消费组运行时。
///
/// 返回：后台停止后结束；每轮读取新命令并分页接管旧 PEL，实际动作仍需重新取得持有权证据。
/// 先持久化命令终态再确认和删除命令条目；基础设施错误保留 PEL，已确认但删除失败的正文仍留在 Stream。
/// 创建消费组只接受 OK 或精确 BUSYGROUP，缺组只认 NOGROUP；非法读取证据不交给动作入口。
pub(super) async fn control_loop(rt: Arc<GroupRuntime>) {
    use std::collections::HashSet;
    let mut ensured: HashSet<u32> = HashSet::new();
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
            // 管理组从头读取，接管前写入的处置请求仍可消费；与业务组分开才能解除隔离来源。
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
                    Ok(reply) if reply == "OK" => {
                        ensured.insert(p);
                    }
                    Err(e) if e.code() == Some("BUSYGROUP") => {
                        ensured.insert(p);
                    }
                    Ok(_) => {
                        tracing::warn!(p, "admin group 创建响应非法");
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(p, err = %e, "admin group 创建失败");
                        continue;
                    }
                }
            }
            // 每轮先读取新请求，再以有限页数接管空闲 PEL，避免旧积压阻塞新管理动作。
            let mut entries: Vec<(String, String)> = Vec::new();
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
                Ok(v) => {
                    if let Err(error) = collect_cmd_entries(v, &cstream, &mut entries) {
                        tracing::warn!(p, err = %error, "control 新命令响应非法，保留待恢复");
                    }
                }
                Err(e) => {
                    // 管理组缺失时撤销本地建组缓存，下一轮重建后才能继续读取。
                    if e.code() == Some("NOGROUP") {
                        ensured.remove(&p);
                        tracing::warn!(p, "control admin group 丢失(NOGROUP),下轮重建");
                    } else {
                        tracing::warn!(p, err = %e, "control 读新命令(>)失败,下轮重试");
                    }
                }
            }
            // XAUTOCLAIM 沿 cursor 连续读取，每轮限制 PAGE_BUDGET 页，避免长 PEL 回收饿死新命令。
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
                let value = match claimed {
                    Ok(value) => value,
                    Err(e) => {
                        if e.code() == Some("NOGROUP") {
                            ensured.remove(&p);
                            tracing::warn!(p, "control XAUTOCLAIM 遇 NOGROUP,下轮重建 admin group");
                        } else {
                            tracing::warn!(p, err = %e, "control XAUTOCLAIM 失败,下轮重试");
                        }
                        break;
                    }
                };
                // 游标与删除引用都是本页完整性证据；不能先发布条目再发现尾部非法，或以替换文本继续扫描。
                let page = (|| -> Result<(String, Vec<(String, String)>)> {
                    let mut parts = wire::array(value)?;
                    if !matches!(parts.len(), 2 | 3) {
                        return Err(NasaRedisError::Parsing("invalid command claim page".into()));
                    }
                    let next = wire::id(parts.remove(0))?;
                    if parts.len() == 2 {
                        for deleted in wire::array(parts.pop().expect("deleted ids"))? {
                            wire::id(deleted)?;
                        }
                    }
                    let mut page = Vec::new();
                    collect_cmd_stream_entries(parts.pop().expect("entries"), &mut page)?;
                    Ok((next, page))
                })();
                let next = match page {
                    Ok((next, page)) => {
                        entries.extend(page);
                        next
                    }
                    Err(error) => {
                        tracing::warn!(p, err = %error, "control 接管响应非法，保留待恢复");
                        break;
                    }
                };
                pages += 1;
                // 游标回 0 = 一轮扫完;达页预算 → 停(下轮 cursor 重 0 续扫剩余,已处理项已出 PEL)
                if next == "0-0" || pages >= PAGE_BUDGET {
                    break;
                }
                cursor = next;
            }
            for (entry_id, op_id) in entries {
                // 基础设施错误不构成命令终态；保留 PEL 让当前或后继 owner 继续幂等执行。
                let should_finalize = run_command(&rt, p, &op_id).await;
                if !should_finalize {
                    continue;
                }
                // 只有持久终态或已无结果记录的孤儿请求才能确认；确认失败时不能删除仍待接管的正文。
                // 确认成功后删除失败不撤销命令结果，残留正文只记录告警。
                let ack: std::result::Result<i64, _> = redis::cmd("XACK")
                    .arg(&cstream)
                    .arg(super::command::ADMIN_GROUP)
                    .arg(&entry_id)
                    .query_async(&mut rt.client.conn())
                    .await;
                // 一条 ID 的确认数只能是零或一；数量异常不能证明 PEL 已收口，因此不得继续删除正文。
                match ack {
                    Ok(0 | 1) => {}
                    Ok(_) => {
                        tracing::warn!(p, entry_id, "control XACK 数量非法，保留待恢复");
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(p, entry_id, err = %e, "control XACK 失败,保留 PEL 下轮重 finalize");
                        continue;
                    }
                }
                let del: std::result::Result<i64, _> = redis::cmd("XDEL")
                    .arg(&cstream)
                    .arg(&entry_id)
                    .query_async(&mut rt.client.conn())
                    .await;
                match del {
                    Ok(0 | 1) => {}
                    Ok(_) => {
                        tracing::warn!(p, entry_id, "control XDEL 数量非法，已确认正文可能残留");
                    }
                    Err(e) => {
                        tracing::warn!(p, entry_id, err = %e, "control XDEL 失败(已 XACK,entry 残留 stream 待 trim)");
                    }
                }
            }
        }
    }
}

/// 业务作用：在单一管理 Stream 坐标内完整解析命令读取响应。
///
/// 参数说明：
/// - `v`: RESP2 序列或 RESP3 映射形式的 Stream 响应；XAUTOCLAIM 条目由调用方包成相同形态。
/// - `stream`: 本次读取唯一允许出现的完整管理 Stream 键。
/// - `out`: 响应全部通过核对后追加命令的缓冲区，失败时保留原有内容。
///
/// 返回：Nil 或空容器表示空轮；坐标、数量或条目结构异常返回 Parsing，不发布部分命令。
pub(super) fn collect_cmd_entries(
    v: redis::Value,
    stream: &str,
    out: &mut Vec<(String, String)>,
) -> Result<()> {
    use redis::Value as V;
    if matches!(v, V::Nil) {
        return Ok(());
    }
    let pairs = match v {
        V::Map(pairs) => pairs,
        value => {
            let mut pairs = Vec::new();
            for item in wire::array(value)? {
                let mut pair = wire::array(item)?;
                if pair.len() != 2 {
                    return Err(NasaRedisError::Parsing(
                        "invalid command stream pair".into(),
                    ));
                }
                let entries = pair.pop().expect("entries");
                pairs.push((pair.pop().expect("stream"), entries));
            }
            pairs
        }
    };
    // 只请求一个物理 Stream；额外或错误坐标不能借本分区的持有凭据执行处置。
    if pairs.len() > 1 {
        return Err(NasaRedisError::Parsing(
            "unexpected command stream count".into(),
        ));
    }
    for (key, entries) in pairs {
        if wire::bytes(key)? != stream.as_bytes() {
            return Err(NasaRedisError::Parsing(
                "unexpected command stream coordinate".into(),
            ));
        }
        collect_cmd_stream_entries(entries, out)?;
    }
    Ok(())
}

/// 业务作用：完整核对单个管理 Stream 的条目身份和唯一操作字段。
///
/// 参数说明：
/// - `entries_v`: 一条 Stream 的条目数组，字段可为交替键值数组或 RESP3 映射。
/// - `out`: 全部条目通过核对后才追加结果的缓冲区。
///
/// 返回：成功时追加合法 ID 与 UTF-8 操作身份，明确 Nil 墓碑不产生动作。
/// 任一形态异常、重复 ID、缺失或重复 op 均返回 Parsing，保留原缓冲区不变，不发布部分命令。
fn collect_cmd_stream_entries(
    entries_v: redis::Value,
    out: &mut Vec<(String, String)>,
) -> Result<()> {
    use redis::Value as V;
    let mut parsed = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for entry in wire::array(entries_v)? {
        let mut pair = wire::array(entry)?;
        if pair.len() != 2 {
            return Err(NasaRedisError::Parsing("invalid command entry".into()));
        }
        let fields = pair.pop().expect("fields");
        let id = wire::id(pair.pop().expect("id"))?;
        // 同一页的重复坐标不能授权多次动作；Nil 只表示已删除正文，不推导或重建操作身份。
        if !seen.insert(id.clone()) {
            return Err(NasaRedisError::Parsing("duplicate command entry".into()));
        }
        if matches!(fields, V::Nil) {
            continue;
        }
        let fields = match fields {
            V::Map(pairs) => pairs,
            value => {
                let fields = wire::array(value)?;
                if fields.len() % 2 != 0 {
                    return Err(NasaRedisError::Parsing("odd command fields".into()));
                }
                let mut pairs = Vec::new();
                let mut fields = fields.into_iter();
                while let (Some(key), Some(value)) = (fields.next(), fields.next()) {
                    pairs.push((key, value));
                }
                pairs
            }
        };
        let mut operation = None;
        for (key, value) in fields {
            let key = wire::bytes(key)?;
            let value = wire::bytes(value)?;
            if key == b"op" {
                // 字节替换或重复字段会把一条请求映射到不同结果键，必须在产生候选动作前拒绝。
                if operation.is_some() {
                    return Err(NasaRedisError::Parsing(
                        "duplicate command operation".into(),
                    ));
                }
                operation =
                    Some(String::from_utf8(value).map_err(|_| {
                        NasaRedisError::Parsing("non UTF-8 command operation".into())
                    })?);
            }
        }
        let operation =
            operation.ok_or_else(|| NasaRedisError::Parsing("missing command operation".into()))?;
        parsed.push((id, operation));
    }
    // 后续条目异常时不能留下已经可执行的前缀；整个集合通过后再交给调用方。
    out.extend(parsed);
    Ok(())
}

/// 业务作用：以稳定操作身份执行持有分区的管理命令，动作成功后通知协调器重新核对隔离状态。
///
/// 参数说明：
/// - `rt`: 提供命令结果、来源持有权和协调器事件通道的消费组运行时。
/// - `p`: 命令所属的物理分区编号。
/// - `op_id`: 命令条目引用的稳定操作身份，用于结果去重及处置续作。
///
/// 返回：持久终态或已无结果记录的孤儿命令返回 true，允许确认和删除该命令条目。
/// 持有权不足、查询失败或执行仍需重试返回 false，保留 PEL；成功通知不直接开放业务来源。
pub(super) async fn run_command(rt: &Arc<GroupRuntime>, p: u32, op_id: &str) -> bool {
    use super::command::{execute_one, query, CmdAck, CmdAction};
    // 预读只选择动作，执行入口仍须复验持久动作、状态与期限，不能以此快照覆盖新结果。
    let pre = match query(&rt.client, &rt.layout, p, op_id).await {
        Ok(Some(pre)) => pre,
        // 缺少结果记录时已无可执行动作；允许回收孤儿引用，不凭命令条目重建处置参数。
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
    // 持有快照不等于当前权威；执行前取得本节点凭据，实际处置写入再原子复验持有者。
    // 同一 op_id 可继续自己的在途动作，失权时保留 PEL 交给后继 owner。
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
            // 仅成功终态发恢复提示；协调器还会复验持久隔离状态与本地修订，迟到提示不能直接解除冻结。
            if let Ok(Some(r)) = query(&rt.client, &rt.layout, p, op_id).await {
                if matches!(r.state, super::command::CmdState::Succeeded) {
                    let _ = rt
                        .event_tx
                        .send(Event::ParkResolved {
                            p,
                        })
                        .await;
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

/// 业务作用：周期请求空闲来源重查 PEL，实际读取仍由来源唯一监督者执行。
///
/// 参数说明：
/// - `rt`: 提供组前缀、最小空闲周期、来源账本和停止信号的运行时。
///
/// 返回：后台关闭时退出；按不小于一秒的周期标记恢复请求，不直接读取 PEL 或改变持有权。
pub(super) async fn sweep_loop(rt: Arc<GroupRuntime>) {
    let mut tick = tokio::time::interval(Duration::from_millis(rt.cfg.min_idle_ms.max(1000)));
    tick.tick().await;
    loop {
        tokio::select! {
            _ = rt.bg_cancel.cancelled() => return,
            _ = tick.tick() => {
            }
        }
        rt.core.request_recovery(&rt.layout.prefix);
    }
}

/// 业务作用：订阅组唤醒通道以加快再平衡，周期再平衡仍是信号丢失后的兜底路径。
///
/// 参数说明：
/// - `rt`: 提供 Redis Pub/Sub 连接、组唤醒键、再平衡互斥与后台停止信号的运行时。
///
/// 返回：观察到停止后退出；连接或订阅失败等待一秒再建连接，订阅断开后重新建立。
/// 收到消息只触发再平衡请求，不直接释放来源锁或发布业务路由。
pub(super) async fn wake_loop(rt: Arc<GroupRuntime>) {
    loop {
        if rt.bg_cancel.is_cancelled() {
            return;
        }
        // Pub/Sub 消息可能丢失；独立重建订阅只加快发现，不能替代周期心跳与来源锁核对。
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
                    if m.is_none() {
                        break;
                    }
                    let _ = rebalance_once(&rt).await;
                }
            }
        }
    }
}
