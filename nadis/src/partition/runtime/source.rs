//! 来源读取与恢复；不拥有业务 mailbox，也不执行 handler。

use super::engine::SourceAuthority;
use super::*;
use crate::lock::HoldStatus;
use std::time::Duration;

/// 业务作用：监督一个持锁期的读取与恢复，全部记录通过共享 dispatcher 接管。
/// 参数说明：`rt` 为物理组；`source` 为不可变任期权威。
/// 返回：Quiescing/Lost 后停止新 I/O；已发命令先返回，再取得本地退出证明。
pub(super) async fn source_loop(rt: Arc<GroupRuntime>, source: Arc<SourceAuthority>) {
    // 明确失权时丢弃旧任期 I/O Future，读取预留随 guard 归还；Quiescing 仍等待当前请求收口。
    tokio::select! {
        biased;
        _ = source.lost_cancel.cancelled() => {
        }
        _ = read_source(rt.clone(), source.clone()) => {
        }
    }
}

/// 业务作用：在单一来源读者中按恢复屏障串行推进接管、精确 PEL 对账和新记录读取。
/// 参数说明：`rt` 为物理组；`source` 为本次持锁期权威。
/// 返回：正常让出时完成当前 I/O 后退出，明确失权由外层监督立即取消。
async fn read_source(rt: Arc<GroupRuntime>, source: Arc<SourceAuthority>) {
    let mut identity = BatchIdentity {
        claim_epoch: source.epoch,
        batch_seq: 0,
    };
    let mut recover = true;
    let mut cursor = "0-0".to_string();
    let mut autoclaim_done = false;
    let mut resume_claim = false;
    let mut claim_idle = 0u64;
    while source.active() {
        if source.parked.load(Ordering::Acquire) || source.blocked.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        if source.uncertain.swap(false, Ordering::AcqRel) {
            recover = true;
            autoclaim_done = true;
        }
        if source.sweep_requested.swap(false, Ordering::AcqRel) {
            recover = true;
            autoclaim_done = false;
            cursor = "0-0".to_string();
            claim_idle = rt.cfg.min_idle_ms;
        }
        // 未知读取先等旧本地 owner 收口，再从当前 consumer 的无 idle 过滤 PEL 重建责任。
        if recover && !rt.core.drained(&source) {
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }
        let Some(permit) = rt.core.reserve(&source, rt.cfg_batch()) else {
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        };
        if rt.lock.holds_status(&source.lock_key, &source.holder).await != HoldStatus::Held
            || !source.can_start()
        {
            drop(permit);
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        let Some(batch) = identity.advance() else {
            source.blocked.store(true, Ordering::Release);
            break;
        };
        let scanning_own = recover && autoclaim_done;
        let result = if recover {
            if !autoclaim_done {
                claim_page(&rt, &source, &cursor, claim_idle)
                    .await
                    .map(|(next, rows)| {
                        if next == "0-0" {
                            autoclaim_done = true;
                        } else if next == cursor {
                            source.blocked.store(true, Ordering::Release);
                        }
                        cursor = next;
                        rows
                    })
            } else {
                own_pending_page(&rt, &source).await
            }
        } else {
            let response = redis::cmd("XREADGROUP")
                .arg("GROUP")
                .arg(rt.layout.group())
                .arg(&rt.node_id)
                .arg("COUNT")
                .arg(rt.cfg_batch())
                .arg("STREAMS")
                .arg(rt.layout.stream(source.partition))
                .arg(">")
                .query_async::<redis::Value>(&mut rt.client.conn())
                .await;
            response
                .map_err(NasaRedisError::from)
                .and_then(|v| wire::read(v, &rt.layout.stream(source.partition)))
        };
        // 读取可能已改变 PEL，结果只有在同一 holder 仍有效时才能进入业务调度。
        let holds = rt.lock.holds_status(&source.lock_key, &source.holder).await;
        if holds == HoldStatus::Lost {
            source.lose();
            drop(permit);
            break;
        }
        if holds != HoldStatus::Held || !source.can_start() {
            recover = true;
            autoclaim_done = true;
            drop(permit);
            continue;
        }
        match result {
            Ok(rows) => {
                let empty = rows.is_empty();
                rt.core.dispatch(permit, batch.batch_seq, rows, &rt.plans);
                if empty {
                    // 只有完整当前 consumer XPENDING 空页加 holder 复验才解除恢复屏障。
                    if scanning_own {
                        if resume_claim {
                            autoclaim_done = false;
                            resume_claim = false;
                        } else {
                            recover = false;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(rt.stream_cfg.poll_timeout_ms.max(1)))
                        .await;
                }
            }
            Err(error) => {
                drop(permit);
                if matches!(&error, NasaRedisError::Redis(error) if error.code() == Some("NOGROUP"))
                {
                    let _ = rt.ensure_stream_contract().await;
                }
                if matches!(error, NasaRedisError::Parsing(_)) {
                    // 协议异常没有空扫描证明，来源保持保护态供观测和显式处置。
                    source.blocked.store(true, Ordering::Release);
                    source.protocol_error.store(true, Ordering::Release);
                }
                if recover && !autoclaim_done {
                    resume_claim = true;
                }
                recover = true;
                autoclaim_done = true;
                tracing::warn!(partition = source.partition, error = %error, "来源读取结果不确定，等待 PEL 对账");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

/// 业务作用：新任期按有界页面接管历史 PEL，区分 deleted ID 与空正文。
/// 参数说明：`rt` 为组；`source` 为当前任期；`cursor` 为服务端游标；`min_idle` 为巡检接管的最小空闲时间。
/// 返回：下一游标与本页记录；异常形态不返回空页。
async fn claim_page(
    rt: &GroupRuntime,
    source: &SourceAuthority,
    cursor: &str,
    min_idle: u64,
) -> Result<(String, Vec<(String, Option<Vec<u8>>)>)> {
    let value = redis::cmd("XAUTOCLAIM")
        .arg(rt.layout.stream(source.partition))
        .arg(rt.layout.group())
        .arg(&rt.node_id)
        .arg(min_idle)
        .arg(cursor)
        .arg("COUNT")
        .arg(rt.cfg_batch())
        .query_async(&mut rt.client.conn())
        .await?;
    let values = wire::array(value)?;
    if !(2..=3).contains(&values.len()) {
        return Err(NasaRedisError::Parsing(
            "invalid XAUTOCLAIM response".into(),
        ));
    }
    let mut it = values.into_iter();
    let cursor = wire::id(it.next().expect("cursor"))?;
    let mut rows = wire::entries(it.next().expect("entries"))?;
    if let Some(deleted) = it.next() {
        for value in wire::array(deleted)? {
            rows.push((wire::id(value)?, None));
        }
    }
    Ok((cursor, rows))
}

/// 业务作用：无 minIdle 扫描当前 consumer PEL，覆盖命令执行后响应丢失的记录。
/// 参数说明：`rt` 为组；`source` 为当前持锁期。
/// 返回：有界精确正文；只有明确 XPENDING 空页返回空集合。
async fn own_pending_page(
    rt: &GroupRuntime,
    source: &SourceAuthority,
) -> Result<Vec<(String, Option<Vec<u8>>)>> {
    let value = redis::cmd("XPENDING")
        .arg(rt.layout.stream(source.partition))
        .arg(rt.layout.group())
        .arg("-")
        .arg("+")
        .arg(rt.cfg_batch())
        .arg(&rt.node_id)
        .query_async(&mut rt.client.conn())
        .await?;
    let pending = wire::pending(value)?;
    let explicitly_empty = pending.is_empty();
    let mut rows = Vec::with_capacity(pending.len());
    for entry in pending {
        if entry.consumer != rt.node_id {
            return Err(NasaRedisError::Parsing("own PEL consumer mismatch".into()));
        }
        let body = wire::exact_body(rt, source.partition, &entry.id).await?;
        // 正文读取后复验 PEL 归属，被其它 consumer 取走的记录不归入当前任期。
        if wire::exact_pending(rt, source.partition, &entry.id)
            .await?
            .is_some_and(|row| row.consumer == rt.node_id)
        {
            rows.push((entry.id, body));
        }
    }
    if !explicitly_empty && rows.is_empty() {
        return Err(NasaRedisError::ExecutionUnknown(
            "PEL ownership changed while reading page".into(),
        ));
    }
    Ok(rows)
}
