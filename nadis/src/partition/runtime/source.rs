//! 来源读取与恢复；不拥有业务 mailbox，也不执行 handler。

use super::engine::SourceAuthority;
use super::*;
use crate::lock::HoldStatus;
use std::time::Duration;

/// 业务作用：监督一个持锁期的读取与恢复，全部记录通过共享 dispatcher 接管。
///
/// 参数说明：
/// - `rt`: 持有物理 Stream 布局、读取配置和共享账本的消费组。
/// - `source`: 本次持锁期冻结的权威及失权信号，不能用后来的 Claim 替换。
///
/// 返回：正常排干等待当前读取分支完成后退出；显式失权立即取消本地读取 Future 并归还预留。
/// 已发送命令仍可能在服务端生效，退出不代表远端撤销，后继必须经 PEL 恢复。
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
///
/// 参数说明：
/// - `rt`: 提供消费组、唯一 consumer、读取预算、路由及 Redis 连接的运行时。
/// - `source`: 本次持锁期权威，读取前后复验 holder，并在登记前检查本地准入。
///
/// 返回：正常让出时当前网络步骤收口后退出；明确失权关闭来源，外层还可取消整个读取分支。
/// 不确定结果进入 PEL 对账，解析异常保持来源保护；只有当前 consumer 的明确空页才能解除恢复屏障。
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
        // Park 或未解决路由仍拥有顺序责任，冻结期间不领取新消息，也不绕过人工处置重新接管。
        if source.parked.load(Ordering::Acquire) || source.blocked.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        if source.uncertain.swap(false, Ordering::AcqRel) {
            // 上次请求可能已改变本 consumer 的 PEL，先无 idle 门槛重建自己的责任。
            recover = true;
            autoclaim_done = true;
        }
        if source.sweep_requested.swap(false, Ordering::AcqRel) {
            // 周期巡检重新扫描历史 PEL，采用配置的 idle 门槛，避免立即夺取其它 consumer 的活跃记录。
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
        // 预算不赋予远端持有权；未知或过期的 holder 不能发起可能改变 PEL 的命令。
        if rt.lock.holds_status(&source.lock_key, &source.holder).await != HoldStatus::Held
            || !source.can_start()
        {
            drop(permit);
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        let Some(batch) = identity.advance() else {
            // 批次身份不能回绕复用，耗尽后保持冻结，不把新读取混入旧任务责任。
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
                            // 服务端游标未推进时不能持续重复接管同页，保留保护态等待明确处置。
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
            // 明确失权后当前响应不进入业务账本；服务端事实由下一持有者接管。
            source.lose();
            drop(permit);
            break;
        }
        if holds != HoldStatus::Held || !source.can_start() {
            // 未知结果不能当空页或业务许可，下一轮先恢复当前 consumer 的精确 PEL。
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
///
/// 参数说明：
/// - `rt`: 提供物理 Stream、消费组、consumer 和批量上限的运行时。
/// - `source`: 当前持锁期，确定待接管的物理分区；调用方负责读前与读后的 holder 复验。
/// - `cursor`: 初始零游标或上一页已解析的服务端游标。
/// - `min_idle`: 允许接管的最小空闲毫秒数，首次恢复可为零，周期巡检采用配置门槛。
///
/// 返回：成功时返回下一游标和本页记录，服务端明确列出的 deleted ID 转为无正文墓碑。
/// 命令失败、响应数量或条目形态异常返回错误，不把异常当作接管完成。
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
///
/// 参数说明：
/// - `rt`: 提供唯一 consumer、物理布局与批量上限的消费组运行时。
/// - `source`: 当前持锁期，确定物理分区；整个页面完成后仍由调用方复验 holder。
///
/// 返回：返回正文读取后仍归本 consumer 的有界记录集合；正文确实不存在时保留墓碑身份。
/// 只有初始 XPENDING 明确为空才返回空集合；初始 owner 不符、全部记录被移交、协议或网络异常均返回错误。
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
        // 服务端筛选不替代协议核对，不能把另一 consumer 的待确认记录作为本次恢复输入。
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
        // 读取途中归属变化只说明本页证据过期，不能伪造可开放新消息的 PEL 空页证明。
        return Err(NasaRedisError::ExecutionUnknown(
            "PEL ownership changed while reading page".into(),
        ));
    }
    Ok(rows)
}
