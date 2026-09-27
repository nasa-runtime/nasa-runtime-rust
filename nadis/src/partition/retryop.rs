// ============================================================================
// src/partition/retryop.rs —— ExactTargetRetry 的 retry-op marker 协议。
// 业务约束由持久化意图和 CAS 五规则共同保证。
//
// 解决的问题
//   · `XCLAIM RETRYCOUNT n` 是【直接置值】可把 delivery count 调低——"盲目重放"会回退
//     已被合法重投的计数、把消息从新 owner 抢回;
//   · same-consumer XCLAIM 的 Unknown 无法靠 owner 变化判断"执行过没有"(owner 前后相同)
//     → 必须先把意图(desired count)写进 Redis 端 marker,重放才有事实依据。
//
// 协议(**按 operation_id 键控**,消除 per-partition 单 marker 串扰)
//   marker = {prefix}:retryop:{p}:{op_id}(HASH + TTL 600s)
//   op_id  = fnv1a64(sorted(ids) | group | consumer 身份与来源任期 | ticket revision)
//            —— 同一票据网络重放稳定；新的业务失败创建新的操作身份
//   字段:state=Pending;ids=JSON [{"id":"1-0","desired":2},...]
//
//   首次:逐 ID XPENDING 查 count → desired = count+1 → 整体写 Pending + TTL
//        → 本地票据冻结意图 → 逐 ID 执行 CAS Lua；终态可删除 marker，残留由 TTL 清理;
//   重放(上次 Unknown 中断,同 op_id 的 marker 仍 Pending):读回 ids+desired → 逐 ID 同一
//        CAS Lua ——desired 来自 marker,不重算,结果幂等(防 crash/响应丢失二次递增);
//   不同 operation 按 op_id 使用独立 marker，同一分区的并行操作不覆盖彼此的意图。
//
// 单 ID CAS 判定表：
//   owner ≠ expected            → 'OWNER'     (OwnershipChanged,绝不 XCLAIM)
//   count == desired - 1        → XCLAIM RETRYCOUNT desired,返回 payload('CLAIMED')
//   count == desired            → 仅 XRANGE 取 payload,**不再 XCLAIM**('HAVE')
//   count >  desired            → 'SUPERSEDED'(绝不把 count 调低)
//   count <  desired - 1        → 'CORRUPT'   (协议损坏,告警停止自动修改)
//   不在 PEL                    → 'RESOLVED'  (AlreadyResolved/EntryDeleted)
// ============================================================================

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::KeyLayout;
use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};

/// 业务作用：retry-op marker key(**按 operation_id 键控**):`{prefix}:retryop:{p}:{op_id}`。
/// op_id = 确定性摘要(同 operation 重试稳定、不同 operation 不同)→ 从根上消除"per-partition
/// 单 marker 被不同 operation 覆盖/串扰"。
///
/// # 参数
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `op_id`: retry operation 的稳定 ID。
pub fn marker_key(layout: &KeyLayout, p: u32, op_id: &str) -> String {
    format!("{}:retryop:{}:{}", layout.prefix, p, op_id)
}

/// 业务作用：计算 operation_id:对(排序后的 target ids + group + consumer + claim_term)做 FNV-1a64。
/// 参数说明：`layout` 提供组身份；`consumer` 包含节点身份、来源任期和操作身份；
/// `claim_term` 为同一票据稳定的 revision；`ids` 为本次精确重投坐标。
/// 返回：同一票据的网络重放得到相同摘要，不同业务失败使用不同操作身份。
pub fn operation_id(layout: &KeyLayout, consumer: &str, claim_term: u64, ids: &[String]) -> String {
    let mut sorted: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
    sorted.sort_unstable();
    let payload = format!(
        "{}|{}|{}|{}",
        sorted.join(","),
        layout.group(),
        consumer,
        claim_term
    );
    crate::keytag::fnv1a64_hex(payload.as_bytes())
}

/// retry-op marker 的 TTL(防 finish 删除失败 / crash 留下孤儿 marker 永久残留)。
const RETRYOP_MARKER_TTL: std::time::Duration = std::time::Duration::from_secs(600);

/// marker 中一条目标的意图记录。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryIntent {
    pub id: String,
    /// 目标 delivery count(= 写 Pending 时的 current + 1;重放恒用此值)。
    pub desired: u64,
}

/// 单 ID 的 CAS 归约结果。
#[derive(Debug)]
pub enum RetryOutcome {
    /// 本次执行了 XCLAIM RETRYCOUNT(count 从 desired-1 → desired),payload 在内。
    Claimed(Vec<u8>),
    /// 前次已执行(count == desired):仅取回 payload,未再递增。
    Have(Vec<u8>),
    /// 已不在 PEL，或脚本未取得 data 正文；调用方须精确读取以区分 tombstone 与缺字段记录。
    Resolved,
    /// owner 已变(被 reaper/新 owner 接管):放弃该 ID。
    OwnershipChanged,
    /// count > desired:已被更新的重投取代,本宗作废。
    Superseded,
    /// count < desired-1:协议损坏(理论不可达),停止自动修改。
    Corrupt,
}

/// retry-op CAS 的 holder 与可选 fencing 凭据，修改 PEL 前须证明来源仍有当前任期权限。
/// PEL consumer 名不能单独证明当前来源仍有权修改 delivery count。
/// `fence_key` 为空表示 LegacyV1 的 holder-only 校验。
pub struct FenceArgs<'a> {
    pub holder: &'a str,
    pub lock_key: &'a str,
    pub fence_key: &'a str,
    pub round: u64,
    pub nonce: &'a str,
    pub counter: u64,
}

// CAS 本身不执行 XACK；RESOLVED 仍由调用方精确检查正文，确认 tombstone 后才交给提交监督清理 PEL。
const RETRY_CAS_LUA: &str = r#"
-- owner/fence 前缀：KEYS[2]=lock KEYS[3]=fence；ARGV[5]=holder ARGV[6]=has_fence
--   ARGV[7]=round ARGV[8]=nonce ARGV[9]=p ARGV[10]=counter。任一不符 → 'OWNER'(交新 owner)。
if redis.call('hexists', KEYS[2], ARGV[5]) == 0 then return {'OWNER'} end
if ARGV[6] == '1' then
    if redis.call('HGET', KEYS[3], 'state') ~= 'Ready' then return {'OWNER'} end
    if redis.call('HGET', KEYS[3], 'round') ~= ARGV[7] then return {'OWNER'} end
    if redis.call('HGET', KEYS[3], 'nonce') ~= ARGV[8] then return {'OWNER'} end
    if redis.call('HGET', KEYS[3], 'c:' .. ARGV[9]) ~= ARGV[10] then return {'OWNER'} end
end
local pend = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
if #pend == 0 then
    return {'RESOLVED'}
end
-- XPENDING 详式返回 {id, consumer, idle, delivery-count}
local owner = pend[1][2]
local count = tonumber(pend[1][4])
local desired = tonumber(ARGV[4])
if owner ~= ARGV[2] then
    return {'OWNER'}
end
if count > desired then
    return {'SUPERSEDED'}
end
if count < desired - 1 then
    return {'CORRUPT'}
end
-- 取 payload 的辅助:XRANGE 精确单条,提取 data field
local function payload_of()
    local rows = redis.call('XRANGE', KEYS[1], ARGV[3], ARGV[3])
    if #rows == 0 then
        return nil  -- entry 已删(tombstone):PEL 在、payload 无
    end
    local fields = rows[1][2]
    for i = 1, #fields, 2 do
        if fields[i] == 'data' then
            return fields[i + 1]
        end
    end
    return nil
end
if count == desired then
    local pl = payload_of()
    if pl == nil then return {'RESOLVED'} end
    return {'HAVE', pl}
end
-- count == desired - 1:执行 claim(置 count = desired,幂等重放的唯一递增点)
local claimed = redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], 0, ARGV[3], 'RETRYCOUNT', desired)
if #claimed == 0 then
    return {'RESOLVED'}
end
local pl = payload_of()
if pl == nil then return {'RESOLVED'} end
return {'CLAIMED', pl}
"#;

/// 业务作用：确保 Pending 意图已持久化(**按 op_id 键控**):marker 不存在 → 按当前 PEL 算
/// desired 写新 Pending + TTL;已 Pending(同 op_id = 同 operation,op_id 由 ids 派生)→
/// 读回沿用已落盘 desired(重放幂等,不重算)。
/// 不同 operation 的 marker 独立；同一 op_id 的 marker ids 与请求不一致时拒绝执行，
/// 防止操作身份碰撞让调用方继承其它记录的重投意图。
///
/// # 参数
/// - `client`: 读写 retry marker 与 PEL 状态的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `op_id`: retry operation 的稳定 ID。
/// - `ids`: 本次要纳入 retry operation 的 stream entry ID 列表。
pub async fn ensure_pending(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    op_id: &str,
    ids: &[String],
) -> Result<Vec<RetryIntent>> {
    // 重复 ID 破坏一次 operation 对应一组唯一坐标的合同，必须在登记意图前拒绝。
    let uniq: std::collections::BTreeSet<&str> = ids.iter().map(|s| s.as_str()).collect();
    if uniq.len() != ids.len() {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "retry-op 请求含重复 ID(分区 {p}):{ids:?},fail-closed"
        )));
    }
    let mkey = marker_key(layout, p, op_id);
    let state: Option<String> = client.h_get(&mkey, "state").await?;

    if state.as_deref() == Some("Pending") {
        let json: Option<String> = client.h_get(&mkey, "ids").await?;
        let old: Vec<RetryIntent> = serde_json::from_str(&json.unwrap_or_default())
            .map_err(|e| NasaRedisError::Codec(format!("retryop ids 解析失败: {e}")))?;
        // 同 op_id = 同 operation(op_id 由 sorted(ids)+group+consumer+term 派生)→ 合法重放。
        // 防 op_id 碰撞:marker ids 集合必须与本次请求一致,否则 fail-closed(绝不据他宗 desired)。
        let marker_set: std::collections::BTreeSet<&str> =
            old.iter().map(|i| i.id.as_str()).collect();
        let req_set: std::collections::BTreeSet<&str> = ids.iter().map(|s| s.as_str()).collect();
        if marker_set == req_set {
            return Ok(old); // 重放:沿用已落盘 desired(幂等,防 crash/响应丢失二次递增)
        }
        return Err(NasaRedisError::ProtocolMarker(format!(
            "retry-op op_id 碰撞(marker ids 与请求不一致),fail-closed: op_id={op_id}"
        )));
    }

    // 首次:逐 ID 查 count → desired = count + 1。复用 `actual_count`
    //(空=不在 PEL 跳过;XPENDING 形态异常 = fail-closed 上抛,绝不静默丢 intent)。
    let mut intents = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(count) = actual_count(client, layout, p, id).await? {
            intents.push(RetryIntent {
                id: id.clone(),
                desired: count + 1,
            });
        }
    }
    if intents.is_empty() {
        return Ok(intents); // 全部已出 PEL:无宗可立
    }
    // 意图、状态与 TTL 在同一 Lua 写入，避免崩溃留下缺少状态或无法到期的 marker。
    let json = serde_json::to_string(&intents).map_err(|e| NasaRedisError::Codec(e.to_string()))?;
    let _: redis::Value = redis::Script::new(ENSURE_PENDING_LUA)
        .key(&mkey)
        .arg(json.as_str())
        .arg(RETRYOP_MARKER_TTL.as_millis() as u64)
        .invoke_async(&mut client.conn())
        .await?;
    Ok(intents)
}

/// 原子写 retry-op Pending marker + TTL(防半 marker / 永久 marker)。
const ENSURE_PENDING_LUA: &str = r#"
redis.call('HSET', KEYS[1], 'ids', ARGV[1], 'state', 'Pending')
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
"#;

/// 业务作用：逐 ID 执行 CAS 归约(可重放:Pending 期间任意次调用结果一致)。**owner-fenced**
/// ——CAS 改 PEL 前在同一 Lua 校验持锁 + fence 任期,失锁/旧任期 → `OwnershipChanged`(交新 owner)。
///
/// # 参数
/// - `client`: 执行 retry CAS Lua 的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `consumer`: 当前 owner consumer 名。
/// - `intents`: 已持久化的目标 entry 与 desired delivery count。
/// - `fence`: 当前 owner 的持锁与任期校验参数。
pub async fn execute(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    consumer: &str,
    intents: &[RetryIntent],
    fence: &FenceArgs<'_>,
) -> Result<Vec<(String, RetryOutcome)>> {
    let stream = layout.stream(p);
    let lua = redis::Script::new(RETRY_CAS_LUA);
    let has_fence = if fence.fence_key.is_empty() {
        "0"
    } else {
        "1"
    };
    let mut out = Vec::with_capacity(intents.len());
    for it in intents {
        let v: redis::Value = lua
            .key(&stream)
            .key(fence.lock_key)
            // LegacyV1 不读取 fencing key，但 Lua 声明的全部 KEYS 仍必须与 Stream 同槽。
            .key(if fence.fence_key.is_empty() {
                stream.as_str()
            } else {
                fence.fence_key
            })
            .arg(layout.group())
            .arg(consumer)
            .arg(&it.id)
            .arg(it.desired)
            .arg(fence.holder)
            .arg(has_fence)
            .arg(fence.round.to_string())
            .arg(fence.nonce)
            .arg(p.to_string())
            .arg(fence.counter.to_string())
            .invoke_async(&mut client.conn())
            .await?;
        let outcome = parse_outcome(v);
        if matches!(outcome, RetryOutcome::Corrupt) {
            tracing::error!(p, id = %it.id, "retry-op 协议损坏(count < desired-1),停止自动修改");
        }
        out.push((it.id.clone(), outcome));
    }
    Ok(out)
}

/// 业务作用：读单 ID 当前在本组 PEL 的真实 delivery count(poison 判断必须以**实际
/// delivery count** 为准,不能直接信 marker desired——损坏 desired 会绕过 CAS 直接 Drop)。
/// 返回 None = 已不在 PEL(Resolved)。
///
/// # 参数
/// - `client`: 执行 XPENDING 查询的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `id`: 要读取 delivery count 的 stream entry ID。
pub async fn actual_count(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    id: &str,
) -> Result<Option<u64>> {
    let v: redis::Value = redis::cmd("XPENDING")
        .arg(layout.stream(p))
        .arg(layout.group())
        .arg(id)
        .arg(id)
        .arg(1)
        .query_async(&mut client.conn())
        .await?;
    // 详式:[[id, consumer, idle, count]];**空数组**=确证不在 PEL(Resolved)。
    //:其余形态异常(有行但缺 count / 非 Array)= 协议损坏,**fail-closed**——绝不当成
    // "不在 PEL"(那会让仍在 PEL 的消息被当 Resolved 跳过)。
    let redis::Value::Array(rows) = v else {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "XPENDING 详式响应非数组(分区 {p} id={id}): {v:?}"
        )));
    };
    let Some(first) = rows.into_iter().next() else {
        return Ok(None); // 空 = 确证不在 PEL
    };
    let redis::Value::Array(row) = first else {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "XPENDING 行非数组(分区 {p} id={id})"
        )));
    };
    match row.get(3) {
        //`try_from` 而非 `as u64`(fail-closed 路径唯一裸强转)——delivery count
        // 理论非负,但负值强转会 wrapping 成巨大 u64 污染毒判;负值即协议异常,上抛 ProtocolMarker。
        Some(redis::Value::Int(count)) => u64::try_from(*count).map(Some).map_err(|_| {
            NasaRedisError::ProtocolMarker(format!(
                "XPENDING delivery count 为负(分区 {p} id={id}): {count}"
            ))
        }),
        other => Err(NasaRedisError::ProtocolMarker(format!(
            "XPENDING 行缺 count 字段(分区 {p} id={id}): {other:?}"
        ))),
    }
}

/// 业务作用：收宗:本 operation 全部 ID 已归约 → **删除该 op_id 的 marker**(:op_id 键控后每
/// operation 独立 marker,直接删除即可,无需 Done 态;删除失败由 TTL 兜底清孤儿)。
///
/// # 参数
/// - `client`: 删除 retry marker 的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `op_id`: 已完成的 retry operation ID。
pub async fn finish(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    op_id: &str,
) -> Result<()> {
    client.del(&[&marker_key(layout, p, op_id)]).await?;
    Ok(())
}

/// 业务作用：解析 Lua 返回:["TAG"(, payload)]。
///
/// # 参数
/// - `v`: 待转换的值。
fn parse_outcome(v: redis::Value) -> RetryOutcome {
    let redis::Value::Array(mut arr) = v else {
        return RetryOutcome::Corrupt;
    };
    let tag = match arr.first() {
        Some(redis::Value::BulkString(b)) => String::from_utf8_lossy(b).into_owned(),
        Some(redis::Value::SimpleString(s)) => s.clone(),
        _ => return RetryOutcome::Corrupt,
    };
    match tag.as_str() {
        "CLAIMED" | "HAVE" => {
            //脚本对 tombstone **明确**返回 RESOLVED;CLAIMED/HAVE 却缺
            // payload = 协议损坏,**当 Corrupt**(冻结告警),绝不当 tombstone 跳过(那会丢消息)。
            let payload = match arr.pop() {
                Some(redis::Value::BulkString(b)) => b,
                _ => return RetryOutcome::Corrupt,
            };
            if tag == "CLAIMED" {
                RetryOutcome::Claimed(payload)
            } else {
                RetryOutcome::Have(payload)
            }
        }
        "RESOLVED" => RetryOutcome::Resolved,
        "OWNER" => RetryOutcome::OwnershipChanged,
        "SUPERSEDED" => RetryOutcome::Superseded,
        _ => RetryOutcome::Corrupt,
    }
}
