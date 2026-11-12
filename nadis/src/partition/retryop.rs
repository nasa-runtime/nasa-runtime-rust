//! ExactTargetRetry 使用按操作身份分离的限时 HASH 保存重投目标次数。
//!
//! XCLAIM RETRYCOUNT 会直接覆盖投递次数，同 consumer 的回包丢失也不能仅凭 owner 判断是否执行。
//! 因此先通过 XPENDING 读取次数并登记 Pending 意图，再由本地票据冻结同一意图执行或重放。
//! 标记键包含物理组、分区及操作摘要，state 保存 Pending，ids 保存记录坐标与 desired 的 JSON；
//! 新标记同时保存完整 request_ids，已有标记不被并发登记覆盖，未包含该字段的旧标记须完整匹配请求。
//! 新建标记的 TTL 为 600 秒，操作结束后可删除，调用方持有的冻结意图不依赖标记继续存活。
//!
//! 每条重投脚本先检查锁 holder、可选 fencing 身份和 PEL consumer，再比较实际次数与目标次数。
//! 目标次数限定为正整数且不超过 2^53−1，避免 Lua 的整数转浮点过程混淆相邻次数。
//! 已达到目标时只读取正文，已被后继重投超越时停止修改，尚未达到前驱次数时报告 Corrupt。
//! RESOLVED 表示未取得待处理正文，调用方仍需精确区分墓碑与缺少 data 的记录；本模块不执行 XACK。

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::KeyLayout;
use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};

/// 业务作用：按物理组、分区与操作身份定位重投意图，避免同分区所有操作共用一个标记。
///
/// 参数说明：
/// - `layout`: 已冻结的分区组键布局。
/// - `p`: 目标记录所在的物理分区。
/// - `op_id`: 调用方在同一次操作重放期间保持不变的身份。
///
/// 返回：该操作的 HASH 键名；不读取 Redis，也不证明标记已存在或意图与请求一致。
pub fn marker_key(layout: &KeyLayout, p: u32, op_id: &str) -> String {
    format!("{}:retryop:{}:{}", layout.prefix, p, op_id)
}

/// 业务作用：由排序后的记录坐标与来源身份生成稳定摘要，使同一重投票据的网络重放复用标记。
///
/// 参数说明：
/// - `layout`: 提供本次重投所属消费组的身份。
/// - `consumer`: 包含节点及来源任期的消费身份，由调用方在票据生命周期内保持一致。
/// - `claim_term`: 本次票据的稳定修订序号，新的业务失败应使用新的修订。
/// - `ids`: 本次精确重投的记录坐标，输入顺序不影响摘要。
///
/// 返回：组合输入的 FNV-1a64 十六进制摘要；摘要不构成无碰撞保证，持久意图仍需核对请求身份。
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

/// 业务作用：将完整 Stream 坐标解析为数值身份，避免精确重投查询接受缩写范围或符号。
///
/// 参数说明：
/// - `id`: 需要定位单条记录的毫秒与序号文本。
///
/// 返回：两个非空 ASCII 十进制分量均可表示为 u64 时返回坐标；前导零保留数值等价，非法输入返回 None。
fn parse_record_id(id: &str) -> Option<(u64, u64)> {
    let (millis, sequence) = id.split_once('-')?;
    // XPENDING 允许范围缩写，但重投意图必须指向完整的一条记录，不能把其它坐标当作查询结果。
    if millis.is_empty()
        || sequence.is_empty()
        || !millis.bytes().all(|byte| byte.is_ascii_digit())
        || !sequence.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    Some((millis.parse().ok()?, sequence.parse().ok()?))
}

/// retry-op marker 的 TTL(防 finish 删除失败 / crash 留下孤儿 marker 永久残留)。
const RETRYOP_MARKER_TTL: std::time::Duration = std::time::Duration::from_secs(600);

/// Lua 将 XPENDING 整数转为双精度数；目标次数不超过该值时，后继更大次数仍可安全判为已被替代。
const MAX_RETRY_COUNT: u64 = 9_007_199_254_740_991;

/// marker 中一条目标的意图记录。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryIntent {
    /// 本次意图对应的完整 Stream 记录坐标。
    pub id: String,
    /// 登记时实际投递次数加一，取值为一到 2^53−1；重放沿用此值而不重新计算。
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
    /// 锁持有者、组身份、任期或 PEL consumer 不符，当前来源不能再修改该记录。
    OwnershipChanged,
    /// count > desired:已被更新的重投取代,本宗作废。
    Superseded,
    /// 次数低于预期前驱，或状态回包无法完整解释；停止本条记录的自动修改。
    Corrupt,
}

/// retry-op CAS 的 holder 与可选 fencing 凭据，修改 PEL 前须证明来源仍有当前任期权限。
/// PEL consumer 名不能单独证明当前来源仍有权修改 delivery count。
/// `fence_key` 为空表示 LegacyV1 的 holder-only 校验。
pub struct FenceArgs<'a> {
    /// 当前来源在分区锁 HASH 中的持有者 token。
    pub holder: &'a str,
    /// 与目标物理分区对应的完整锁键。
    pub lock_key: &'a str,
    /// RustV2 的组 fence HASH 键；为空时仅核对 LegacyV1 holder。
    pub fence_key: &'a str,
    /// 当前组缓存的初始化轮次。
    pub round: u64,
    /// 当前组缓存的激活身份。
    pub nonce: &'a str,
    /// 当前任期凭据所属的组初始化 epoch。
    pub epoch: u64,
    /// 当前持锁者取得的分区任期计数。
    pub counter: u64,
}

// CAS 本身不执行 XACK；RESOLVED 仍由调用方精确检查正文，确认 tombstone 后才交给提交监督清理 PEL。
const RETRY_CAS_LUA: &str = r#"
-- owner/fence 前缀：KEYS[2]=lock KEYS[3]=fence；ARGV[5]=holder ARGV[6]=has_fence
-- ARGV[7]=round、ARGV[8]=nonce、ARGV[9]=p、ARGV[10]=counter、ARGV[11]=epoch。
-- 修改投递次数同样需要完整权威，不能只依赖仍相同的 PEL consumer。
if redis.call('hexists', KEYS[2], ARGV[5]) == 0 then
    return {'OWNER'}
end
if ARGV[6] == '1' then
    if redis.call('HGET', KEYS[3], 'state') ~= 'Ready' then
        return {'OWNER'}
    end
    if redis.call('HGET', KEYS[3], 'round') ~= ARGV[7] then
        return {'OWNER'}
    end
    if redis.call('HGET', KEYS[3], 'nonce') ~= ARGV[8] then
        return {'OWNER'}
    end
    if redis.call('HGET', KEYS[3], 'c:' .. ARGV[9]) ~= ARGV[10] then
        return {'OWNER'}
    end
    local epoch = redis.call('HGET', KEYS[3], 'epoch')
    if epoch == false or string.match(epoch, '^%d+$') == nil then
        return {'OWNER'}
    end
    local normalized_epoch = string.gsub(epoch, '^0+', '')
    if normalized_epoch == '' then
        normalized_epoch = '0'
    end
    if normalized_epoch ~= ARGV[11] then
        return {'OWNER'}
    end
end
-- XPENDING 的整数进入 Lua 后已经转为双精度数，超出精确范围的目标不能参与相等和前驱判定。
if string.match(ARGV[4], '^%d+$') == nil or string.sub(ARGV[4], 1, 1) == '0' or
    string.len(ARGV[4]) > 16 or (string.len(ARGV[4]) == 16 and ARGV[4] > '9007199254740991') then
    return {'CORRUPT'}
end
local pend = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
if #pend == 0 then
    return {'RESOLVED'}
end
-- XPENDING 详式返回 {id, consumer, idle, delivery-count}
local owner = pend[1][2]
local count = pend[1][4]
local desired = tonumber(ARGV[4])
if owner ~= ARGV[2] then
    return {'OWNER'}
end
if type(count) ~= 'number' or count < 0 or count % 1 ~= 0 then
    return {'CORRUPT'}
end
-- 目标严格低于双精度舍入边界，即使更大实际次数已被舍入，仍能可靠判为被后继替代。
if count > desired then
    return {'SUPERSEDED'}
end
if count < desired - 1 then
    return {'CORRUPT'}
end
-- 业务作用：读取同一原子步骤内的精确记录正文，避免用其它坐标代替重投目标。
-- 参数说明：无。
-- 返回：存在 data 字段时返回原始字节，否则返回 nil，由调用方继续区分墓碑与缺字段记录。
local function payload_of()
    local rows = redis.call('XRANGE', KEYS[1], ARGV[3], ARGV[3])
    if #rows == 0 then
        return nil
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
    if pl == nil then
        return {'RESOLVED'}
    end
    return {'HAVE', pl}
end
-- 仅前驱次数才能递增；发送原十进制参数，避免 Lua 数字转文本再次改变目标值。
local claimed = redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], 0, ARGV[3], 'RETRYCOUNT', ARGV[4])
if #claimed == 0 then
    return {'RESOLVED'}
end
local pl = payload_of()
if pl == nil then
    return {'RESOLVED'}
end
return {'CLAIMED', pl}
"#;

/// 业务作用：读取已有 Pending 重投意图，或从当前 PEL 计算目标投递次数并登记限时标记。
///
/// 参数说明：
/// - `client`: 读取 PEL 并保存操作意图的 Redis 客户端。
/// - `layout`: 本次记录所属物理组的键布局。
/// - `p`: 精确重投记录所在的物理分区。
/// - `op_id`: 同一操作重放期间不变的标记身份。
/// - `ids`: 本次操作的唯一记录坐标集合，已不在 PEL 的记录在新建意图时略过。
///
/// 返回：完整且有期限的 Pending 沿用原意图；不存在时从 PEL 计算后原子登记，竞争时返回胜出者的意图。
/// 全部记录已出 PEL 且没有并发登记时返回空列表；非法身份、不可精确比较的次数或网络错误返回失败。
/// 新目标次数上限为 2^53−1，此步骤本身不执行 XCLAIM，也不续延已有标记的 TTL。
pub async fn ensure_pending(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    op_id: &str,
    ids: &[String],
) -> Result<Vec<RetryIntent>> {
    let mut requested = std::collections::BTreeSet::new();
    // 同一坐标的不同文本表示也不能占两个意图，全部输入应在读取或登记前确认。
    for id in ids {
        let coordinate = parse_record_id(id).ok_or_else(|| {
            NasaRedisError::ProtocolMarker("retry-op 请求 ID 不完整或越界".into())
        })?;
        if !requested.insert(coordinate) {
            return Err(NasaRedisError::ProtocolMarker(
                "retry-op 请求包含重复坐标".into(),
            ));
        }
    }
    let identities: Vec<_> = requested
        .iter()
        .map(|(millis, sequence)| format!("{millis}-{sequence}"))
        .collect();
    let request_json = serde_json::to_string(&identities)
        .map_err(|_| NasaRedisError::Codec("retry-op 请求编码失败".into()))?;
    let mkey = marker_key(layout, p, op_id);
    let script = redis::Script::new(ENSURE_PENDING_LUA);
    // 状态、意图、完整请求及期限一次读取，避免拼接两个时刻的标记或覆盖持久损坏。
    let reply = script
        .key(&mkey)
        .arg("load")
        .arg("")
        .arg(&request_json)
        .arg(RETRYOP_MARKER_TTL.as_millis() as u64)
        .invoke_async(&mut client.conn())
        .await?;
    if let Some(intents) = parse_pending_reply(reply, &requested)? {
        return Ok(intents);
    }
    // 目标必须落在 Lua 可精确比较的范围，不能先写入一个后续无法判定是否已执行的意图。
    let mut intents = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(count) = actual_count(client, layout, p, id).await? {
            let desired = count
                .checked_add(1)
                .filter(|desired| *desired <= MAX_RETRY_COUNT)
                .ok_or_else(|| {
                    NasaRedisError::ProtocolMarker("retry-op 目标次数超过精确比较范围".into())
                })?;
            intents.push(RetryIntent {
                id: id.clone(),
                desired,
            });
        }
    }
    let json = serde_json::to_string(&intents)
        .map_err(|_| NasaRedisError::Codec("retry-op 意图编码失败".into()))?;
    // 只有仍不存在的标记才可创建；并发胜出的旧意图必须原样沿用，不能用较晚或较早快照覆盖。
    let reply = script
        .key(&mkey)
        .arg("create")
        .arg(&json)
        .arg(&request_json)
        .arg(RETRYOP_MARKER_TTL.as_millis() as u64)
        .invoke_async(&mut client.conn())
        .await?;
    parse_pending_reply(reply, &requested)?
        .ok_or_else(|| NasaRedisError::ProtocolMarker("retry-op 登记未返回持久意图".into()))
}

/// 原子读取或仅在键不存在时登记意图；已有 Pending 保留原值和 TTL，竞争者必须沿用胜出的意图。
const ENSURE_PENDING_LUA: &str = r#"
-- 残缺或无期限的已有标记不能覆盖，否则迟到登记可能改变已经执行过的目标次数。
if redis.call('EXISTS', KEYS[1]) ~= 0 then
    local state = redis.call('HGET', KEYS[1], 'state')
    local intents = redis.call('HGET', KEYS[1], 'ids')
    if state ~= 'Pending' or intents == false or redis.call('PTTL', KEYS[1]) <= 0 then
        return {'CORRUPT'}
    end
    return {'FOUND', intents, redis.call('HGET', KEYS[1], 'request_ids')}
end
if ARGV[1] == 'load' then
    return {'MISSING'}
end
if ARGV[1] ~= 'create' then
    return {'CORRUPT'}
end
-- 全部已出 PEL 时没有需要持久化的意图，但仍须先检查是否存在并发登记的原操作。
if ARGV[2] == '[]' then
    return {'CREATED', ARGV[2], ARGV[3]}
end
redis.call('HSET', KEYS[1], 'ids', ARGV[2], 'request_ids', ARGV[3], 'state', 'Pending')
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return {'CREATED', ARGV[2], ARGV[3]}
"#;

/// 业务作用：核对原子读取或登记返回的完整意图，避免继承其它请求、重复坐标或不可精确比较的次数。
///
/// 参数说明：
/// - `reply`: 标记脚本返回的状态、意图 JSON 与可选完整请求 JSON。
/// - `requested`: 调用方已校验且去除表示差异的完整请求坐标集合。
///
/// 返回：明确缺失时返回 None；请求身份及意图全部一致时返回持久意图，旧标记只允许完整集合相等。
/// 损坏或未知响应返回固定协议错误，不回显 JSON 正文；已存空意图不作为可重放标记接受。
fn parse_pending_reply(
    reply: redis::Value,
    requested: &std::collections::BTreeSet<(u64, u64)>,
) -> Result<Option<Vec<RetryIntent>>> {
    let invalid = || NasaRedisError::ProtocolMarker("retry-op 标记响应或持久意图非法".into());
    let redis::Value::Array(fields) = reply else {
        return Err(invalid());
    };
    if let [redis::Value::BulkString(tag)] = fields.as_slice() {
        return if tag == b"MISSING" {
            Ok(None)
        } else {
            Err(invalid())
        };
    }
    let [redis::Value::BulkString(tag), redis::Value::BulkString(json), request_json] =
        fields.as_slice()
    else {
        return Err(invalid());
    };
    if tag != b"FOUND" && tag != b"CREATED" {
        return Err(invalid());
    }
    let intents: Vec<RetryIntent> = serde_json::from_slice(json).map_err(|_| invalid())?;
    if tag == b"FOUND" && intents.is_empty() {
        return Err(invalid());
    }
    let mut stored = std::collections::BTreeSet::new();
    // 每个意图只能占据一个真实坐标，非法目标次数不能在后续 Lua 中变成错误的相等判断。
    for intent in &intents {
        let id = parse_record_id(&intent.id).ok_or_else(invalid)?;
        if !stored.insert(id) || intent.desired == 0 || intent.desired > MAX_RETRY_COUNT {
            return Err(invalid());
        }
    }
    match request_json {
        redis::Value::BulkString(json) => {
            let identities: Vec<String> = serde_json::from_slice(json).map_err(|_| invalid())?;
            let mut declared = std::collections::BTreeSet::new();
            for id in identities {
                if !declared.insert(parse_record_id(&id).ok_or_else(invalid)?) {
                    return Err(invalid());
                }
            }
            // 记录可能在登记前已出 PEL，因此意图可以是子集，但完整请求身份必须精确一致。
            if &declared != requested || !stored.is_subset(&declared) {
                return Err(invalid());
            }
        }
        // 没有完整请求字段的旧标记仅能证明其全部意图，不能猜测它还曾包含哪些已消失的记录。
        redis::Value::Nil if tag == b"FOUND" && &stored == requested => {}
        _ => return Err(invalid()),
    }
    Ok(Some(intents))
}

/// 业务作用：逐条按持久重投意图执行 CAS，在改变 PEL 前原子复验 holder 和可选任期身份。
///
/// 参数说明：
/// - `client`: 执行每条记录原子重投判定的 Redis 客户端。
/// - `layout`: 当前物理组的 Stream 与消费组布局。
/// - `p`: 本批意图所属的物理分区。
/// - `consumer`: 预期仍持有各记录 PEL 项的消费身份。
/// - `intents`: 已登记的记录坐标与目标投递次数，重放时不得重新计算目标值。
/// - `fence`: 当前来源的锁 holder 与可选 fencing 身份。
///
/// 返回：所有脚本调用完成时按输入顺序返回逐条结果；网络或脚本错误立即返回失败，
/// 此前已执行的记录不会回滚。Corrupt 只记录错误并保留该条结果，不在本函数中确认或删除正文。
/// 整批坐标与目标次数在首次请求前校验，目标上限为 2^53−1；空批次不读取或改变持久状态。
pub async fn execute(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    consumer: &str,
    intents: &[RetryIntent],
    fence: &FenceArgs<'_>,
) -> Result<Vec<(String, RetryOutcome)>> {
    let mut identities = std::collections::BTreeSet::new();
    // 任一后段参数非法时也不得先修改前段 PEL，重复数值坐标不能被不同文本包装成两条意图。
    for intent in intents {
        let id = parse_record_id(&intent.id).ok_or_else(|| {
            NasaRedisError::ProtocolMarker("retry-op 意图 ID 不完整或越界".into())
        })?;
        if !identities.insert(id) || intent.desired == 0 || intent.desired > MAX_RETRY_COUNT {
            return Err(NasaRedisError::ProtocolMarker(
                "retry-op 意图重复或目标次数不可精确比较".into(),
            ));
        }
    }
    if intents.is_empty() {
        return Ok(Vec::new());
    }
    // 凭据本身必须可表示为有效任期，服务端只负责复验这份完整身份是否仍然成立。
    if fence.holder.is_empty()
        || fence.lock_key.is_empty()
        || (!fence.fence_key.is_empty()
            && (fence.round == 0
                || fence.nonce.is_empty()
                || fence.counter == 0
                || fence.counter > i64::MAX as u64))
    {
        return Err(NasaRedisError::ProtocolMarker(
            "retry-op 来源持有权凭据不完整".into(),
        ));
    }
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
            .arg(fence.epoch)
            .invoke_async(&mut client.conn())
            .await?;
        let outcome = parse_outcome(v);
        if matches!(outcome, RetryOutcome::Corrupt) {
            tracing::error!(p, id = %it.id, "retry-op 响应或计数状态异常，停止本条自动修改");
        }
        out.push((it.id.clone(), outcome));
    }
    Ok(out)
}

/// 业务作用：查询单条记录在当前消费组 PEL 中的实际投递次数，为重投和毒消息判定提供依据。
///
/// 参数说明：
/// - `client`: 执行精确 XPENDING 查询的 Redis 客户端。
/// - `layout`: 提供目标 Stream 与消费组的物理布局。
/// - `p`: 记录所属的物理分区。
/// - `id`: 要读取实际投递次数的完整记录坐标。
///
/// 返回：合法查询的空数组表示记录已不在 PEL；唯一完整行的坐标匹配且时间与次数非负时返回次数。
/// 输入缩写、越界、字段损坏或响应坐标不符时失败，不把原始回包内容写入协议错误。
/// 此读取不改变 PEL，也不能替代实际重投时对 owner 与任期的原子检查。
pub async fn actual_count(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    id: &str,
) -> Result<Option<u64>> {
    // 发送前限定完整坐标，防止 Redis 将缩写当范围起点而返回其它记录的次数。
    let expected = parse_record_id(id)
        .ok_or_else(|| NasaRedisError::ProtocolMarker("retry-op 记录 ID 不完整或越界".into()))?;
    let v: redis::Value = redis::cmd("XPENDING")
        .arg(layout.stream(p))
        .arg(layout.group())
        .arg(id)
        .arg(id)
        .arg(1)
        .query_async(&mut client.conn())
        .await?;
    let invalid = || NasaRedisError::ProtocolMarker("retry-op XPENDING 响应形态或字段非法".into());
    // 只有完整空数组能证明不在 PEL；有行时必须同时确认唯一坐标及全部字段，不能只抽取次数。
    let redis::Value::Array(rows) = v else {
        return Err(invalid());
    };
    if rows.is_empty() {
        return Ok(None);
    }
    let [redis::Value::Array(row)] = rows.as_slice() else {
        return Err(invalid());
    };
    let [redis::Value::BulkString(returned), redis::Value::BulkString(consumer), redis::Value::Int(idle), redis::Value::Int(count)] =
        row.as_slice()
    else {
        return Err(invalid());
    };
    if std::str::from_utf8(returned).ok().and_then(parse_record_id) != Some(expected)
        || std::str::from_utf8(consumer).is_err()
        || *idle < 0
    {
        return Err(invalid());
    }
    u64::try_from(*count).map(Some).map_err(|_| invalid())
}

/// 业务作用：在调用方确认本操作全部记录已收口后删除其重投意图标记。
///
/// 参数说明：
/// - `client`: 执行标记删除的 Redis 客户端。
/// - `layout`: 该操作所属的物理组布局。
/// - `p`: 操作记录所在的物理分区。
/// - `op_id`: 已完成且不再重放的操作身份。
///
/// 返回：Redis 删除调用成功时完成，标记已不存在也接受；网络错误返回失败，残留依靠既有 TTL 回收。
/// 本函数不核对记录终态，也不执行 XACK 或删除 Stream 正文，调用方须保证删除时机。
pub async fn finish(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    op_id: &str,
) -> Result<()> {
    client.del(&[&marker_key(layout, p, op_id)]).await?;
    Ok(())
}

/// 业务作用：将逐条重投 Lua 的状态标签与可选正文转换为调用方可处理的归约结果。
///
/// 参数说明：
/// - `v`: 本条记录的原始 Redis 脚本响应。
///
/// 返回：完整且字段数精确的响应转换为已认领、已有正文、已消失、失权或被后继替代的状态；
/// 标签编码、字段数或正文类型异常时返回 Corrupt，正文允许为空或含二进制字节。
/// 本函数只解释响应，不确认消息，也不把未知响应当作已完成。
fn parse_outcome(v: redis::Value) -> RetryOutcome {
    let redis::Value::Array(mut arr) = v else {
        return RetryOutcome::Corrupt;
    };
    let tag = match arr.first() {
        Some(redis::Value::BulkString(bytes)) => std::str::from_utf8(bytes).ok(),
        Some(redis::Value::SimpleString(value)) => Some(value.as_str()),
        _ => None,
    };
    match tag {
        Some("CLAIMED" | "HAVE") if arr.len() == 2 => {
            let claimed = tag == Some("CLAIMED");
            // 正文只能来自第二个字段，缺项不能取标签充当正文，多余字段也不能改变消息内容。
            let payload = match arr.pop() {
                Some(redis::Value::BulkString(bytes)) => bytes,
                _ => return RetryOutcome::Corrupt,
            };
            if claimed {
                RetryOutcome::Claimed(payload)
            } else {
                RetryOutcome::Have(payload)
            }
        }
        Some("RESOLVED") if arr.len() == 1 => RetryOutcome::Resolved,
        Some("OWNER") if arr.len() == 1 => RetryOutcome::OwnershipChanged,
        Some("SUPERSEDED") if arr.len() == 1 => RetryOutcome::Superseded,
        _ => RetryOutcome::Corrupt,
    }
}
