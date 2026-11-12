// ============================================================================
// src/partition/disposition.rs —— 毒消息处置:Park / Resume / Drop。
// 实现 disposition 状态机及其恢复边界。
//
// ┌─ Key 布局（Cluster 下由 KeyLayout 将 prefix 包成 `{group}` hash-tag，本组所有 key） ┐
// │  含同一 tag 落同 slot;**非** keytag::derive_same_slot_key——后者为 per-key 细粒度同槽派生,  │
// │  当前 group 级 relayout 不走它,见 keytag.rs 注)                                            │
// │ disposition marker : {prefix}:disposition:{p}     (HASH,每分区同时最多一宗)  │
// │ quarantine         : {prefix}:quarantine:{park_id}(HASH,field=序号→payload,  │
// │                       "id:序号"→源 entry ID;确定性 key,重入幂等, 选型) │
// │ parked 索引        : {prefix}:parked:{p} = park_id (新 owner 接管时恢复停拉)   │
// └────────────────────────────────────────────────────────────────────────┘
//
// marker 的在途状态保存可重入意图，终态在清理后附带 TTL：
//   (无) → Parked → ResumePublishing → Resumed / ResumeIndeterminate
//                  ↘ DlqPublishing → Dlqed / DlqIndeterminate
//                  ↘ Dropping → Dropped
//   ResumeIndeterminate → ForcePublishing → Resumed
//
// fenced PARK_LUA:单脚本原子执行——
//   ①校验分区锁 holder(防旧 owner 失锁后迟到 park 冻结新 owner 的分区);
//   ②校验目标 ID 仍在本组 PEL(XPENDING 精确查;不在 = 状态已变,拒绝);
//   ③payload 转存 quarantine(确定性 HASH,源 stream 此后可随意 trim);
//   ④XACK 源消息;⑤写 marker(Parked)+ parked 索引。
//   Lua 运行时错误不回滚，持久副作用按恢复数据优先的顺序写入：
//   quarantine 先于 XACK(源被 ACK 前 payload 必有副本),重入按 park_id 幂等覆盖。
//
// resume = planned-ID reservations 协议：
//   ①读 last-generated-id,为批内每条预约【严格递增的显式 ID】(seq 溢出进位 ms+1,
//     StreamIdExhausted 不 wrapping—— 算法封口);
//   ②planned_ids[] **先整体持久化进 marker**,再逐条 `XADD 显式 ID`;
//   ③重入判定表(保守方案):entry 存在且 payload 与 quarantine 全文一致 → 已发布;
//     存在但不一致 → ProtocolConflict→Indeterminate;不存在且 last-gen < planned → 补发;
//     不存在且 last-gen ≥ planned → **PublishOutcomeIndeterminate(禁自动重预约)**
//     ——无法区分"从未执行被越过"与"已发布后被消费删除";
//   ④任一 Indeterminate → marker=ResumeIndeterminate{uncertain_indexes},分区保持 Parked;
//     仅 `force_republish`(显式接受重复风险)可为缺失 index 分配全新 planned 推进到
//     Resumed{forced=1, audit};
//   ⑤全部确认 → 写 new_ids 和 Resumed 终态 → 删 parked index → 最后删 quarantine。
//     payload 对账使用 quarantine 原文；终态提交前始终保留恢复数据。
// ============================================================================

use std::sync::Arc;

use super::KeyLayout;
use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};

/// 业务作用：定位分区唯一的处置记录，使当前 Park 及其恢复流程共享状态权威。
///
/// 参数说明：
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
///
/// 返回：该分区的 marker 键；只生成名称，不检查记录状态或持有权。
pub fn marker_key(layout: &KeyLayout, p: u32) -> String {
    format!("{}:disposition:{}", layout.prefix, p)
}

/// 业务作用：定位 Park 批次的恢复数据，重入时使用同一键读取原始载荷。
///
/// 参数说明：
/// - `layout`: 分区组 key 布局。
/// - `park_id`: 本次 Park 批次的唯一 ID。
///
/// 返回：批次隔离区键；调用方负责批次身份唯一性，此函数不创建或覆盖数据。
pub fn quarantine_key(layout: &KeyLayout, park_id: &str) -> String {
    format!("{}:quarantine:{}", layout.prefix, park_id)
}

/// 业务作用：定位分区的 Park 索引，供接管时与 marker 一起核对冻结状态。
///
/// 参数说明：
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
///
/// 返回：值为 park_id 的索引键；索引不能单独替代 marker 的权威状态。
pub fn parked_index_key(layout: &KeyLayout, p: u32) -> String {
    format!("{}:parked:{}", layout.prefix, p)
}

/// 管理处置携带的持有权证据；字段公开不代表授权，实际权威必须由 Redis 验证。
/// 空 fence_key 采用 LegacyV1 的 holder 校验，非空时还校验任期、随机身份和分区计数。
pub struct OwnerLease<'a> {
    /// 证据所属物理分区，应与本次处置坐标一致。
    pub partition: u32,
    /// 本次管理转换的稳定身份，重入时保持一致。
    pub operation_id: &'a str,
    /// 已由前序步骤确认的 Park 批次；续作必须保持一致，None 表示由入口捕获当前批次。
    pub expected_park_id: Option<&'a str>,
    /// 当前分布式锁 holder，不能仅凭本地保存值认定仍持锁。
    pub holder: &'a str,
    /// 服务端锁完整键，包含 DistributedLock 配置前缀。
    pub lock_key: &'a str,
    /// 严格 fencing 状态键；空值表示使用仅校验 holder 的协议。
    pub fence_key: &'a str,
    /// 当前已确认的 fencing 轮次。
    pub round: u64,
    /// 与轮次配对的随机身份，阻止相同数值轮次混用。
    pub nonce: &'a str,
    /// 本分区在该轮中的所有权计数，接管后旧值不再有效。
    pub counter: u64,
}

impl OwnerLease<'_> {
    /// 业务作用：为不启用 fencing 的协议提供同槽 Lua 占位 key，保持 Cluster 路由合法。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：启用 fencing 时返回 fence key；否则返回当前锁 key，脚本仍由 has_fence 禁止读取 fence 字段。
    fn lua_fence_key(&self) -> &str {
        if self.fence_key.is_empty() {
            // Cluster 在脚本执行前校验全部 KEYS，未启用的分支也不能传入跨槽的空字符串。
            self.lock_key
        } else {
            self.fence_key
        }
    }

    /// 业务作用：返回本次管理转换是否携带 V2 fence 的序列化标记。
    ///
    /// 参数说明：无。
    ///
    /// 返回：fence_key 非空时为 "1"，否则为 "0"；是否实际匹配仍由服务端脚本检查。
    fn has_fence(&self) -> &'static str {
        if self.fence_key.is_empty() {
            "0"
        } else {
            "1"
        }
    }
}

/// owner/fence 只读校验 Lua。KEYS=[lock_key, fence_key];ARGV=[holder, has_fence, round, nonce, p, counter]。
const VERIFY_OWNER_LUA: &str = r#"
if redis.call('hexists', KEYS[1], ARGV[1]) == 0 then return 'NOT_HOLDER' end
if ARGV[2] == '1' then
    if redis.call('HGET', KEYS[2], 'state') ~= 'Ready' then return 'FENCE_NOT_READY' end
    if redis.call('HGET', KEYS[2], 'round') ~= ARGV[3] then return 'ROUND_MISMATCH' end
    if redis.call('HGET', KEYS[2], 'nonce') ~= ARGV[4] then return 'NONCE_MISMATCH' end
    if redis.call('HGET', KEYS[2], 'c:' .. ARGV[5]) ~= ARGV[6] then return 'STALE_STAMP' end
end
return 'OK'
"#;

/// 业务作用：在进入管理状态分支前核对当前 holder 和已启用的 fencing，拒绝确定失权的请求。
///
/// 参数说明：
/// - `client`: 读取分区持有权的 Redis 客户端。
/// - `lease`: 调用方提供的锁、任期和分区计数证据。
///
/// 返回：脚本执行时所有证据匹配才成功；失败拒绝继续。成功仅为当次观测，不保证后续命令仍持有权威。
async fn verify_owner_fenced(client: &Arc<RedisClient>, lease: &OwnerLease<'_>) -> Result<()> {
    let tag: String = redis::Script::new(VERIFY_OWNER_LUA)
        .key(lease.lock_key)
        .key(lease.lua_fence_key())
        .arg(lease.holder)
        .arg(lease.has_fence())
        .arg(lease.round.to_string())
        .arg(lease.nonce)
        .arg(lease.partition.to_string())
        .arg(lease.counter.to_string())
        .invoke_async(&mut client.conn())
        .await?;
    if tag == "OK" {
        return Ok(());
    }
    Err(NasaRedisError::OwnershipChanged(format!(
        "分区 {}:owner-fence 校验失败({tag}),拒绝改 disposition(留 PEL 交新 owner)",
        lease.partition
    )))
}

/// 业务作用：在处置副作用发生的同一脚本中复验分区权威、批次及在途身份，阻止过期读取授权后续写入。
///
/// 参数说明：
/// - `client`: 执行原子处置脚本的 Redis 客户端。
/// - `layout`: 分区组的同槽键布局。
/// - `p`: 本次处置所属物理分区。
/// - `lease`: 当前持有权与可选的前序批次约束。
/// - `park_id`: 当前步骤已捕获的恢复批次。
/// - `state`: 允许执行该副作用的精确 marker 状态。
/// - `stream`: 当前操作涉及的源 Stream 或 DLQ 键。
/// - `body`: 仅由内部固定代码提供的操作脚本，使用第十个及后续参数，返回带 OK 标签的结果。
/// - `arguments`: 操作脚本的业务参数，不参与拼接 Lua 源码。
///
/// 返回：权威与记录均匹配时返回操作结果；失权、冲突或协议损坏均拒绝写入，通信失败仍可能已经执行。
#[allow(clippy::too_many_arguments)]
async fn guarded_disposition<A: redis::ToRedisArgs + Send>(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    lease: &OwnerLease<'_>,
    park_id: &str,
    state: &str,
    stream: &str,
    body: &str,
    arguments: A,
) -> Result<redis::Value> {
    // 内部参数也必须保持同一坐标与捕获批次，不能靠锁 key 的命名替代显式约束。
    if lease.partition != p {
        return Err(NasaRedisError::OwnershipChanged(format!(
            "分区 {p} 与持有权证据分区 {} 不一致",
            lease.partition
        )));
    }
    if park_id.is_empty()
        || lease
            .expected_park_id
            .is_some_and(|expected| expected != park_id)
    {
        return Err(NasaRedisError::OperationConflict(format!(
            "分区 {p} 处置批次与前序身份不一致"
        )));
    }
    const GUARD_LUA: &str = r#"
-- KEYS 为 marker、锁、fence、隔离区、索引、目标 Stream；ARGV 前九项绑定持有权、批次、操作与状态。
-- 类型读取和全部拒绝条件先于操作正文，避免 Lua 不回滚时出现已写入却因旧证据继续失败的状态。
for i = 1, 5 do
    local kind = redis.call('TYPE', KEYS[i])['ok']
    local expected = 'hash'
    if i == 5 then
        expected = 'string'
    end
    if kind ~= 'none' and kind ~= expected then
        return {'PROTOCOL', 'KEY_TYPE'}
    end
end
if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 0 then
    return {'OWNERSHIP', 'NOT_HOLDER'}
end
if ARGV[2] == '1' then
    if redis.call('HGET', KEYS[3], 'state') ~= 'Ready' then
        return {'OWNERSHIP', 'FENCE_NOT_READY'}
    end
    if redis.call('HGET', KEYS[3], 'round') ~= ARGV[3] then
        return {'OWNERSHIP', 'ROUND_MISMATCH'}
    end
    if redis.call('HGET', KEYS[3], 'nonce') ~= ARGV[4] then
        return {'OWNERSHIP', 'NONCE_MISMATCH'}
    end
    if redis.call('HGET', KEYS[3], 'c:' .. ARGV[5]) ~= ARGV[6] then
        return {'OWNERSHIP', 'STALE_STAMP'}
    end
end
if redis.call('HGET', KEYS[1], 'park_id') ~= ARGV[7]
    or redis.call('HGET', KEYS[1], 'state') ~= ARGV[9] then
    return {'CONFLICT', 'BATCH_OR_STATE'}
end
-- 认领计数不能从负值继续累加，否则可能先改变状态、再因无符号响应解析失败留下半完成意图。
if ARGV[9] == 'Parked' or ARGV[9] == 'ResumeIndeterminate' then
    local version = redis.call('HGET', KEYS[1], 'version')
    if version and string.sub(version, 1, 1) == '-' then
        return {'PROTOCOL', 'VERSION'}
    end
end
local index = redis.call('GET', KEYS[5])
if index and index ~= ARGV[7] then
    return {'CONFLICT', 'INDEX'}
end
-- 终态补清理与新转换认领由当前 owner 授权；已进入发布阶段后必须保持原操作，旧空身份保留续作兼容。
if ARGV[9] ~= 'Resumed' and ARGV[9] ~= 'Dlqed' and ARGV[9] ~= 'Dropped'
    and ARGV[9] ~= 'Parked' and ARGV[9] ~= 'ResumeIndeterminate' then
    local operation = redis.call('HGET', KEYS[1], 'transition_operation_id')
    if operation and operation ~= '' and operation ~= ARGV[8] then
        return {'CONFLICT', 'OPERATION'}
    end
end
"#;
    // 固定门禁与操作正文一次执行；普通只读校验返回后再发命令仍会重新出现失权窗口。
    let script = redis::Script::new(&format!("{GUARD_LUA}\n{body}"));
    let (tag, value): (String, redis::Value) = script
        .key(marker_key(layout, p))
        .key(lease.lock_key)
        .key(lease.lua_fence_key())
        .key(quarantine_key(layout, park_id))
        .key(parked_index_key(layout, p))
        .key(stream)
        .arg(lease.holder)
        .arg(lease.has_fence())
        .arg(lease.round.to_string())
        .arg(lease.nonce)
        .arg(p.to_string())
        .arg(lease.counter.to_string())
        .arg(park_id)
        .arg(lease.operation_id)
        .arg(state)
        .arg(arguments)
        .invoke_async(&mut client.conn())
        .await?;
    match tag.as_str() {
        "OK" => Ok(value),
        // 失权与处置冲突分别交给权威撤销和管理重试路径，不能降级为可继续发布。
        "OWNERSHIP" => Err(NasaRedisError::OwnershipChanged(format!(
            "分区 {p} 处置副作用被持有权校验拒绝"
        ))),
        "CONFLICT" => Err(NasaRedisError::OperationConflict(format!(
            "分区 {p} 处置副作用与当前批次、状态或操作冲突"
        ))),
        _ => Err(NasaRedisError::ProtocolMarker(format!(
            "分区 {p} 处置记录或脚本响应异常"
        ))),
    }
}

/// 业务作用：重入时核对已登记的转换操作身份，阻止不同操作接续已知在途转换。
///
/// 参数说明：
/// - `client`: 读取转换身份的 Redis 客户端。
/// - `marker`: 分区处置状态 marker key。
/// - `op_id`: 本次要求重入的稳定操作标识。
///
/// 返回：记录为空或匹配时成功；已登记其它操作时返回 OperationConflict，读取失败透传。
/// 空身份兼容不构成独占证明，调用方仍需按状态执行认领与持有权校验。
async fn assert_op_owns(client: &Arc<RedisClient>, marker: &str, op_id: &str) -> Result<()> {
    let owner: Option<String> = client.h_get(marker, "transition_operation_id").await?;
    match owner.as_deref() {
        None | Some("") => Ok(()),
        Some(o) if o == op_id => Ok(()),
        // 保留专属冲突类型，命令层据接纳窗口决定继续等待或拒绝，不把它当作已执行的副作用。
        Some(o) => Err(NasaRedisError::OperationConflict(format!(
            "在途转换属 operation={o},本 operation={op_id} 不得接管(交原 op 或新 op 显式重授权)"
        ))),
    }
}

/// 业务作用：为自动处置生成可由接管方重建的操作身份，无需依赖外部调用方重新提交。
///
/// 参数说明：
/// - `p`: 分区编号。
/// - `park_id`: Park 批次 ID。
///
/// 返回：稳定的 auto 前缀、分区和批次组合；相同坐标重建相同操作，不授予执行权限。
pub fn auto_op_id(p: u32, park_id: &str) -> String {
    format!("auto:{p}:{park_id}")
}

/// 业务作用：是否 auto-poison 处置 op(接管方据此判定可否自动续作在途 `*Publishing`)。
///
/// 参数说明：
/// - `op_id`: 要判断的 operation ID。
///
/// 返回：具有 auto 前缀时为真；只识别命名约定，不验证后续坐标、来源或执行权限。
pub fn is_auto_op(op_id: &str) -> bool {
    op_id.starts_with("auto:")
}

/// 业务作用：读 marker 当前 version(诊断/审计;无 marker 或无 version 字段 → 0)。
///
/// 参数说明：
/// - `client`: 读取 marker 的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
///
/// 返回：当前版本观测，缺字段时为零；通信或类型错误透传，单次读取不证明某操作已经完成。
pub async fn marker_version(client: &Arc<RedisClient>, layout: &KeyLayout, p: u32) -> Result<u64> {
    Ok(client
        .h_get::<u64>(&marker_key(layout, p), "version")
        .await?
        .unwrap_or(0))
}

/// 业务作用：按隔离区声明的条数读取原始载荷，缺失恢复数据时拒绝继续重投。
///
/// 参数说明：
/// - `client`: 读取隔离区的 Redis 客户端。
/// - `qkey`: 当前批次的恢复数据键。
/// - `p`: 用于异常定位的分区编号。
/// - `park_id`: 用于异常定位的 Park 批次身份。
///
/// 返回：按原序号排列的完整载荷；条数无法由现有字段支撑、容量不可表示或预留失败、任一载荷缺失时拒绝。
/// 多次读取不构成原子快照，调用方必须维持该批次恢复数据的生命周期。
async fn load_quarantine_payloads(
    client: &Arc<RedisClient>,
    qkey: &str,
    p: u32,
    park_id: &str,
) -> Result<Vec<Vec<u8>>> {
    let n: Option<u64> = client.h_get(qkey, "n").await?;
    let Some(n) = n else {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "quarantine 损坏:分区 {p} park_id={park_id} 缺 'n' 字段,拒绝处置(fail-closed)"
        )));
    };
    // 持久计数不是分配授权；至少需要 n 个正文加计数字段，先拒绝不可能成立的批次规模。
    let fields: u64 = redis::cmd("HLEN")
        .arg(qkey)
        .query_async(&mut client.conn())
        .await?;
    if n.checked_add(1).is_none_or(|minimum| minimum > fields) {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "quarantine 损坏:分区 {p} park_id={park_id} 声明 {n} 条但仅有 {fields} 个字段"
        )));
    }
    // 不窄化持久数量，也不让容量溢出或可报告的预留失败绕过错误返回并终止执行器。
    let capacity = usize::try_from(n).map_err(|_| {
        NasaRedisError::ProtocolMarker(format!(
            "quarantine 条数超出本机可表示范围:分区 {p} park_id={park_id} n={n}"
        ))
    })?;
    let mut payloads = Vec::new();
    payloads.try_reserve_exact(capacity).map_err(|_| {
        NasaRedisError::ProtocolMarker(format!(
            "quarantine 载荷索引容量不可预留:分区 {p} park_id={park_id} n={n}"
        ))
    })?;
    for i in 0..n {
        let pl: Option<Vec<u8>> = client.h_get(qkey, &i.to_string()).await?;
        match pl {
            Some(v) => payloads.push(v),
            None => {
                return Err(NasaRedisError::ProtocolMarker(format!(
                    "quarantine 损坏:分区 {p} park_id={park_id} 缺第 {i} 条 payload(n={n}),\
                     拒绝处置(fail-closed,禁空字节替代)"
                )))
            }
        }
    }
    Ok(payloads)
}

/// 业务作用：读取恢复载荷后核对已持久化的预约数量，避免重入时把不同长度数据配对发布。
///
/// 参数说明：
/// - `client`: 读取恢复数据的 Redis 客户端。
/// - `qkey`: 本批次隔离区键。
/// - `p`: 操作所属分区编号。
/// - `park_id`: 本次恢复的 Park 批次身份。
/// - `expected`: 已持久化 planned 列表的长度。
///
/// 返回：完整载荷且数量匹配时成功；读取失败或长度不符时拒绝，尚未发布任何预约记录。
async fn load_quarantine_payloads_for(
    client: &Arc<RedisClient>,
    qkey: &str,
    p: u32,
    park_id: &str,
    expected: usize,
) -> Result<Vec<Vec<u8>>> {
    let payloads = load_quarantine_payloads(client, qkey, p, park_id).await?;
    if payloads.len() != expected {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "quarantine 损坏:分区 {p} park_id={park_id} payload 数 {} 与已持久化 planned 数 {expected} 不符",
            payloads.len()
        )));
    }
    Ok(payloads)
}

/// fenced PARK 脚本(写入顺序见文件头;返回 1 成功 / 文本 = 拒绝原因)。
///
/// Redis Lua 运行时错误不回滚已执行写入；因此所有键类型、marker 状态及 PEL 必须在写入前校验。
/// 先保留恢复载荷，再确认源消息，最后发布冻结事实；其它脚本异常仍可能留下部分持久状态。
const PARK_LUA: &str = r#"
-- KEYS[1]=分区锁 KEYS[2]=源 Stream KEYS[3]=quarantine KEYS[4]=marker KEYS[5]=parked 索引 KEYS[6]=fence
-- ARGV[1]=holder ARGV[2]=group ARGV[3]=park_id ARGV[4]=墙钟ms ARGV[5]=条数 n
-- ARGV[6]=启用 fence ARGV[7]=round ARGV[8]=nonce ARGV[9]=分区 ARGV[10]=counter
-- ARGV[11..] 为 id,payload 对；所有权威和版本检查先于恢复数据写入与源消息确认。

-- ① 写前 TYPE 预检(none 容忍;非预期类型即拒,绝不进入写阶段)
local function type_bad(key, want)
    local t = redis.call('TYPE', key)['ok']
    return t ~= 'none' and t ~= want
end
if type_bad(KEYS[1], 'hash')   then return 'PRECHECK_LOCK_TYPE' end
if type_bad(KEYS[2], 'stream') then return 'PRECHECK_STREAM_TYPE' end
if type_bad(KEYS[3], 'hash')   then return 'PRECHECK_QUARANTINE_TYPE' end
if type_bad(KEYS[4], 'hash')   then return 'PRECHECK_MARKER_TYPE' end
if type_bad(KEYS[5], 'string') then return 'PRECHECK_INDEX_TYPE' end
if ARGV[6] == '1' and type_bad(KEYS[6], 'hash') then
    return 'PRECHECK_FENCE_TYPE'
end

-- ② holder 校验(类型已确认是 hash,hexists 安全)
if redis.call('hexists', KEYS[1], ARGV[1]) == 0 then
    return 'NOT_HOLDER'
end
-- holder 仍在锁中不代表原分区任期仍有效；ACK 必须与来源捕获的 fence 一起校验。
if ARGV[6] == '1' then
    if redis.call('HGET', KEYS[6], 'state') ~= 'Ready' then
        return 'FENCE_NOT_READY'
    end
    if redis.call('HGET', KEYS[6], 'round') ~= ARGV[7] then
        return 'ROUND_MISMATCH'
    end
    if redis.call('HGET', KEYS[6], 'nonce') ~= ARGV[8] then
        return 'NONCE_MISMATCH'
    end
    if redis.call('HGET', KEYS[6], 'c:' .. ARGV[9]) ~= ARGV[10] then
        return 'STALE_STAMP'
    end
end

-- ③ marker 状态预检:幂等重入(同 park_id)直接成功;在途的异 park_id 处置拒绝覆盖
local mstate = redis.call('HGET', KEYS[4], 'state')
local mpark  = redis.call('HGET', KEYS[4], 'park_id')
if mstate == 'Parked' and mpark == ARGV[3] then
    return 1
end
if mstate ~= false and mstate ~= 'Resumed' and mstate ~= 'Dropped' and mstate ~= 'Dlqed' then
    return 'PRECHECK_MARKER_STATE:' .. tostring(mstate)
end
if mstate == false and redis.call('EXISTS', KEYS[4]) ~= 0 then
    return 'PRECHECK_MARKER_STATE_MISSING'
end

-- 版本必须在 ACK 前证明可递增；十进制字符串运算避免 Lua 浮点舍入和非法值降为零。
local next_ver = '1'
if mstate then
    local previous = redis.call('HGET', KEYS[4], 'version') or '0'
    if not string.match(previous, '^%d+$')
        or (#previous > 1 and string.sub(previous, 1, 1) == '0')
        or #previous > 19
        or (#previous == 19 and previous >= '9223372036854775807') then
        return 'PRECHECK_VERSION'
    end
    local digits = {}
    local carry = 1
    for i = #previous, 1, -1 do
        local digit = string.byte(previous, i) - 48 + carry
        if digit == 10 then
            digits[i] = '0'
            carry = 1
        else
            digits[i] = string.char(48 + digit)
            carry = 0
        end
    end
    next_ver = table.concat(digits)
    if carry == 1 then
        next_ver = '1' .. next_ver
    end
end

-- ④ 目标 ID 仍在本组 PEL(读;不在 = 状态已变,拒绝)
local n = tonumber(ARGV[5])
for i = 0, n - 1 do
    local id = ARGV[11 + i * 2]
    local pend = redis.call('XPENDING', KEYS[2], ARGV[2], id, id, 1)
    if #pend == 0 then
        return 'NOT_IN_PEL:' .. id
    end
end

-- ⑤ 转存(先于 ACK:源被确认前 payload 必有副本;同 key 重入 = 幂等覆盖)
for i = 0, n - 1 do
    redis.call('HSET', KEYS[3], tostring(i), ARGV[12 + i * 2])
    redis.call('HSET', KEYS[3], 'id:' .. tostring(i), ARGV[11 + i * 2])
end
redis.call('HSET', KEYS[3], 'n', ARGV[5])
-- ⑥ ACK 源(消息出 PEL;源 stream 此后可被 trim,payload 已安全)
for i = 0, n - 1 do
    redis.call('XACK', KEYS[2], ARGV[2], ARGV[11 + i * 2])
end
-- 新批次不能继承终态的预约、操作字段或 TTL；版本已在写前按原值计算，跨批次继续递增。
if mstate then
    redis.call('DEL', KEYS[4])
end
redis.call('HSET', KEYS[4], 'state', 'Parked', 'park_id', ARGV[3], 'n', ARGV[5], 'parked_at', ARGV[4], 'version', next_ver)
redis.call('SET', KEYS[5], ARGV[3])
return 1
"#;

/// 业务作用：原子复验原来源的持有权、任期、版本和 PEL，再保存恢复载荷并确认源消息、冻结分区。
///
/// 参数说明：
/// - `client`: 执行 fenced park Lua 的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `lease`: 原消息来源的分区、锁及可选 fence 凭据；不能用后来取得的新任期替代。
/// - `records`: 要转存到 quarantine 的源 entry ID 与 envelope payload。
///
/// 返回：确认 Park 成功时返回新批次 ID；坐标不符、空批次、墙钟不可表示、校验拒绝或 Redis 错误时失败。
/// 脚本异常和通信失败不代表没有部分写入；parked_at 使用本地墙钟，只用于记录处置时刻。
pub async fn park(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    lease: &OwnerLease<'_>,
    records: &[(String, Vec<u8>)],
) -> Result<String> {
    // 原来源凭据不能授权另一分区的 ACK；锁名不能代替明确的坐标约束。
    if lease.partition != p {
        return Err(NasaRedisError::OwnershipChanged(format!(
            "分区 {p} 与 Park 持有权证据分区 {} 不一致",
            lease.partition
        )));
    }
    // 没有载荷的批次不能建立冻结事实，否则会留下无消息可恢复的 Park 状态。
    if records.is_empty() {
        return Err(NasaRedisError::Config(format!(
            "分区 {p} park 被拒:records 为空(不允许 park 空批,防分区冻结 + 毒消息静默丢失)"
        )));
    }
    let park_id = uuid::Uuid::new_v4().simple().to_string();
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| NasaRedisError::Config("Park 墙钟早于 Unix epoch".into()))?;
    let now_ms = u64::try_from(elapsed.as_millis())
        .map_err(|_| NasaRedisError::Config("Park 墙钟毫秒超出 u64 范围".into()))?;

    // Script 必须先绑定到具名变量(prepare_invoke 借用它;临时值会在语句末被释放)
    let lua = redis::Script::new(PARK_LUA);
    let mut script = lua.prepare_invoke();
    script
        .key(lease.lock_key)
        .key(layout.stream(p))
        .key(quarantine_key(layout, &park_id))
        .key(marker_key(layout, p))
        .key(parked_index_key(layout, p))
        .key(lease.lua_fence_key())
        .arg(lease.holder)
        .arg(layout.group())
        .arg(&park_id)
        .arg(now_ms)
        .arg(records.len())
        .arg(lease.has_fence())
        .arg(lease.round)
        .arg(lease.nonce)
        .arg(p)
        .arg(lease.counter);
    for (id, payload) in records {
        script.arg(id.as_str()).arg(payload.as_slice());
    }
    let r: redis::Value = script.invoke_async(&mut client.conn()).await?;
    match r {
        redis::Value::Int(1) => Ok(park_id),
        other => Err(NasaRedisError::Config(format!(
            "park 被拒绝(fenced 校验失败): {other:?}"
        ))),
    }
}

/// 业务作用：将接管快照映射为管理查询结果，损坏状态不能被报告为未冻结。
///
/// 参数说明：
/// - `client`: 读取 marker/index 的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
///
/// 返回：冻结或自动 DLQ 待续作时返回批次身份，确定未冻结时为 None；不一致或读取失败返回错误。
pub async fn parked_id(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
) -> Result<Option<String>> {
    match takeover_disposition(client, layout, p).await? {
        TakeoverDisposition::NotParked => Ok(None),
        TakeoverDisposition::Frozen {
            park_id,
        }
        | TakeoverDisposition::AutoResumeDlq {
            park_id,
            ..
        } => Ok(Some(park_id)),
        TakeoverDisposition::Inconsistent {
            reason,
        } => Err(NasaRedisError::ProtocolMarker(format!(
            "分区 {p} disposition 状态不一致/损坏: {reason}"
        ))),
    }
}

/// 接管时的 disposition 归约结果(不能只信 parked index——marker=Parked
/// 而 index 缺失会被当"未 Park"恢复消费 = fail-open)。**marker 是权威事实源**,index
/// 仅作加速/校验。
#[derive(Debug)]
pub enum TakeoverDisposition {
    /// 无任何处置在途(marker 缺失/已终态)→ 可进 Recovering/Ready。
    NotParked,
    /// 处置在途(marker 非终态)→ 冻结分区禁业务 poll(park_id 来自 marker)。
    Frozen {
        /// marker 确认的恢复批次。
        park_id: String,
    },
    /// **auto-DLQ 在途**（marker=`DlqPublishing` 且 op 为 `auto:`）：无外部重提主体，接管方
    /// **自动续作** dlq_from_parked 到终态(成功→恢复消费,基础设施错误→暂冻结下轮再续),
    /// 不像普通 `*Publishing` 那样永久等 producer/admin 重提。
    AutoResumeDlq {
        /// 等待自动继续发布的恢复批次。
        park_id: String,
        /// 必须沿用的自动转换身份。
        operation_id: String,
    },
    /// marker/index 不一致或损坏 → fail-closed,放弃接管(由调用方释锁退避 + 告警)。
    Inconsistent {
        /// 当前快照不能安全接管的具体原因。
        reason: String,
    },
}

/// marker 的非终态(冻结业务消费)与终态集合。
const DISPOSITION_NON_TERMINAL: &[&str] = &[
    "Parked",
    // PARK 脚本直接写 Parked；其它未知状态不能推断为可恢复的冻结态。
    "ResumePublishing",
    "DlqPublishing",
    // 强制重发中断仍保留恢复意图；接管时冻结普通消费，由原操作或显式移交继续。
    "ForcePublishing",
    "Dropping",
    "ResumeIndeterminate",
    "DlqIndeterminate",
];
const DISPOSITION_TERMINAL: &[&str] = &["Resumed", "Dlqed", "Dropped"];

// marker 与 index 必须来自同一 Redis 执行时刻，避免把合法的原子 Park 转换拼成元数据缺失。
// Redis 空值以 false 占位，使返回数组始终保留四个字段；同组布局保证两 key 位于同一 slot。
const DISPOSITION_SNAPSHOT_LUA: &str = r#"
return {
    redis.call('HGET', KEYS[1], 'state'),
    redis.call('HGET', KEYS[1], 'park_id'),
    redis.call('HGET', KEYS[1], 'transition_operation_id'),
    redis.call('GET', KEYS[2])
}
"#;

/// 业务作用：以原子快照归约接管时的 disposition，marker 为权威，index 仅校验。
///
/// 参数说明：
/// - `client`：读取一致快照的 Redis 客户端。
/// - `layout`：marker 与索引处于同槽的分区组布局。
/// - `p`：待接管的物理分区编号。
///
/// 返回：完整快照决定冻结、续作或放行；状态损坏返回 Inconsistent，读取失败上抛并拒绝接管。
pub async fn takeover_disposition(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
) -> Result<TakeoverDisposition> {
    // 任何字段读取失败（含 WRONGTYPE）都拒绝本次快照，不能凭部分信息开放业务来源。
    let (mstate, mpark, mop, index): (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = redis::Script::new(DISPOSITION_SNAPSHOT_LUA)
        .key(marker_key(layout, p))
        .key(parked_index_key(layout, p))
        .invoke_async(&mut client.conn())
        .await?;

    match mstate.as_deref() {
        // marker 非终态:**冻结**(marker 权威,无论 index 是否一致)。但 park_id 缺失/为空时
        // 无法构造可恢复的 Parked(resume/drop 找不到 park_id)→ fail-closed。
        Some(s) if DISPOSITION_NON_TERMINAL.contains(&s) => {
            let park_id = mpark.unwrap_or_default();
            if park_id.is_empty() {
                return Ok(TakeoverDisposition::Inconsistent {
                    reason: format!("disposition marker 非终态({s})但缺 park_id,无法恢复"),
                });
            }
            match &index {
                Some(idx) if *idx == park_id => {}
                _ => tracing::warn!(
                    p,
                    marker_state = s,
                    ?index,
                    marker_park = %park_id,
                    "disposition marker 非终态但 parked index 不一致(以 marker 为准冻结)"
                ),
            }
            // auto-DLQ 在途（DlqPublishing + `auto:` op）由接管方自动续作，不能永久冻结。
            // 只有 auto-DLQ 这一种 owner 内部触发的在途态可自动续(无外部重提主体);其余
            // 其它 *Publishing（producer resume/dlq、direct）保持 Frozen，等待原 op 或 admin 重提。
            if s == "DlqPublishing" {
                if let Some(op) = mop.as_deref() {
                    if is_auto_op(op) {
                        return Ok(TakeoverDisposition::AutoResumeDlq {
                            park_id,
                            operation_id: op.to_string(),
                        });
                    }
                }
            }
            Ok(TakeoverDisposition::Frozen { park_id })
        }
        // marker 已终态:不冻结。index 若仍存在 = 陈旧索引,告警放行(终态 marker 明确证明
        // 处置已完成,放行安全)
        Some(s) if DISPOSITION_TERMINAL.contains(&s) => {
            if index.is_some() {
                tracing::warn!(p, marker_state = s, ?index, "disposition marker 已终态但 parked index 仍在(陈旧,放行消费)");
            }
            Ok(TakeoverDisposition::NotParked)
        }
        // 未知 marker 状态:fail-closed(不猜测)
        Some(other) => Ok(TakeoverDisposition::Inconsistent {
            reason: format!("未知 disposition marker 状态: {other}"),
        }),
        // marker 缺失:
        //   · index 也缺失 → 真正未 Park,放行;
        //   · index 存在 → **不确定**(parked index 是 Park 的持久证据,
        //     marker 缺失可能是 marker 被误删/淘汰/部分恢复/历史迁移——不能猜成"陈旧索引"
        //     就消费)→ fail-closed Inconsistent,人工对账。
        None => match index {
            None => Ok(TakeoverDisposition::NotParked),
            Some(idx) => Ok(TakeoverDisposition::Inconsistent {
                reason: format!("parked index 存在(park_id={idx})但 disposition marker 缺失:Park 元数据可能部分丢失,需对账"),
            }),
        },
    }
}

/// 业务作用：将 Stream ID 拆为毫秒与序号，供预约递增和先后关系判断。
///
/// 参数说明：
/// - `s`: 采用毫秒与序号两段形式的 Stream ID。
///
/// 返回：两段均为非空十进制数字且可表示为 u64 时返回数值对；符号、空白、溢出或结构异常返回 None。
fn parse_sid(s: &str) -> Option<(u64, u64)> {
    let (ms, seq) = s.split_once('-')?;
    // Stream ID 只接受无符号数字文本；整数解析器允许的前导加号不能作为有效线路证据。
    if ms.is_empty()
        || seq.is_empty()
        || !ms.bytes().all(|byte| byte.is_ascii_digit())
        || !seq.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    Some((ms.parse().ok()?, seq.parse().ok()?))
}

/// 业务作用：从已知最大 Stream ID 之后生成严格递增的预约序列，防止序号回绕覆盖旧坐标。
///
/// 参数说明：
/// - `base`: 已观察的最后生成 ID，包含毫秒和序号。
/// - `n`: 本批需要预约的记录数量。
///
/// 返回：按元组顺序递增的 n 个 ID；序号耗尽时向毫秒进位，ID 空间耗尽或索引容量不能预留则拒绝，不提交预约。
fn plan_ids(base: (u64, u64), n: usize) -> Result<Vec<String>> {
    let (mut ms, mut seq) = base;
    // 在构造目标前确认索引容量可表示并可预留；异常批量不能通过容量 panic 中断恢复流程。
    let mut out = Vec::new();
    out.try_reserve_exact(n)
        .map_err(|_| NasaRedisError::ProtocolMarker("预约 ID 索引容量不可预留".into()))?;
    for _ in 0..n {
        if seq == u64::MAX {
            if ms == u64::MAX {
                return Err(NasaRedisError::Config(
                    "StreamIdExhausted: ID 空间耗尽".into(),
                ));
            }
            ms += 1;
            seq = 0;
        } else {
            seq += 1;
        }
        out.push(format!("{ms}-{seq}"));
    }
    Ok(out)
}

/// 业务作用：统一读取 XINFO 的两种文本响应形态，供字段匹配及 ID 解析使用。
///
/// 参数说明：
/// - `v`: 响应中的字段名或字段值。
///
/// 返回：SimpleString 或有效 UTF-8 BulkString 的文本；其它形态或非法编码返回 None，不合成字段或 ID 证据。
fn xinfo_str(v: &redis::Value) -> Option<String> {
    match v {
        redis::Value::SimpleString(s) => Some(s.clone()),
        redis::Value::BulkString(b) => std::str::from_utf8(b).ok().map(str::to_owned),
        _ => None,
    }
}

/// 业务作用：从 RESP2 键值数组或 RESP3 映射中定位 XINFO 字段。
///
/// 参数说明：
/// - `v`: 完整 XINFO 响应，消费后逐对读取。
/// - `field`: 要定位的字段名称。
///
/// 返回：完整且字段名唯一的响应中匹配的文本值；缺失、重复、非法字段名或不完整结构均返回 None。
fn xinfo_field(v: redis::Value, field: &str) -> Option<String> {
    let pairs: Vec<(redis::Value, redis::Value)> = match v {
        redis::Value::Map(pairs) => pairs,
        redis::Value::Array(items) => {
            // 不完整的字段对不能丢弃尾部后当作可用快照，否则预约会使用损坏的最大 ID 证据。
            if items.len() % 2 != 0 {
                return None;
            }
            let mut it = items.into_iter();
            let mut out = Vec::new();
            while let (Some(k), Some(val)) = (it.next(), it.next()) {
                out.push((k, val));
            }
            out
        }
        _ => return None,
    };
    let mut found = None;
    let mut names = std::collections::HashSet::new();
    for (k, val) in pairs {
        let name = xinfo_str(&k)?;
        // 重复字段会让取值依赖响应顺序；连同其它字段的重复一起拒绝，不猜测哪个值有效。
        if !names.insert(name.clone()) {
            return None;
        }
        if name == field {
            found = Some(xinfo_str(&val)?);
        }
    }
    found
}

/// 业务作用：读源 stream 的 last-generated-id(stream 不存在 → 0-0 基准)。
///
/// 参数说明：
/// - `client`: 查询 Stream 元信息的 Redis 客户端。
/// - `stream`: 即将发布预约记录的目标 Stream 键。
///
/// 返回：可解析的最后生成 ID；不存在响应使用零基准，其它读取或格式错误拒绝继续预约。
async fn last_generated(client: &Arc<RedisClient>, stream: &str) -> Result<(u64, u64)> {
    // 只有精确的 ERR/no such key 组合能证明 Stream 缺失；代理附加正文或其它错误码不能授权从零预约。
    let v: redis::Value = match redis::cmd("XINFO")
        .arg("STREAM")
        .arg(stream)
        .query_async(&mut client.conn())
        .await
    {
        Ok(v) => v,
        Err(e) if e.code() == Some("ERR") && e.detail() == Some("no such key") => {
            return Ok((0, 0));
        }
        Err(e) => return Err(e.into()),
    };
    // 响应既非 Map 也非 Array = 协议形态异常,同样不能当 0-0(fail-closed)
    match &v {
        redis::Value::Map(_) | redis::Value::Array(_) => {}
        other => {
            return Err(NasaRedisError::ProtocolMarker(format!(
                "XINFO STREAM {stream} 响应形态异常(非 Map/Array): {other:?}"
            )))
        }
    }
    // RESP3 Map / RESP2 Array 双形态:key/value 一律走兼容 `xinfo_str`(不硬匹配 BulkString,见上)。
    let last = xinfo_field(v, "last-generated-id");
    //stream 存在(非 no-such-key)时 XINFO 必有 last-generated-id;
    // 缺字段/值不可解析 = 协议损坏,**fail-closed**(绝不回 0-0,否则会把规划基准错误地拉回 0-1)。
    match last {
        Some(s) => parse_sid(&s).ok_or_else(|| {
            NasaRedisError::ProtocolMarker(format!(
                "XINFO STREAM {stream} 的 last-generated-id 不可解析: {s:?}"
            ))
        }),
        None => Err(NasaRedisError::ProtocolMarker(format!(
            "XINFO STREAM {stream} 字段结构不完整、重复或缺少有效 last-generated-id"
        ))),
    }
}

/// 业务作用：对账单条预约 ID 的目标记录与原始载荷，避免未知投递结果自动变成新发布。
///
/// 参数说明：
/// - `client`: 读取目标记录及 Stream 元信息的 Redis 客户端。
/// - `stream`: 需要按计划 entry id 复查的 Redis Stream key。
/// - `planned`: 本条记录已持久化的预约 ID。
/// - `expect_payload`: 重发校验时期望的 parked payload。
///
/// 返回：记录与载荷一致时为真，结果不确定时为假；目标尚未被越过时返回 RETRYABLE 信号供同 ID 补发。
/// 读取、ID 或响应结构异常直接拒绝，不把损坏响应当作缺失；假值不能证明从未发布或从未被消费。
async fn reconcile_one(
    client: &Arc<RedisClient>,
    stream: &str,
    planned: &str,
    expect_payload: &[u8],
) -> Result<bool> {
    // 预约身份损坏时不能查询或生成补发依据，尤其不能用极大替代值推断旧目标尚未被越过。
    let pl = parse_sid(planned).ok_or_else(|| {
        NasaRedisError::ProtocolMarker(format!("reconcile:planned ID 不可解析: {planned:?}"))
    })?;
    let rows: redis::Value = redis::cmd("XRANGE")
        .arg(stream)
        .arg(planned)
        .arg(planned)
        .query_async(&mut client.conn())
        .await?;
    let redis::Value::Array(rows) = rows else {
        return Err(NasaRedisError::ProtocolMarker(
            "XRANGE 对账响应必须为记录数组".into(),
        ));
    };
    if !rows.is_empty() {
        // 精确 ID 查询只能有一条同 ID 记录；额外记录、字段或缺项都不能证明本预约已发布。
        let [redis::Value::Array(pair)] = rows.as_slice() else {
            return Err(NasaRedisError::ProtocolMarker(
                "XRANGE 精确对账返回多条记录或非法记录结构".into(),
            ));
        };
        let [id, redis::Value::Array(fields)] = pair.as_slice() else {
            return Err(NasaRedisError::ProtocolMarker(
                "XRANGE 记录必须包含 ID 和完整字段列表".into(),
            ));
        };
        if xinfo_str(id).and_then(|id| parse_sid(&id)) != Some(pl)
            || fields.is_empty()
            || fields.len() % 2 != 0
        {
            return Err(NasaRedisError::ProtocolMarker(
                "XRANGE 记录 ID 不符或字段列表不完整".into(),
            ));
        }
        let mut names = std::collections::HashSet::new();
        let mut payload_matches = None;
        for field in fields.chunks_exact(2) {
            let name = match &field[0] {
                redis::Value::BulkString(bytes) => bytes.as_slice(),
                redis::Value::SimpleString(value) => value.as_bytes(),
                _ => {
                    return Err(NasaRedisError::ProtocolMarker(
                        "XRANGE 字段名不是字节字符串".into(),
                    ));
                }
            };
            let value = match &field[1] {
                redis::Value::BulkString(bytes) => bytes.as_slice(),
                redis::Value::SimpleString(value) => value.as_bytes(),
                _ => {
                    return Err(NasaRedisError::ProtocolMarker(
                        "XRANGE 字段值不是字节字符串".into(),
                    ));
                }
            };
            // 完整遍历后才接受正文，避免首个 data 匹配掩盖重复字段或损坏的尾部。
            if !names.insert(name) {
                return Err(NasaRedisError::ProtocolMarker(
                    "XRANGE 对账记录包含重复字段".into(),
                ));
            }
            if name == super::DATA_FIELD.as_bytes() {
                payload_matches = Some(value == expect_payload);
            }
        }
        return payload_matches
            .ok_or_else(|| NasaRedisError::ProtocolMarker("XRANGE 对账记录缺少 data 字段".into()));
    }
    // 只有完整的空记录数组才是缺失证据；再以合法 last-generated-id 区分同 ID 可补发与结果不确定。
    let lg = last_generated(client, stream).await?;
    if lg < pl {
        return Err(NasaRedisError::Config("RETRYABLE".into())); // 特殊信号:可补发
    }
    Ok(false)
}

/// 业务作用：按持久预约 ID 将隔离载荷恢复至原 Stream，同操作可继续未完成发布，确定终态补齐清理。
///
/// 参数说明：
/// - `client`: 执行 resume 状态机的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `lease`: 当前 owner 的锁和 fence 凭据。
///
/// 返回：全部发布确认并完成清理时返回目标 ID；不确定时保持冻结并返回错误，不自动另行预约。
/// 处于不确定状态的普通恢复请求被拒绝，通信失败可能保留已经完成的部分副作用。
pub async fn resume(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    lease: &OwnerLease<'_>,
) -> Result<Vec<String>> {
    // 内部调用也不能用另一分区的锁证据授权恢复或终态清理。
    if lease.partition != p {
        return Err(NasaRedisError::OwnershipChanged(format!(
            "分区 {p} 与持有权证据分区 {} 不一致",
            lease.partition
        )));
    }
    // 入口 owner-fence:所有分支(含 ResumePublishing 重入)进入前先校验 owner
    verify_owner_fenced(client, lease).await?;
    let marker = marker_key(layout, p);
    let state: Option<String> = client.h_get(&marker, "state").await?;
    let park_id: Option<String> = client.h_get(&marker, "park_id").await?;
    let (Some(state), Some(park_id)) = (state, park_id) else {
        return Err(NasaRedisError::Config(format!("分区 {p} 无 Park 记录")));
    };
    // 上一步确认的批次是续作边界，重新读取到新批次时不能沿用旧接管意图。
    if lease
        .expected_park_id
        .is_some_and(|expected| expected != park_id.as_str())
    {
        return Err(NasaRedisError::OperationConflict(format!(
            "分区 {p} 恢复续作的 Park 批次已变化"
        )));
    }
    let stream = layout.stream(p);
    let qkey = quarantine_key(layout, &park_id);

    match state.as_str() {
        "Parked" => {
            // 新转换也必须绑定已捕获批次；状态同为 Parked 不能证明等待期间仍是原记录。
            const CLAIM_LUA: &str = r#"
redis.call('HINCRBY', KEYS[1], 'version', 1)
redis.call('HSET', KEYS[1], 'state', 'ResumePublishing', 'owner_holder', ARGV[1], 'transition_operation_id', ARGV[8])
-- 从持久字符串返回精确计数，避免 Lua 数值转换丢失大整数精度或产生科学计数法。
return {'OK', redis.call('HGET', KEYS[1], 'version')}
"#;
            let _: u64 = redis::from_redis_value(guarded_disposition(
                client, layout, p, lease, &park_id, "Parked", &stream, CLAIM_LUA,
                &[] as &[&str],
            ).await?)?;
            publish_step(
                client, layout, p, &marker, &qkey, &park_id, &stream,
                "Resumed", "ResumeIndeterminate", lease,
            )
            .await
        }
        // 重入(认领后/发布中 crash):state 已 ResumePublishing,续作(planned 缺失则补规划)。
        //:必须**同一 operation** 才能续作(否则 op B 接管 op A 在途转换 → OperationConflict)。
        "ResumePublishing" => {
            assert_op_owns(client, &marker, lease.operation_id).await?;
            publish_step(
                client, layout, p, &marker, &qkey, &park_id, &stream,
                "Resumed", "ResumeIndeterminate", lease,
            )
            .await
        }
        "ResumeIndeterminate" => Err(NasaRedisError::OperationInDoubt(format!(
            "分区 {p} 处于 ResumeIndeterminate:普通 resume 被拒,需 force_republish(接受重复风险)或人工对账"
        ))),
        "Resumed" => {
            // 清理和读取完成 ID 共用终态批次门禁，不能在等待中删去新索引或读回另一批的结果。
            const CLEANUP_LUA: &str = r#"
local ids = redis.call('HGET', KEYS[1], 'new_ids') or ''
redis.call('DEL', KEYS[4], KEYS[5])
redis.call('EXPIRE', KEYS[1], 86400)
return {'OK', ids}
"#;
            let ids: String = redis::from_redis_value(guarded_disposition(
                client, layout, p, lease, &park_id, "Resumed", &stream, CLEANUP_LUA,
                &[] as &[&str],
            ).await?)?;
            Ok(ids.split(',').filter(|s| !s.is_empty()).map(String::from).collect())
        }
        other => Err(NasaRedisError::Config(format!("分区 {p} marker 状态 {other},拒绝 resume"))),
    }
}

/// 业务作用：认领发布态后读取或建立唯一预约列表，再核对恢复载荷并逐条对账发布。
///
/// 参数说明：
/// - `client`: 执行恢复读取及发布的 Redis 客户端。
/// - `layout`: 目标分区组的键布局。
/// - `p`: 当前恢复的分区编号。
/// - `marker`: 分区处置状态 marker key。
/// - `qkey`: 持久原始载荷的隔离区键。
/// - `park_id`: 当前恢复批次，用于异常定位。
/// - `target_stream`: 处置完成后要写入的目标 stream。
/// - `final_state`: 处置成功后 marker 应进入的最终状态。
/// - `indeterminate_state`: 结果不确定时 marker 应进入的保护状态。
/// - `lease`: 从认领到预约持久化和后续发布始终复验的分区权威。
///
/// 返回：全部预约完成时返回 ID；恢复数据或发布失败时返回错误，已持久化的预约和部分投递不撤销。
#[allow(clippy::too_many_arguments)]
async fn publish_step(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    marker: &str,
    qkey: &str,
    park_id: &str,
    target_stream: &str,
    final_state: &str,
    indeterminate_state: &str,
    lease: &OwnerLease<'_>,
) -> Result<Vec<String>> {
    let publishing_state = match (final_state, indeterminate_state) {
        ("Resumed", "ResumeIndeterminate") => "ResumePublishing",
        ("Dlqed", "DlqIndeterminate") => "DlqPublishing",
        _ => {
            return Err(NasaRedisError::ProtocolMarker(
                "预约准备的状态组合不合法".into(),
            ));
        }
    };
    let planned_str: Option<String> = client.h_get(marker, "planned").await?;
    let planned_json = match planned_str.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => s.to_owned(),
        None => {
            // 中断可能尚未保存预约；并发同操作续作仍共享唯一首写，同时复验权威，不能给新批次写入旧计划。
            let payloads = load_quarantine_payloads(client, qkey, p, park_id).await?;
            let planned = plan_ids(last_generated(client, target_stream).await?, payloads.len())?;
            let planned_json = serde_json::to_string(&planned).unwrap();
            const PERSIST_PLAN_LUA: &str = r#"
redis.call('HSETNX', KEYS[1], 'planned', ARGV[10])
return {'OK', redis.call('HGET', KEYS[1], 'planned')}
"#;
            redis::from_redis_value(
                guarded_disposition(
                    client,
                    layout,
                    p,
                    lease,
                    park_id,
                    publishing_state,
                    target_stream,
                    PERSIST_PLAN_LUA,
                    &planned_json,
                )
                .await?,
            )?
        }
    };
    let planned: Vec<String> = serde_json::from_str(&planned_json)
        .map_err(|e| NasaRedisError::Codec(format!("planned 解析失败: {e}")))?;
    let payloads = load_quarantine_payloads_for(client, qkey, p, park_id, planned.len()).await?;
    publish_reserved(
        client,
        layout,
        p,
        park_id,
        lease,
        &planned_json,
        &planned,
        &payloads,
        target_stream,
        final_state,
        indeterminate_state,
    )
    .await
}

/// 业务作用：逐条对账并发布持久预约，全部确认后提交终态，再删除恢复数据并设置保留期限。
///
/// 参数说明：
/// - `client`: 执行对账、发布和清理的 Redis 客户端。
/// - `layout`: 分区组布局，用于定位冻结索引。
/// - `p`: 当前处置的分区编号。
/// - `park_id`: 认领时捕获的恢复批次，所有写入必须保持一致。
/// - `lease`: 在每次副作用内复验的持有权与操作身份。
/// - `planned_json`: 已读取的完整持久预约文本，用于阻止计划被替换后沿用旧发布证据。
/// - `planned`: 已持久化的预约 ID，顺序与载荷一致。
/// - `payloads`: 调用方已核对长度的原始载荷列表。
/// - `target_stream`: 恢复至源 Stream 或 DLQ 的目标键。
/// - `final_state`: 处置成功后 marker 应进入的最终状态。
/// - `indeterminate_state`: 结果不确定时 marker 应进入的保护状态。
///
/// 返回：全部确认并完成清理时返回预约 ID；不确定项登记后返回 PublishIndeterminate，读取和写入失败透传。
/// 已发布项不撤回；每次写入原子复验权威与预约，终态提交和清理同脚本完成，通信失败仍须重入对账。
#[allow(clippy::too_many_arguments)]
async fn publish_reserved(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    park_id: &str,
    lease: &OwnerLease<'_>,
    planned_json: &str,
    planned: &[String],
    payloads: &[Vec<u8>],
    // 泛化:resume 回源 / dlq 写 DLQ stream 共用同一 reservations 协议,
    // 仅目标 stream 与终态/不确定态标签不同(Resumed/ResumeIndeterminate vs Dlqed/DlqIndeterminate)。
    target_stream: &str,
    final_state: &str,
    indeterminate_state: &str,
) -> Result<Vec<String>> {
    let stream = target_stream.to_string();
    let publishing_state = match (final_state, indeterminate_state) {
        ("Resumed", "ResumeIndeterminate") => "ResumePublishing",
        ("Dlqed", "DlqIndeterminate") => "DlqPublishing",
        _ => {
            return Err(NasaRedisError::ProtocolMarker(
                "预约发布的状态组合不合法".into(),
            ));
        }
    };
    let mut uncertain: Vec<usize> = Vec::new();
    for (i, (pid, payload)) in planned.iter().zip(payloads.iter()).enumerate() {
        // 先对账(重入时 entry 可能已在);RETRYABLE 信号 = 可安全首发
        match reconcile_one(client, &stream, pid, payload).await {
            Ok(true) => continue, // 已发布且一致
            Ok(false) => {
                uncertain.push(i);
                continue;
            }
            Err(NasaRedisError::Config(sig)) if sig == "RETRYABLE" => {
                // 缺失证据只决定使用哪个 ID，不能授权未来写入；XADD 与权威、批次和完整预约复验同脚本执行。
                const PUBLISH_LUA: &str = r#"
if redis.call('HGET', KEYS[1], 'planned') ~= ARGV[10] then
    return {'CONFLICT', 'PLANNED'}
end
return {'OK', redis.call('XADD', KEYS[6], ARGV[11], ARGV[12], ARGV[13])}
"#;
                let r = guarded_disposition(
                    client,
                    layout,
                    p,
                    lease,
                    park_id,
                    publishing_state,
                    &stream,
                    PUBLISH_LUA,
                    (planned_json, pid, super::DATA_FIELD, payload.as_slice()),
                )
                .await;
                match r {
                    Ok(value) => {
                        let id: String = redis::from_redis_value(value)?;
                        if parse_sid(&id) != parse_sid(pid) {
                            return Err(NasaRedisError::ProtocolMarker(
                                "预约发布响应与目标 ID 不一致".into(),
                            ));
                        }
                        continue;
                    }
                    //写出后**断线** = 可能已执行 → `ExecutionUnknown` 上抛(留
                    // marker 在 *Publishing 可重入),**不得**把基础设施故障固化成 Indeterminate
                    Err(NasaRedisError::Redis(e))
                        if e.is_io_error() || e.is_timeout() || e.is_connection_dropped() =>
                    {
                        return Err(NasaRedisError::ExecutionUnknown(format!(
                            "分区 {p} 发布 planned={pid} 写出后断线: {e}"
                        )));
                    }
                    // 其它发布错误重新对账同一目标，复查仍失败时继续透传，不能凭错误文本断言已发布。
                    Err(NasaRedisError::Redis(_)) => {
                        match reconcile_one(client, &stream, pid, payload).await {
                            Ok(true) => continue,
                            Ok(false) => {
                                uncertain.push(i);
                                continue;
                            }
                            Err(e) => return Err(e), // 协议/网络:上抛,不固化成 Indeterminate
                        }
                    }
                    // 明确失权、批次变化或协议损坏不能借再次对账改写为成功并继续提交终态。
                    Err(e) => return Err(e),
                }
            }
            Err(e) => return Err(e), // 真实网络/协议错误:上抛(marker 留在 ResumePublishing 可重入)
        }
    }

    if !uncertain.is_empty() {
        // 不确定性只属于被检查的同一预约；状态和索引一起提交，不能把旧结论写到新批次。
        const UNCERTAIN_LUA: &str = r#"
if redis.call('HGET', KEYS[1], 'planned') ~= ARGV[10] then
    return {'CONFLICT', 'PLANNED'}
end
redis.call('HSET', KEYS[1], 'uncertain', ARGV[11], 'state', ARGV[12])
return {'OK', 1}
"#;
        guarded_disposition(
            client,
            layout,
            p,
            lease,
            park_id,
            publishing_state,
            &stream,
            UNCERTAIN_LUA,
            (
                planned_json,
                serde_json::to_string(&uncertain).unwrap(),
                indeterminate_state,
            ),
        )
        .await?;
        return Err(NasaRedisError::PublishIndeterminate(format!(
            "分区 {p} 有 {} 条 reservation 结果不确定(indexes={uncertain:?}):不自动重发,需 force_republish 或人工对账",
            uncertain.len()
        )));
    }

    // 完整发布证据与终态属于同一预约；先写完成事实再删除恢复数据，整个步骤不能被其它持有者穿插。
    const FINISH_LUA: &str = r#"
if redis.call('HGET', KEYS[1], 'planned') ~= ARGV[10] then
    return {'CONFLICT', 'PLANNED'}
end
redis.call('HSET', KEYS[1], 'new_ids', ARGV[11], 'state', ARGV[12])
redis.call('DEL', KEYS[4], KEYS[5])
redis.call('EXPIRE', KEYS[1], 86400)
return {'OK', 1}
"#;
    guarded_disposition(
        client,
        layout,
        p,
        lease,
        park_id,
        publishing_state,
        &stream,
        FINISH_LUA,
        (planned_json, planned.join(","), final_state),
    )
    .await?;
    Ok(planned.to_vec())
}

/// 业务作用：定位分区组共享的 DLQ Stream，供失败载荷转存与独立消费者读取。
///
/// 参数说明：
/// - `layout`: 分区组 key 布局。
///
/// 返回：该组的 DLQ 键；不创建消费者、不设置自动保留策略，也不读取消息。
pub fn dlq_stream_key(layout: &KeyLayout) -> String {
    format!("{}:dlq", layout.prefix)
}

/// 业务作用：按持久预约将隔离消息发布到组级 DLQ，重入继续原计划，确定终态补齐清理。
///
/// 参数说明：
/// - `client`: 执行 DLQ 状态机的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `lease`: 当前 owner 的锁和 fence 凭据。
///
/// 返回：确定发布并完成清理时返回 DLQ ID；结果不确定则保持保护态，普通重入不能自动越过该状态。
/// 中断保留实际已提交的状态和预约，不把已进入发布态的批次回退为 Parked。
pub async fn dlq_from_parked(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    lease: &OwnerLease<'_>,
) -> Result<Vec<String>> {
    // DLQ 与源分区共享处置权威，坐标不一致时不得认领或删除任何批次数据。
    if lease.partition != p {
        return Err(NasaRedisError::OwnershipChanged(format!(
            "分区 {p} 与持有权证据分区 {} 不一致",
            lease.partition
        )));
    }
    verify_owner_fenced(client, lease).await?; // 入口 owner-fence
    let marker = marker_key(layout, p);
    let state: Option<String> = client.h_get(&marker, "state").await?;
    let park_id: Option<String> = client.h_get(&marker, "park_id").await?;
    let (Some(state), Some(park_id)) = (state, park_id) else {
        return Err(NasaRedisError::Config(format!("分区 {p} 无 Park 记录")));
    };
    // 接管或自动续作只能处理已确认的批次；新批次需要独立判断其处置意图。
    if lease
        .expected_park_id
        .is_some_and(|expected| expected != park_id.as_str())
    {
        return Err(NasaRedisError::OperationConflict(format!(
            "分区 {p} DLQ 续作的 Park 批次已变化"
        )));
    }
    let dlq = dlq_stream_key(layout);
    let qkey = quarantine_key(layout, &park_id);
    match state.as_str() {
        "Parked" => {
            // 认领 DLQ 转换时同时绑定批次，避免给等待期间出现的新 Park 写入旧处置意图。
            const CLAIM_LUA: &str = r#"
redis.call('HINCRBY', KEYS[1], 'version', 1)
redis.call('HSET', KEYS[1], 'state', 'DlqPublishing', 'owner_holder', ARGV[1], 'transition_operation_id', ARGV[8])
-- Redis 保存的十进制字符串保留完整整数，不能由 Lua 浮点返回值重建认领计数。
return {'OK', redis.call('HGET', KEYS[1], 'version')}
"#;
            let _: u64 = redis::from_redis_value(
                guarded_disposition(
                    client,
                    layout,
                    p,
                    lease,
                    &park_id,
                    "Parked",
                    &dlq,
                    CLAIM_LUA,
                    &[] as &[&str],
                )
                .await?,
            )?;
            publish_step(
                client,
                layout,
                p,
                &marker,
                &qkey,
                &park_id,
                &dlq,
                "Dlqed",
                "DlqIndeterminate",
                lease,
            )
            .await
        }
        "DlqPublishing" => {
            // 重入(认领后/崩溃恢复):planned 缺失则补规划,逐条对账续作。:须同一 operation。
            assert_op_owns(client, &marker, lease.operation_id).await?;
            publish_step(
                client,
                layout,
                p,
                &marker,
                &qkey,
                &park_id,
                &dlq,
                "Dlqed",
                "DlqIndeterminate",
                lease,
            )
            .await
        }
        "DlqIndeterminate" => Err(NasaRedisError::OperationInDoubt(format!(
            "分区 {p} 处于 DlqIndeterminate:需人工对账(DLQ 重复风险评估后手工处置)"
        ))),
        "Dlqed" => {
            // 终态不是永久删除授权；补清理必须与持有权、批次复验及结果读取处于同一原子步骤。
            const CLEANUP_LUA: &str = r#"
local ids = redis.call('HGET', KEYS[1], 'new_ids') or ''
redis.call('DEL', KEYS[4], KEYS[5])
redis.call('EXPIRE', KEYS[1], 86400)
return {'OK', ids}
"#;
            let ids: String = redis::from_redis_value(
                guarded_disposition(
                    client,
                    layout,
                    p,
                    lease,
                    &park_id,
                    "Dlqed",
                    &dlq,
                    CLEANUP_LUA,
                    &[] as &[&str],
                )
                .await?,
            )?;
            Ok(ids
                .split(',')
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect())
        }
        other => Err(NasaRedisError::Config(format!(
            "分区 {p} marker 状态 {other},拒绝 dlq"
        ))),
    }
}

/// 业务作用：显式接受重复及变序风险后，对不确定记录重新对账并按需分配新目标，同操作可继续在途重发。
///
/// 参数说明：
/// - `client`: 执行 force republish 状态机的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `lease`: 当前 owner 的锁和 fence 凭据。
///
/// 返回：完成后给出每条记录的最终目标 ID；非法恢复数据先拒绝，网络或发布失败保留实际持久进度。
/// 被越过的目标无法证明从未发布；继续分配新目标可能重复，不能作为恰好一次投递承诺。
pub async fn force_republish(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    lease: &OwnerLease<'_>,
) -> Result<Vec<String>> {
    // 接受重复风险只针对指定分区，不能因此跳过分区持有权坐标校验。
    if lease.partition != p {
        return Err(NasaRedisError::OwnershipChanged(format!(
            "分区 {p} 与持有权证据分区 {} 不一致",
            lease.partition
        )));
    }
    verify_owner_fenced(client, lease).await?; // 入口 owner-fence(ForcePublishing 重入也校验)
    let marker = marker_key(layout, p);
    let state: Option<String> = client.h_get(&marker, "state").await?;
    let st = state.as_deref();
    if !matches!(st, Some("ResumeIndeterminate") | Some("ForcePublishing")) {
        return Err(NasaRedisError::Config(format!(
            "分区 {p} 非 ResumeIndeterminate(当前 {st:?}),force_republish 被拒"
        )));
    }
    //:ForcePublishing 重入必须是同一 operation(否则 op B 接管 op A 的 force → OperationConflict)
    if st == Some("ForcePublishing") {
        assert_op_owns(client, &marker, lease.operation_id).await?;
    }

    // 先只读校验恢复数据再转换状态；证据不完整时保留当前状态，禁止登记无法继续的发布意图。
    let park_id: String = client.h_get(&marker, "park_id").await?.unwrap_or_default();
    if park_id.is_empty() {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "分区 {p} force: marker 缺 park_id(损坏),拒绝(fail-closed)"
        )));
    }
    // 重发授权属于前序确认的批次；新批次不能继承旧的不确定性判断或重复风险授权。
    if lease
        .expected_park_id
        .is_some_and(|expected| expected != park_id.as_str())
    {
        return Err(NasaRedisError::OperationConflict(format!(
            "分区 {p} 强制重发续作的 Park 批次已变化"
        )));
    }
    let qkey = quarantine_key(layout, &park_id);
    // 保留持久文本供副作用时逐字节复验；解析成功不代表等待之后仍是同一份重发计划。
    let planned_json: String = client.h_get(&marker, "planned").await?.ok_or_else(|| {
        NasaRedisError::ProtocolMarker(format!("分区 {p} force: marker 缺 planned 字段"))
    })?;
    let planned: Vec<String> = serde_json::from_str(&planned_json).map_err(|e| {
        NasaRedisError::ProtocolMarker(format!("分区 {p} force: planned 损坏不可解析: {e}"))
    })?;
    let uncertain_json: String = client.h_get(&marker, "uncertain").await?.ok_or_else(|| {
        NasaRedisError::ProtocolMarker(format!("分区 {p} force: marker 缺 uncertain 字段"))
    })?;
    let uncertain: Vec<usize> = serde_json::from_str(&uncertain_json).map_err(|e| {
        NasaRedisError::ProtocolMarker(format!("分区 {p} force: uncertain 损坏不可解析: {e}"))
    })?;
    if uncertain.is_empty() {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "分区 {p} force: uncertain 为空,不应进入 force(marker 语义不一致)"
        )));
    }
    // 重复 index 会让同一 payload 被重发多次，越界则无法取得原 planned ID，均在副作用前拒绝。
    let mut seen = std::collections::HashSet::with_capacity(uncertain.len());
    for &i in &uncertain {
        if i >= planned.len() {
            return Err(NasaRedisError::ProtocolMarker(format!(
                "分区 {p} force: uncertain index {i} 越界(planned.len={}),marker 损坏,拒绝",
                planned.len()
            )));
        }
        if !seen.insert(i) {
            return Err(NasaRedisError::ProtocolMarker(format!(
                "分区 {p} force: uncertain 含重复 index {i},marker 损坏,拒绝(fail-closed)"
            )));
        }
    }
    // 预读全部 uncertain payload；缺任一条即 fail-closed，避免进入发布阶段后才发现数据不完整。
    let mut payloads: Vec<(usize, Vec<u8>)> = Vec::with_capacity(uncertain.len());
    for &i in &uncertain {
        let payload: Vec<u8> = client.h_get::<Vec<u8>>(&qkey, &i.to_string()).await?.ok_or_else(|| {
            NasaRedisError::ProtocolMarker(format!(
                "quarantine 损坏:分区 {p} park_id={park_id} 缺第 {i} 条 payload,force 拒绝(fail-closed)"
            ))
        })?;
        payloads.push((i, payload));
    }

    let stream = layout.stream(p);
    // 预检后的批次和两份计划仍须与认领时一致，不能把旧的重复风险授权套用到新记录。
    if st == Some("ResumeIndeterminate") {
        const CLAIM_FORCE_LUA: &str = r#"
if redis.call('HGET', KEYS[1], 'planned') ~= ARGV[10]
    or redis.call('HGET', KEYS[1], 'uncertain') ~= ARGV[11] then
    return {'CONFLICT', 'FORCE_PLAN'}
end
redis.call('HINCRBY', KEYS[1], 'version', 1)
redis.call('HSET', KEYS[1], 'state', 'ForcePublishing', 'owner_holder', ARGV[1], 'transition_operation_id', ARGV[8])
-- 返回持久整数原文，避免大计数已认领后才因科学计数法无法解析而中断。
return {'OK', redis.call('HGET', KEYS[1], 'version')}
"#;
        let _: u64 = redis::from_redis_value(
            guarded_disposition(
                client,
                layout,
                p,
                lease,
                &park_id,
                "ResumeIndeterminate",
                &stream,
                CLAIM_FORCE_LUA,
                (&planned_json, &uncertain_json),
            )
            .await?,
        )?;
    }

    // 每条记录先持久化目标再 XADD；重入优先复用已确认或尚可补发的同一目标。
    // 目标被越过且无法确认时才另行分配；旧记录可能已被消费删除，因此显式重发仍承担重复风险。
    let mut final_ids = planned.clone();
    let mut audit: Vec<(usize, String, String)> = Vec::new(); // (index, 旧 planned, 新 target)
    let mut expected_targets: Vec<(String, Option<String>)> = Vec::new();
    // 同一 operation 的并发续作也可能更换目标，必须对旧字段做 CAS，禁止晚到的规划覆盖新目标。
    const FORCE_PLAN_LUA: &str = r#"
if redis.call('HGET', KEYS[1], 'planned') ~= ARGV[10]
    or redis.call('HGET', KEYS[1], 'uncertain') ~= ARGV[11] then
    return {'CONFLICT', 'FORCE_PLAN'}
end
local current = redis.call('HGET', KEYS[1], ARGV[12])
if (ARGV[13] == '0' and current) or (ARGV[13] == '1' and current ~= ARGV[14]) then
    return {'CONFLICT', 'FORCE_TARGET'}
end
redis.call('HSET', KEYS[1], ARGV[12], ARGV[15])
return {'OK', 1}
"#;
    for (i, payload) in &payloads {
        let i = *i;
        let item_field = format!("force:{i}");
        // 重入:本 index 已提交过 target → 只对账/补发同一 target
        if let Some(target) = client.h_get::<String>(&marker, &item_field).await? {
            match reconcile_one(client, &stream, &target, payload).await {
                Ok(true) => {
                    expected_targets.push((item_field, Some(target.clone())));
                    final_ids[i] = target;
                    continue;
                }
                Err(NasaRedisError::Config(sig)) if sig == "RETRYABLE" => {
                    // 同 target 幂等补发(显式 ID;崩在首发前的续作)
                    xadd_force_target(
                        client,
                        layout,
                        p,
                        lease,
                        &park_id,
                        &planned_json,
                        &uncertain_json,
                        &stream,
                        &target,
                        payload,
                        i,
                    )
                    .await?;
                    expected_targets.push((item_field, Some(target.clone())));
                    final_ids[i] = target;
                    continue;
                }
                Ok(false) => {
                    // 旧目标无法再次写入且发布结果不确定；在显式重发授权下保存新目标后再发送。
                    // 这里无法区分从未发布与发布后被删除，不能据此排除重复；中断后仍从已存目标继续对账。
                    let base = last_generated(client, &stream).await?;
                    let fresh = plan_ids(base, 1)?.remove(0);
                    guarded_disposition(
                        client,
                        layout,
                        p,
                        lease,
                        &park_id,
                        "ForcePublishing",
                        &stream,
                        FORCE_PLAN_LUA,
                        (
                            &planned_json,
                            &uncertain_json,
                            &item_field,
                            "1",
                            &target,
                            &fresh,
                        ),
                    )
                    .await?;
                    xadd_force_target(
                        client,
                        layout,
                        p,
                        lease,
                        &park_id,
                        &planned_json,
                        &uncertain_json,
                        &stream,
                        &fresh,
                        payload,
                        i,
                    )
                    .await?;
                    audit.push((i, target.clone(), fresh.clone()));
                    expected_targets.push((item_field, Some(fresh.clone())));
                    final_ids[i] = fresh;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        // 首次:决定 target —— 原 planned 已发布则复用;RETRYABLE 安全按原 planned 首发;
        // 真不确定(Ok(false))才分配新 ID(force 显式接受一次重复)
        let target = match reconcile_one(client, &stream, &planned[i], payload).await {
            Ok(true) => {
                expected_targets.push((item_field, None));
                final_ids[i] = planned[i].clone();
                continue; // 原 planned 已发布且一致:无需提交 force item
            }
            Err(NasaRedisError::Config(sig)) if sig == "RETRYABLE" => planned[i].clone(),
            Ok(false) => {
                let base = last_generated(client, &stream).await?;
                plan_ids(base, 1)?.remove(0)
            }
            Err(e) => return Err(e), // 网络/ACL/WRONGTYPE/解析:上抛
        };
        // 缺字段也要作为 CAS 依据，目标先持久化再发布，失败后重入只能对账已保存目标。
        guarded_disposition(
            client,
            layout,
            p,
            lease,
            &park_id,
            "ForcePublishing",
            &stream,
            FORCE_PLAN_LUA,
            (
                &planned_json,
                &uncertain_json,
                &item_field,
                "0",
                "",
                &target,
            ),
        )
        .await?;
        xadd_force_target(
            client,
            layout,
            p,
            lease,
            &park_id,
            &planned_json,
            &uncertain_json,
            &stream,
            &target,
            payload,
            i,
        )
        .await?;
        if target != planned[i] {
            audit.push((i, planned[i].clone(), target.clone()));
        }
        expected_targets.push((item_field, Some(target.clone())));
        final_ids[i] = target;
    }

    // 每个完成证据必须仍指向当前目标；全部读取与比较先于终态写入和恢复数据删除。
    const FINISH_FORCE_LUA: &str = r#"
if redis.call('HGET', KEYS[1], 'planned') ~= ARGV[10]
    or redis.call('HGET', KEYS[1], 'uncertain') ~= ARGV[11] then
    return {'CONFLICT', 'FORCE_PLAN'}
end
local targets = cjson.decode(ARGV[12])
for _, target in ipairs(targets) do
    local current = redis.call('HGET', KEYS[1], target[1])
    if (target[2] == cjson.null and current)
        or (target[2] ~= cjson.null and current ~= target[2]) then
        return {'CONFLICT', 'FORCE_TARGET'}
    end
end
redis.call('HSET', KEYS[1], 'forced', '1', 'force_audit', ARGV[13], 'new_ids', ARGV[14], 'state', 'Resumed')
redis.call('DEL', KEYS[4], KEYS[5])
redis.call('EXPIRE', KEYS[1], 86400)
return {'OK', 1}
"#;
    guarded_disposition(
        client,
        layout,
        p,
        lease,
        &park_id,
        "ForcePublishing",
        &stream,
        FINISH_FORCE_LUA,
        (
            &planned_json,
            &uncertain_json,
            serde_json::to_string(&expected_targets).unwrap(),
            serde_json::to_string(&audit).unwrap(),
            final_ids.join(","),
        ),
    )
    .await?;
    tracing::warn!(p, ?audit, "force_republish 完成(可能重复/变序,审计已记)");
    Ok(final_ids)
}

/// 业务作用：向已持久化的强制重发目标写入原载荷，失败时对账同一目标而不在本步骤重新预约。
///
/// 参数说明：
/// - `client`: 发布和对账的 Redis 客户端。
/// - `layout`: 当前处置分区组的同槽键布局。
/// - `p`: 恢复批次所属物理分区。
/// - `lease`: 每次 XADD 必须复验的持有权与操作身份。
/// - `park_id`: 已捕获的恢复批次。
/// - `planned_json`: 读取时的完整预约记录，防止目标来自另一份计划。
/// - `uncertain_json`: 本次显式重发所覆盖的索引记录，发布时必须仍保持一致。
/// - `stream`: 显式 ID 写入的目标 Redis Stream key。
/// - `target`: 已提交的目标 stream entry id。
/// - `payload`: 待写入 entry 的 parked payload。
/// - `i`: 本条记录在恢复批次中的序号，用于定位必须仍匹配的 force 目标字段。
///
/// 返回：发布确认或对账一致时成功；传输未知返回 ExecutionUnknown，无法确认时返回 PublishIndeterminate。
/// 后续同操作重入是否另行分配由上层显式重发流程决定，本函数不改变持久目标。
#[allow(clippy::too_many_arguments)]
async fn xadd_force_target(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    lease: &OwnerLease<'_>,
    park_id: &str,
    planned_json: &str,
    uncertain_json: &str,
    stream: &str,
    target: &str,
    payload: &[u8],
    i: usize,
) -> Result<()> {
    // 目标曾持久化不等于现在仍归本操作所有；XADD 与目标、批次和当前权威检查同脚本执行。
    const FORCE_XADD_LUA: &str = r#"
if redis.call('HGET', KEYS[1], 'planned') ~= ARGV[10]
    or redis.call('HGET', KEYS[1], 'uncertain') ~= ARGV[11]
    or redis.call('HGET', KEYS[1], ARGV[12]) ~= ARGV[13] then
    return {'CONFLICT', 'FORCE_TARGET'}
end
return {'OK', redis.call('XADD', KEYS[6], ARGV[13], ARGV[14], ARGV[15])}
"#;
    let r = guarded_disposition(
        client,
        layout,
        p,
        lease,
        park_id,
        "ForcePublishing",
        stream,
        FORCE_XADD_LUA,
        (
            planned_json,
            uncertain_json,
            format!("force:{i}"),
            target,
            super::DATA_FIELD,
            payload,
        ),
    )
    .await;
    match r {
        Ok(value) => {
            let id: String = redis::from_redis_value(value)?;
            if parse_sid(&id) != parse_sid(target) {
                return Err(NasaRedisError::ProtocolMarker(
                    "强制发布响应与目标 ID 不一致".into(),
                ));
            }
            Ok(())
        }
        Err(NasaRedisError::Redis(e)) if e.is_io_error() || e.is_timeout() || e.is_connection_dropped() => {
            Err(NasaRedisError::ExecutionUnknown(format!(
                "force XADD target={target}(index {i})写出后断线: {e}"
            )))
        }
        Err(NasaRedisError::Redis(_)) => match reconcile_one(client, stream, target, payload).await {
            Ok(true) => Ok(()), // 被越过但已确认发布
            Ok(false) => Err(NasaRedisError::PublishIndeterminate(format!(
                "force index {i}: target={target} 被越过且无法发布(真不确定)——marker 停 ForcePublishing,需 operator 介入(force_takeover / 人工对账)"
            ))),
            Err(e) => Err(e),
        },
        // 明确权威拒绝不能转为对账成功，避免旧执行继续推进终态。
        Err(e) => Err(e),
    }
}

/// 业务作用：按显式丢弃意图删除 Park 恢复数据，并提交 Dropped 终态供后续查询和清理重入。
///
/// 参数说明：
/// - `client`: 执行 drop 状态机的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `lease`: 当前 owner 的锁和 fence 凭据。
///
/// 返回：同批次删除与终态保留均确认时成功；失权、批次或操作冲突、Redis 错误时失败。
/// 认领和清理分别原子复验权威；中断后按 Dropping 意图或 Dropped 终态重入，通信失败不证明未执行。
pub async fn drop_parked(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    lease: &OwnerLease<'_>,
) -> Result<()> {
    // 锁证据只能授权所属分区；内部坐标不一致时不得读取或改变另一分区的处置记录。
    if lease.partition != p {
        return Err(NasaRedisError::OwnershipChanged(format!(
            "分区 {p} 与持有权证据分区 {} 不一致",
            lease.partition
        )));
    }
    let marker = marker_key(layout, p);
    let park_id: Option<String> = client.h_get(&marker, "park_id").await?;
    let Some(park_id) = park_id.filter(|id| !id.is_empty()) else {
        return Err(NasaRedisError::Config(format!("分区 {p} 无 Park 记录")));
    };
    // 接管已确认的批次不能在网络等待后改为新批次；之后的两阶段脚本继续绑定本次捕获身份。
    if lease
        .expected_park_id
        .is_some_and(|expected| expected != park_id.as_str())
    {
        return Err(NasaRedisError::OperationConflict(format!(
            "分区 {p} 丢弃续作的 Park 批次已变化"
        )));
    }
    // 两阶段均绑定已读取的批次；否则认领等待期间的新 Park 也可能被旧请求改变为 Dropping。
    const DROP_FENCED_LUA: &str = r#"
-- KEYS 为 marker、锁、fence、隔离区、索引；ARGV 为 holder、fence 标记、轮次、nonce、分区、计数、批次、操作和阶段。
-- Lua 出错不回滚，先检查全部键类型和身份，避免删除后才因损坏字段中止。
for i = 1, 5 do
    local kind = redis.call('TYPE', KEYS[i])['ok']
    local expected = 'hash'
    if i == 5 then
        expected = 'string'
    end
    if kind ~= 'none' and kind ~= expected then
        return 'INVALID_TYPE'
    end
end
if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 0 then
    return 'NOT_HOLDER'
end
if ARGV[2] == '1' then
    if redis.call('HGET', KEYS[3], 'state') ~= 'Ready' then
        return 'FENCE_NOT_READY'
    end
    if redis.call('HGET', KEYS[3], 'round') ~= ARGV[3] then
        return 'ROUND_MISMATCH'
    end
    if redis.call('HGET', KEYS[3], 'nonce') ~= ARGV[4] then
        return 'NONCE_MISMATCH'
    end
    if redis.call('HGET', KEYS[3], 'c:' .. ARGV[5]) ~= ARGV[6] then
        return 'STALE_STAMP'
    end
end
-- 当前批次和索引必须与捕获身份一致；缺索引允许终态清理或中断续作，异批次索引绝不能删除。
if redis.call('HGET', KEYS[1], 'park_id') ~= ARGV[7] then
    return 'BATCH_CONFLICT'
end
local index = redis.call('GET', KEYS[5])
if index and index ~= ARGV[7] then
    return 'INDEX_CONFLICT'
end
local state = redis.call('HGET', KEYS[1], 'state')
local operation = redis.call('HGET', KEYS[1], 'transition_operation_id')
if state ~= 'Dropping' and state ~= 'Dropped' and not (state == 'Parked' and ARGV[9] == 'claim') then
    return 'STATE_CONFLICT'
end
-- 旧记录缺操作身份时保留续作兼容；已登记的在途操作必须匹配，终态允许当前 owner 补清残留。
if state == 'Dropping' and operation and operation ~= '' and operation ~= ARGV[8] then
    return 'OPERATION_CONFLICT'
end
if ARGV[9] == 'claim' then
    if state == 'Parked' then
        -- 负计数不能被认领隐式转成合法批次，先拒绝，保留恢复数据与原状态。
        local version = redis.call('HGET', KEYS[1], 'version')
        if version and string.sub(version, 1, 1) == '-' then
            return 'INVALID_VERSION'
        end
        redis.call('HINCRBY', KEYS[1], 'version', 1)
        redis.call('HSET', KEYS[1], 'state', 'Dropping', 'owner_holder', ARGV[1], 'transition_operation_id', ARGV[8])
    end
    return 'OK'
end
if ARGV[9] ~= 'finish' then
    return 'INVALID_PHASE'
end
-- 权威复验与终态、删除、TTL 同脚本执行；先写终态，避免字段写入失败发生在恢复数据删除之后。
redis.call('HSET', KEYS[1], 'state', 'Dropped')
redis.call('DEL', KEYS[4], KEYS[5])
redis.call('EXPIRE', KEYS[1], 86400)
return 'OK'
"#;
    // 先持久化 Dropping 意图，随后再次原子复验；第一次成功回应不能延长 holder 或 fence 的有效期。
    for phase in ["claim", "finish"] {
        let tag: String = redis::Script::new(DROP_FENCED_LUA)
            .key(&marker)
            .key(lease.lock_key)
            .key(lease.lua_fence_key())
            .key(quarantine_key(layout, &park_id))
            .key(parked_index_key(layout, p))
            .arg(lease.holder)
            .arg(lease.has_fence())
            .arg(lease.round.to_string())
            .arg(lease.nonce)
            .arg(p.to_string())
            .arg(lease.counter.to_string())
            .arg(&park_id)
            .arg(lease.operation_id)
            .arg(phase)
            .invoke_async(&mut client.conn())
            .await?;
        match tag.as_str() {
            "OK" => {}
            // 失权后保留当前恢复数据与 marker，不以入口成功或旧认领记录作为删除凭据。
            "NOT_HOLDER" | "FENCE_NOT_READY" | "ROUND_MISMATCH" | "NONCE_MISMATCH"
            | "STALE_STAMP" => {
                return Err(NasaRedisError::OwnershipChanged(format!(
                    "分区 {p} 丢弃被持有权校验拒绝({tag})"
                )));
            }
            // 批次、状态或操作身份改变时交由对应操作续作，避免旧请求清除新的冻结事实。
            "BATCH_CONFLICT" | "INDEX_CONFLICT" | "STATE_CONFLICT" | "OPERATION_CONFLICT" => {
                return Err(NasaRedisError::OperationConflict(format!(
                    "分区 {p} 丢弃与当前处置记录冲突({tag})"
                )));
            }
            _ => {
                return Err(NasaRedisError::ProtocolMarker(format!(
                    "分区 {p} 丢弃记录或脚本响应异常({tag})"
                )));
            }
        }
    }
    tracing::warn!(p, park_id, "Parked 消息已 Drop(审计 marker 保留 24h)");
    Ok(())
}

/// 业务作用：在外部已确认原操作停止后，显式移交在途转换身份并继续对应的发布或丢弃流程。
///
/// 参数说明：
/// - `client`: 执行强制接管状态机的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `lease`: 当前 owner 的锁和 fence 凭据。
///
/// 返回：接续至终态时给出发布 ID，丢弃成功返回空列表；失权、批次或操作变化、不可接管状态直接拒绝。
/// 只允许四种在途状态；Parked、终态及不确定态不由此入口接管。状态比较不证明原操作已退出，
/// 调用方必须先取得外部停止依据；移交后失败仍保留已写入的新操作身份及部分副作用。
pub async fn force_takeover(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    lease: &OwnerLease<'_>,
) -> Result<Vec<String>> {
    // 分区坐标不能借用另一分区的持有权；拒绝必须先于读取和移交在途操作身份。
    if lease.partition != p {
        return Err(NasaRedisError::OwnershipChanged(format!(
            "分区 {p} 与持有权证据分区 {} 不一致",
            lease.partition
        )));
    }
    verify_owner_fenced(client, lease).await?;
    let marker = marker_key(layout, p);
    // 一次读取捕获同批次的状态与操作身份；仅比较状态无法区分替换后的同状态记录。
    let (state, park_id, operation): (Option<String>, Option<String>, Option<String>) =
        redis::cmd("HMGET")
            .arg(&marker)
            .arg(&["state", "park_id", "transition_operation_id"])
            .query_async(&mut client.conn())
            .await?;
    let (Some(state), Some(park_id)) = (state, park_id.filter(|id| !id.is_empty())) else {
        return Err(NasaRedisError::Config(format!(
            "分区 {p} 缺少完整 marker,无在途转换可接管"
        )));
    };
    // 嵌套续作不能把旧批次的接管授权转移到新 marker，即使它也处于可接管状态。
    if lease
        .expected_park_id
        .is_some_and(|expected| expected != park_id.as_str())
    {
        return Err(NasaRedisError::OperationConflict(format!(
            "分区 {p} 接管续作的 Park 批次已变化"
        )));
    }
    if !matches!(
        state.as_str(),
        "ResumePublishing" | "DlqPublishing" | "ForcePublishing" | "Dropping"
    ) {
        return Err(NasaRedisError::Config(format!(
            "分区 {p} state={state} 非可接管的在途 *Publishing/Dropping\
             (Parked 用 resume/drop/dlq;*Indeterminate 需人工对账)"
        )));
    }
    // 移交与当前权威复验必须同脚本执行；快照后的失权或身份变化均不能覆写新操作。
    const TAKEOVER_LUA: &str = r#"
if redis.call('HEXISTS', KEYS[2], ARGV[3]) == 0 then
    return 'NOT_HOLDER'
end
if ARGV[4] == '1' then
    if redis.call('HGET', KEYS[3], 'state') ~= 'Ready' then
        return 'FENCE_NOT_READY'
    end
    if redis.call('HGET', KEYS[3], 'round') ~= ARGV[5] then
        return 'ROUND_MISMATCH'
    end
    if redis.call('HGET', KEYS[3], 'nonce') ~= ARGV[6] then
        return 'NONCE_MISMATCH'
    end
    if redis.call('HGET', KEYS[3], 'c:' .. ARGV[7]) ~= ARGV[8] then
        return 'STALE_STAMP'
    end
end
-- 缺字段与旧空操作身份兼容，但已知身份被替换时必须重新取得接管依据。
if redis.call('HGET', KEYS[1], 'state') ~= ARGV[1]
    or redis.call('HGET', KEYS[1], 'park_id') ~= ARGV[9]
    or (redis.call('HGET', KEYS[1], 'transition_operation_id') or '') ~= ARGV[10] then
    return 'CONFLICT'
end
local index = redis.call('GET', KEYS[4])
if index and index ~= ARGV[9] then
    return 'CONFLICT'
end
-- 所有可能失败的读取在移交之前完成，防止损坏索引导致身份已覆写却无法继续。
redis.call('HSET', KEYS[1], 'transition_operation_id', ARGV[2])
return 'OK'
"#;
    let tag: String = redis::Script::new(TAKEOVER_LUA)
        .key(&marker)
        .key(lease.lock_key)
        .key(lease.lua_fence_key())
        .key(parked_index_key(layout, p))
        .arg(&state)
        .arg(lease.operation_id)
        .arg(lease.holder)
        .arg(lease.has_fence())
        .arg(lease.round.to_string())
        .arg(lease.nonce)
        .arg(p.to_string())
        .arg(lease.counter.to_string())
        .arg(&park_id)
        .arg(operation.as_deref().unwrap_or_default())
        .invoke_async(&mut client.conn())
        .await?;
    match tag.as_str() {
        "OK" => {}
        // 失权时禁止移交；当前持有者负责决定是否重建接管依据。
        "NOT_HOLDER" | "FENCE_NOT_READY" | "ROUND_MISMATCH" | "NONCE_MISMATCH" | "STALE_STAMP" => {
            return Err(NasaRedisError::OwnershipChanged(format!(
                "分区 {p} 接管被持有权校验拒绝({tag})"
            )));
        }
        // 状态相同也可能是不同批次或并发移交，不能沿用旧快照继续推进。
        "CONFLICT" => {
            return Err(NasaRedisError::OperationConflict(format!(
                "分区 {p} 接管期间处置身份或索引已变化"
            )));
        }
        _ => {
            return Err(NasaRedisError::ProtocolMarker(format!(
                "分区 {p} 接管脚本响应异常({tag})"
            )));
        }
    }
    tracing::warn!(p, %state, new_op = lease.operation_id, "通用 liveness-takeover:已覆写 op,续作在途转换");
    // 移交成功只证明这个批次在脚本时刻有效；把同一批次传给续作，防止等待回应后读取并处置新 Park。
    let continuation = OwnerLease {
        partition: lease.partition,
        operation_id: lease.operation_id,
        expected_park_id: Some(&park_id),
        holder: lease.holder,
        lock_key: lease.lock_key,
        fence_key: lease.fence_key,
        round: lease.round,
        nonce: lease.nonce,
        counter: lease.counter,
    };
    match state.as_str() {
        "ResumePublishing" => resume(client, layout, p, &continuation).await,
        "DlqPublishing" => dlq_from_parked(client, layout, p, &continuation).await,
        "ForcePublishing" => force_republish(client, layout, p, &continuation).await,
        "Dropping" => {
            drop_parked(client, layout, p, &continuation).await?;
            Ok(Vec::new())
        }
        _ => unreachable!(),
    }
}
