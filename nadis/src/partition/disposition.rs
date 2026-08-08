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
// marker 状态机包含 ResumeIndeterminate 与 ForceRepublish：
//   (无) → Parked → ResumePublishing → Resumed(终态,带 TTL 保留供对账)
//                  ↘ Dropped(终态)
//
// fenced PARK_LUA:单脚本原子执行——
//   ①校验分区锁 holder(防旧 owner 失锁后迟到 park 冻结新 owner 的分区);
//   ②校验目标 ID 仍在本组 PEL(XPENDING 精确查;不在 = 状态已变,拒绝);
//   ③payload 转存 quarantine(确定性 HASH,源 stream 此后可随意 trim);
//   ④XACK 源消息;⑤写 marker(Parked)+ parked 索引。
//   注意:Lua 运行时错误不回滚(实测)——本脚本写入顺序经过设计:
//   quarantine 先于 XACK(源被 ACK 前 payload 必有副本),重入按 park_id 幂等覆盖。
//
// resume = planned-ID reservations 协议(定稿):
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
//   ⑤全部确认 → 删 quarantine → Resumed{new_ids}(payload 对账用 quarantine 原文逐字节
//     比对——quarantine 删除发生在全部确认之后,对账期原文恒可得,无需 digest)。
// ============================================================================

use std::sync::Arc;

use super::KeyLayout;
use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};

/// 业务作用：disposition marker key（每分区一个，约束“同一分区同时最多一宗 Park”，
/// 与 的 park_id 多宗模型的差异已在头部注明)。
///
/// # 参数
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
pub fn marker_key(layout: &KeyLayout, p: u32) -> String {
    format!("{}:disposition:{}", layout.prefix, p)
}

/// 业务作用：quarantine key(确定性:同 park_id 重入即幂等覆盖校验)。
///
/// # 参数
/// - `layout`: 分区组 key 布局。
/// - `park_id`: 本次 Park 批次的唯一 ID。
pub fn quarantine_key(layout: &KeyLayout, park_id: &str) -> String {
    format!("{}:quarantine:{}", layout.prefix, park_id)
}

/// 业务作用：parked 索引 key(value = park_id;新 owner 接管分区时读它恢复停拉)。
///
/// # 参数
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
pub fn parked_index_key(layout: &KeyLayout, p: u32) -> String {
    format!("{}:parked:{}", layout.prefix, p)
}

/// 管理转换 owner-fenced CAS-claim Lua。
/// KEYS=[marker, lock_key, fence_key];ARGV=[expected, new_state, holder, has_fence,
///        round, nonce, p, counter, operation_id]。先**校验 owner/fence**(holder 持锁 + fence
/// Ready/round/nonce + c:{p}==我的 counter)再 state CAS + HINCRBY version + 记 owner_holder +
/// **transition_operation_id**(:把"是谁认领了这次在途转换"写进 marker,重入据此判同 op)。
const CLAIM_TRANSITION_FENCED_LUA: &str = r#"
if redis.call('hexists', KEYS[2], ARGV[3]) == 0 then return {'NOT_HOLDER', ''} end
if ARGV[4] == '1' then
    if redis.call('HGET', KEYS[3], 'state') ~= 'Ready' then return {'FENCE_NOT_READY', ''} end
    if redis.call('HGET', KEYS[3], 'round') ~= ARGV[5] then return {'ROUND_MISMATCH', ''} end
    if redis.call('HGET', KEYS[3], 'nonce') ~= ARGV[6] then return {'NONCE_MISMATCH', ''} end
    if redis.call('HGET', KEYS[3], 'c:' .. ARGV[7]) ~= ARGV[8] then return {'STALE_STAMP', ''} end
end
local cur = redis.call('HGET', KEYS[1], 'state')
if cur ~= ARGV[1] then return {'CONFLICT', cur or ''} end
local v = redis.call('HINCRBY', KEYS[1], 'version', 1)
redis.call('HSET', KEYS[1], 'state', ARGV[2], 'owner_holder', ARGV[3], 'transition_operation_id', ARGV[9])
return {'OK', tostring(v)}
"#;

/// owner 凭据:**不可缺省**——disposition mutator 必须传它证明调用者是
/// 当前 partition owner,且携带本次管理操作的稳定 `operation_id`(状态机身份)。
/// `fence_key` 为空串 = **原实现V1**(无 V2 fence,仅校验 holder 持锁);非空 = RustV2 带 fence
/// 三元组 + 任期 counter。**由 runtime 私有构造**,外部不可伪造,杜绝 None 绕过。
pub struct OwnerLease<'a> {
    pub partition: u32,
    pub operation_id: &'a str,
    pub holder: &'a str,
    pub lock_key: &'a str,
    pub fence_key: &'a str,
    pub round: u64,
    pub nonce: &'a str,
    pub counter: u64,
}

impl OwnerLease<'_> {
    /// 业务作用：返回本次管理转换是否携带 V2 fence 的序列化标记。
    ///
    /// Lua 脚本用 `"1"`/`"0"` 区分严格 fence 校验和原实现 V1 holder-only 校验。
    fn has_fence(&self) -> &'static str {
        if self.fence_key.is_empty() {
            "0"
        } else {
            "1"
        }
    }
}

/// 业务作用：原子认领管理转换(owner-fenced + 记 operation_id)。Ok(new_version)=认领成功;Err=owner 不符
/// (`OwnershipChanged`,留 PEL 交新 owner)/ 状态冲突(`VersionConflict`)。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `marker`: 分区处置状态 marker key。
/// - `expected`: 协议或状态机期望值。
/// - `new_state`: 分区处置 marker 即将写入的新状态。
/// - `lease`: 当前 owner lease 与 fencing 信息。
async fn claim_transition(
    client: &Arc<RedisClient>,
    marker: &str,
    expected: &str,
    new_state: &str,
    lease: &OwnerLease<'_>,
) -> Result<u64> {
    let v: redis::Value = redis::Script::new(CLAIM_TRANSITION_FENCED_LUA)
        .key(marker)
        .key(lease.lock_key)
        .key(lease.fence_key)
        .arg(expected)
        .arg(new_state)
        .arg(lease.holder)
        .arg(lease.has_fence())
        .arg(lease.round.to_string())
        .arg(lease.nonce)
        .arg(lease.partition.to_string())
        .arg(lease.counter.to_string())
        .arg(lease.operation_id)
        .invoke_async(&mut client.conn())
        .await?;
    let redis::Value::Array(arr) = v else {
        return Err(NasaRedisError::ProtocolMarker(
            "claim_transition 响应异常".into(),
        ));
    };
    let tag = match arr.first() {
        Some(redis::Value::BulkString(b)) => String::from_utf8_lossy(b).into_owned(),
        Some(redis::Value::SimpleString(s)) => s.clone(),
        _ => String::new(),
    };
    if tag == "OK" {
        //CAS 成功的 version **绝不 `unwrap_or(0)`**(损坏被静默归零会反转
        // command 的 marker_version 审计)——形态/解析异常一律 `ProtocolMarker` 上抛(对齐 acquire_stamp)。
        let ver: u64 = match arr.get(1) {
            Some(redis::Value::BulkString(b)) => {
                String::from_utf8_lossy(b).parse().map_err(|_| {
                    NasaRedisError::ProtocolMarker(
                        "claim_transition 返回的 version 不可解析".into(),
                    )
                })?
            }
            other => {
                return Err(NasaRedisError::ProtocolMarker(format!(
                    "claim_transition OK 但缺 version 段: {other:?}"
                )))
            }
        };
        return Ok(ver);
    }
    // owner/fence 校验失败 → `OwnershipChanged`(非业务拒绝,留 PEL 交新 owner)
    if matches!(
        tag.as_str(),
        "NOT_HOLDER" | "FENCE_NOT_READY" | "ROUND_MISMATCH" | "NONCE_MISMATCH" | "STALE_STAMP"
    ) {
        return Err(NasaRedisError::OwnershipChanged(format!(
            "分区 {}:认领转换被 owner-fence 拒绝({tag})",
            lease.partition
        )));
    }
    let cur = match arr.get(1) {
        Some(redis::Value::BulkString(b)) => String::from_utf8_lossy(b).into_owned(),
        _ => String::new(),
    };
    Err(NasaRedisError::Config(format!(
        "管理转换冲突(VersionConflict):期望 state={expected} 实际={cur}(已被并发管理操作转换)"
    )))
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

/// 业务作用：入口 owner-fence 校验:每个 mutator 函数**开头**调用——所有状态分支
/// (含 `*Publishing`/`Dropping`/terminal 重入)进入前都先确认调用者仍是当前 owner。**不可缺省**。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `lease`: 当前 owner lease 与 fencing 信息。
async fn verify_owner_fenced(client: &Arc<RedisClient>, lease: &OwnerLease<'_>) -> Result<()> {
    let tag: String = redis::Script::new(VERIFY_OWNER_LUA)
        .key(lease.lock_key)
        .key(lease.fence_key)
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

/// 业务作用：重入身份校验:marker 记的 `transition_operation_id` 必须 == 本 op,否则 `OperationConflict`
/// ——**op B 不得续作 op A 的在途转换**。无记录(老 marker/首次)→ 放行。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `marker`: 分区处置状态 marker key。
/// - `op_id`: 业务标识,用于定位具体对象或记录。
async fn assert_op_owns(client: &Arc<RedisClient>, marker: &str, op_id: &str) -> Result<()> {
    let owner: Option<String> = client.h_get(marker, "transition_operation_id").await?;
    match owner.as_deref() {
        None | Some("") => Ok(()),
        Some(o) if o == op_id => Ok(()),
        //用专属 `OperationConflict` 变体(非 Config)——command 路径据此**保留 PEL**
        // (不写 Rejected 终态),原 op 重提续作,避免"原 op 命令丢失 → 永久冻结"。
        Some(o) => Err(NasaRedisError::OperationConflict(format!(
            "在途转换属 operation={o},本 operation={op_id} 不得接管(交原 op 或新 op 显式重授权)"
        ))),
    }
}

/// 业务作用：**auto-poison 处置的 operation_id**，用于 liveness-takeover 与 auto-DLQ 崩溃恢复。
/// auto-DLQ 由 owner **内部**触发(非 producer/admin 命令),无外部重提主体——若 publish 中途崩溃,
/// 旧实现用一次性 `park_id` 作 op,接管方无从续作 → 分区永久冻结。
/// 改用**可识别 + 稳定**的 `auto:{p}:{park_id}`:① `auto:` 前缀让接管方识别"这是 auto 处置、可自动续作"
/// (区别于 producer 的 `op-{uuid}` / direct 的 `direct:{...}` —— 那些要等原 op 重提,不自动续);
/// ② 稳定可由 `(p, park_id)` 重导出 / 直接从 marker 读回,重入 `assert_op_owns` 必同 op。
///
/// # 参数
/// - `p`: 分区编号。
/// - `park_id`: Park 批次 ID。
pub fn auto_op_id(p: u32, park_id: &str) -> String {
    format!("auto:{p}:{park_id}")
}

/// 业务作用：是否 auto-poison 处置 op(接管方据此判定可否自动续作在途 `*Publishing`)。
///
/// # 参数
/// - `op_id`: 要判断的 operation ID。
pub fn is_auto_op(op_id: &str) -> bool {
    op_id.starts_with("auto:")
}

/// 业务作用：读 marker 当前 version(诊断/审计;无 marker 或无 version 字段 → 0)。
///
/// # 参数
/// - `client`: 读取 marker 的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
pub async fn marker_version(client: &Arc<RedisClient>, layout: &KeyLayout, p: u32) -> Result<u64> {
    Ok(client
        .h_get::<u64>(&marker_key(layout, p), "version")
        .await?
        .unwrap_or(0))
}

/// 业务作用：读 quarantine 批次的全部 payload(**fail-closed**; 修正)。
/// quarantine 字段缺失 = 持久状态损坏,**绝不用空字节替代**(那会把原业务数据
/// 静默替换成空,resume/dlq 还报告成功 = 确定性数据损坏)——一律返回携带
/// `partition/park_id/index` 的协议损坏错误,由人工对账。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `qkey`: 业务 key 或 Redis key,用于定位数据。
/// - `p`: 分区编号或当前协议步骤中的短名参数。
/// - `park_id`: 业务标识,用于定位具体对象或记录。
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
    let mut payloads = Vec::with_capacity(n as usize);
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

/// 业务作用：重入场景按已持久化的 planned 长度读 payload(同样 fail-closed)。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `qkey`: 业务 key 或 Redis key,用于定位数据。
/// - `p`: 分区编号或当前协议步骤中的短名参数。
/// - `park_id`: 业务标识,用于定位具体对象或记录。
/// - `expected`: 协议或状态机期望值。
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
///Redis Lua 运行时错误**不回滚**已执行写入。原脚本先写
/// quarantine+XACK 源、最后才写 marker/index——若末步因 key 类型不符(WRONGTYPE)
/// 等运行时错误失败,则源 PEL 已清、quarantine 已写,但 marker/index 缺失 = 半提交,
/// 消息既不在 Parked 也无法被普通 PEL recovery 找回。
/// 解法:**任何写操作之前完成全部可检查预检**(五 key 的 TYPE + marker 状态 + PEL),
/// 预检通过后的写入序不再可能触发 WRONGTYPE。
const PARK_LUA: &str = r#"
-- KEYS[1]=分区锁完整key KEYS[2]=源stream KEYS[3]=quarantine KEYS[4]=marker KEYS[5]=parked索引
-- ARGV[1]=holder ARGV[2]=group ARGV[3]=park_id ARGV[4]=墙钟ms ARGV[5]=条数 n
-- ARGV[6..6+2n-1] = id1,payload1,id2,payload2,...

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

-- ② holder 校验(类型已确认是 hash,hexists 安全)
if redis.call('hexists', KEYS[1], ARGV[1]) == 0 then
    return 'NOT_HOLDER'
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

-- ④ 目标 ID 仍在本组 PEL(读;不在 = 状态已变,拒绝)
local n = tonumber(ARGV[5])
for i = 0, n - 1 do
    local id = ARGV[6 + i * 2]
    local pend = redis.call('XPENDING', KEYS[2], ARGV[2], id, id, 1)
    if #pend == 0 then
        return 'NOT_IN_PEL:' .. id
    end
end

-- ⑤ 转存(先于 ACK:源被确认前 payload 必有副本;同 key 重入 = 幂等覆盖)
for i = 0, n - 1 do
    redis.call('HSET', KEYS[3], tostring(i), ARGV[7 + i * 2])
    redis.call('HSET', KEYS[3], 'id:' .. tostring(i), ARGV[6 + i * 2])
end
redis.call('HSET', KEYS[3], 'n', ARGV[5])
-- ⑥ ACK 源(消息出 PEL;源 stream 此后可被 trim,payload 已安全)
for i = 0, n - 1 do
    redis.call('XACK', KEYS[2], ARGV[2], ARGV[6 + i * 2])
end
-- ⑦ marker + 索引。复用旧**终态** marker(Resumed/Dropped/Dlqed)前必须
--    **DEL 整个 marker**,否则新 Park 会继承旧 planned/uncertain/new_ids/forced/force_audit/
--    owner_holder 字段(污染本轮 resume/force),且 HSET 不清 TTL → 新 Parked marker 会继承上一轮
--    终态的剩余 TTL 在仍 Parked 时自动消失,留下 index-only 冻结。DEL 一次性清字段+清 TTL。
--    跨轮审计由 per-park_id marker 隔离；重建时必须消除字段污染与 TTL 继承。
-- version 跨 Park 轮**单调递增**——DEL 前先读旧 version,新 marker 用 old+1
-- (而非恒置 1)。否则慢命令读 before=旧高值后阻塞,被 DEL 重置成 1 的 marker 让其 after<before,
-- 破坏 command 审计的"after≥before"不变量(把正常执行误标可疑重放/降级)。审计是信任根(规范1)。
local next_ver = 1
if mstate == 'Resumed' or mstate == 'Dropped' or mstate == 'Dlqed' then
    local old_ver = tonumber(redis.call('HGET', KEYS[4], 'version') or '0') or 0
    next_ver = old_ver + 1
    redis.call('DEL', KEYS[4])
end
redis.call('HSET', KEYS[4], 'state', 'Parked', 'park_id', ARGV[3], 'n', ARGV[5], 'parked_at', ARGV[4], 'version', next_ver)
redis.call('SET', KEYS[5], ARGV[3])
return 1
"#;

/// 业务作用：Park 一批毒消息(coordinator 在重投超限 + policy=Park 时调用)。
/// `records` = (源 entry ID, envelope payload);成功返回 park_id。
///
/// # 参数
/// - `client`: 执行 fenced park Lua 的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `lock_full_key`: DistributedLock 完整 Redis 锁 key。
/// - `holder`: 当前 owner 的锁 holder token。
/// - `records`: 要转存到 quarantine 的源 entry ID 与 envelope payload。
pub async fn park(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    lock_full_key: &str,
    holder: &str,
    records: &[(String, Vec<u8>)],
) -> Result<String> {
    //(latent):空 records 守门(规范 20)——n=0 时 PARK_LUA 各循环全跳过,只写
    // quarantine n=0 + marker Parked + index → 分区冻结但无 payload,resume 读 0 条直接 Resumed
    // 报"成功",上游误传的毒消息既没 ACK 也没转存 → 静默丢失。fail-closed,绝不 park 空批。
    if records.is_empty() {
        return Err(NasaRedisError::Config(format!(
            "分区 {p} park 被拒:records 为空(不允许 park 空批,防分区冻结 + 毒消息静默丢失)"
        )));
    }
    let park_id = uuid::Uuid::new_v4().simple().to_string();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("墙钟早于 epoch")
        .as_millis() as u64;

    // Script 必须先绑定到具名变量(prepare_invoke 借用它;临时值会在语句末被释放)
    let lua = redis::Script::new(PARK_LUA);
    let mut script = lua.prepare_invoke();
    script
        .key(lock_full_key)
        .key(layout.stream(p))
        .key(quarantine_key(layout, &park_id))
        .key(marker_key(layout, p))
        .key(parked_index_key(layout, p))
        .arg(holder)
        .arg(layout.group())
        .arg(&park_id)
        .arg(now_ms)
        .arg(records.len());
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

/// 业务作用：读取分区当前 Park 信息(管理/诊断)。复用 `takeover_disposition` 归约(marker 权威),
/// 三态映射(损坏状态不能误报 None,必须 Err)
///   · NotParked   → Ok(None)
///   · Frozen      → Ok(Some(park_id))
///   · Inconsistent(index-only / 非终态缺 park_id / 未知 state)→ **Err(ProtocolMarker)**
///     (接管会 fail-closed 拒绝该分区,API 也必须如实暴露不一致,不能显示"未 Park")。
///
/// # 参数
/// - `client`: 读取 marker/index 的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
pub async fn parked_id(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
) -> Result<Option<String>> {
    match takeover_disposition(client, layout, p).await? {
        TakeoverDisposition::NotParked => Ok(None),
        TakeoverDisposition::Frozen { park_id }
        | TakeoverDisposition::AutoResumeDlq { park_id, .. } => Ok(Some(park_id)),
        TakeoverDisposition::Inconsistent { reason } => Err(NasaRedisError::ProtocolMarker(
            format!("分区 {p} disposition 状态不一致/损坏: {reason}"),
        )),
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
    Frozen { park_id: String },
    /// **auto-DLQ 在途**（marker=`DlqPublishing` 且 op 为 `auto:`）：无外部重提主体，接管方
    /// **自动续作** dlq_from_parked 到终态(成功→恢复消费,基础设施错误→暂冻结下轮再续),
    /// 不像普通 `*Publishing` 那样永久等 producer/admin 重提。
    AutoResumeDlq {
        park_id: String,
        operation_id: String,
    },
    /// marker/index 不一致或损坏 → fail-closed,放弃接管(由调用方释锁退避 + 告警)。
    Inconsistent { reason: String },
}

/// marker 的非终态(冻结业务消费)与终态集合。
const DISPOSITION_NON_TERMINAL: &[&str] = &[
    "Parked",
    // 删除 "Parking"：PARK_LUA 会直接写入 Parked，因此该值不属于可达状态集合。
    // 若存量数据中意外出现 "Parking"，takeover 会进入
    // Inconsistent→fail-closed 弃管+告警,反而比静默当 Frozen 更安全。
    "ResumePublishing",
    "DlqPublishing",
    // **ForcePublishing 必须在此**：force_republish 把
    // ResumeIndeterminate→ForcePublishing 后崩溃/失锁,接管 `takeover_disposition` 读到它若不在
    // 本白名单 → 落 `Some(other)→Inconsistent` → coordinator fail-closed 放弃接管 → 分区**永久冻结**。
    // 加入后崩溃可被接管为 Frozen,再经 command/control 续作(同 resume/dlq 的 *Publishing)。
    "ForcePublishing",
    "Dropping",
    "ResumeIndeterminate",
    "DlqIndeterminate",
];
const DISPOSITION_TERMINAL: &[&str] = &["Resumed", "Dlqed", "Dropped"];

/// 业务作用：接管时归约 disposition marker + parked index(marker 权威,index 仅校验)。
/// 任一读失败上抛(调用方 fail-closed 放弃接管)。
///
/// # 参数
/// - `client`: 读取 marker/index 的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
pub async fn takeover_disposition(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
) -> Result<TakeoverDisposition> {
    let mkey = marker_key(layout, p);
    let mstate: Option<String> = client.h_get(&mkey, "state").await?;
    let mpark: Option<String> = client.h_get(&mkey, "park_id").await?;
    let mop: Option<String> = client.h_get(&mkey, "transition_operation_id").await?;
    // index 读失败(含 WRONGTYPE)会上抛 → 调用方 fail-closed
    let index: Option<String> = client.get(&parked_index_key(layout, p)).await?;

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

/// 业务作用：解析 stream ID "ms-seq"。
///
/// # 参数
/// - `s`: 要解析的输入字符串。
fn parse_sid(s: &str) -> Option<(u64, u64)> {
    let (ms, seq) = s.split_once('-')?;
    Some((ms.parse().ok()?, seq.parse().ok()?))
}

/// 业务作用：从 base(last-generated-id)起预约 n 个严格递增 ID( 算法封口:
/// 按 (ms, seq) 元组递增;seq 溢出进位 ms+1、seq=0;u64::MAX-u64::MAX → 报错不 wrapping)。
///
/// # 参数
/// - `base`: 归档、配置或路径拼接使用的基础名称。
/// - `n`: 长度、数量或循环次数。
fn plan_ids(base: (u64, u64), n: usize) -> Result<Vec<String>> {
    let (mut ms, mut seq) = base;
    let mut out = Vec::with_capacity(n);
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

/// 业务作用：XINFO 等响应的 key/value 兼容取文本:SimpleString(`+`)或 BulkString(`$`)都接受。
/// 各命令的 RESP3 Map key 类型并不统一（FT.*=SimpleString、XINFO/CONFIG=BulkString）。
/// 硬匹配单一形态会在 Redis 版本变更 key 类型时静默漏字段，因此必须 fail-closed。
/// 统一兼容(对齐 actuator::val_str)消除"必须记住每个命令 key 类型"的 footgun。
///
/// # 参数
/// - `v`: 待转换的值。
fn xinfo_str(v: &redis::Value) -> Option<String> {
    match v {
        redis::Value::SimpleString(s) => Some(s.clone()),
        redis::Value::BulkString(b) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

/// 业务作用：从 XINFO 响应抽取指定字段值。**RESP3 Map / RESP2 Array 双形态**归一成 (k,val) 对后线扫,
/// key/value 一律走兼容 `xinfo_str`(不硬匹配 BulkString)。非 Map/Array → None(形态校验由调用方做)。
///
/// # 参数
/// - `v`: 待转换的值。
/// - `field`: Hash 字段名或业务字段名,用于定位 key 内的子项。
fn xinfo_field(v: redis::Value, field: &str) -> Option<String> {
    let pairs: Vec<(redis::Value, redis::Value)> = match v {
        redis::Value::Map(pairs) => pairs,
        redis::Value::Array(items) => {
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
    for (k, val) in pairs {
        if xinfo_str(&k).as_deref() == Some(field) {
            found = xinfo_str(&val);
        }
    }
    found
}

/// 业务作用：读源 stream 的 last-generated-id(stream 不存在 → 0-0 基准)。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `stream`: 作为分区发布源的 Redis Stream key。
async fn last_generated(client: &Arc<RedisClient>, stream: &str) -> Result<(u64, u64)> {
    //只有 Redis 明确的 "no such key"(stream 不存在)才从 0-0 起;
    // 其他错误(网络/WRONGTYPE/ACL/协议)必须**上抛**,绝不静默降级成合法规划 0-1
    // (否则把基础设施错误改写成协议状态,planned 持久化后进入冲突/Indeterminate)。
    let v: redis::Value = match redis::cmd("XINFO")
        .arg("STREAM")
        .arg(stream)
        .query_async(&mut client.conn())
        .await
    {
        Ok(v) => v,
        Err(e) if e.to_string().contains("no such key") => return Ok((0, 0)),
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
            "XINFO STREAM {stream} 缺 last-generated-id 字段(协议损坏)"
        ))),
    }
}

/// 业务作用：单条 reservation 的对账(重入判定表;返回 Ok(true)=已确认发布,Ok(false)=Indeterminate)。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `stream`: 需要按计划 entry id 复查的 Redis Stream key。
/// - `planned`: 预先计算的 stream entry id 列表。
/// - `expect_payload`: 重发校验时期望的 parked payload。
async fn reconcile_one(
    client: &Arc<RedisClient>,
    stream: &str,
    planned: &str,
    expect_payload: &[u8],
) -> Result<bool> {
    let rows: redis::Value = redis::cmd("XRANGE")
        .arg(stream)
        .arg(planned)
        .arg(planned)
        .query_async(&mut client.conn())
        .await?;
    if let redis::Value::Array(rows) = rows {
        if let Some(redis::Value::Array(pair)) = rows.into_iter().next() {
            // entry 存在:取 data field 与 quarantine 原文逐字节比对
            if let Some(redis::Value::Array(fields)) = pair.into_iter().nth(1) {
                let mut it = fields.into_iter();
                while let (Some(f), Some(val)) = (it.next(), it.next()) {
                    if matches!(&f, redis::Value::BulkString(b) if b == b"data") {
                        if let redis::Value::BulkString(got) = val {
                            return Ok(got == expect_payload); // 一致=已发布;不一致=冲突→Indeterminate
                        }
                    }
                }
            }
            return Ok(false); // 结构异常:保守 Indeterminate
        }
    }
    // entry 不存在:last-gen < planned → 可补发(调用方处理);≥ → Indeterminate
    let lg = last_generated(client, stream).await?;
    //planned ID **解析失败 = 协议损坏**,fail-closed 上抛——绝不
    // `unwrap_or((MAX,MAX))` 把损坏 ID 当"已被越过"掩盖成可恢复 Indeterminate。
    let pl = parse_sid(planned).ok_or_else(|| {
        NasaRedisError::ProtocolMarker(format!("reconcile:planned ID 不可解析: {planned:?}"))
    })?;
    if lg < pl {
        return Err(NasaRedisError::Config("RETRYABLE".into())); // 特殊信号:可补发
    }
    Ok(false)
}

/// 业务作用：Resume:planned-ID reservations 协议(全流程见文件头;幂等可重入)。
/// Ok(new_ids) = 已到 Resumed;Err(PublishIndeterminate) = 进入不确定态(分区保持 Parked,
/// 仅 force_republish 可推进);Err(OperationInDoubt) = 已在不确定态被普通 resume 调用。
///
/// # 参数
/// - `client`: 执行 resume 状态机的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `lease`: 当前 owner 的锁和 fence 凭据。
pub async fn resume(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    lease: &OwnerLease<'_>,
) -> Result<Vec<String>> {
    // 入口 owner-fence:所有分支(含 ResumePublishing 重入)进入前先校验 owner
    verify_owner_fenced(client, lease).await?;
    let marker = marker_key(layout, p);
    let state: Option<String> = client.h_get(&marker, "state").await?;
    let park_id: Option<String> = client.h_get(&marker, "park_id").await?;
    let (Some(state), Some(park_id)) = (state, park_id) else {
        return Err(NasaRedisError::Config(format!("分区 {p} 无 Park 记录")));
    };
    let stream = layout.stream(p);
    let qkey = quarantine_key(layout, &park_id);

    match state.as_str() {
        "Parked" => {
            // **先 CAS 认领** Parked → ResumePublishing（并发管理操作只允许一个成功；
            // 冲突 → 本 op 放弃,不重复副作用),认领成功后才规划发布。
            claim_transition(client, &marker, "Parked", "ResumePublishing", lease).await?;
            publish_step(
                client, layout, p, &marker, &qkey, &park_id, &stream,
                "Resumed", "ResumeIndeterminate",
            )
            .await
        }
        // 重入(认领后/发布中 crash):state 已 ResumePublishing,续作(planned 缺失则补规划)。
        //:必须**同一 operation** 才能续作(否则 op B 接管 op A 在途转换 → OperationConflict)。
        "ResumePublishing" => {
            assert_op_owns(client, &marker, lease.operation_id).await?;
            publish_step(
                client, layout, p, &marker, &qkey, &park_id, &stream,
                "Resumed", "ResumeIndeterminate",
            )
            .await
        }
        "ResumeIndeterminate" => Err(NasaRedisError::OperationInDoubt(format!(
            "分区 {p} 处于 ResumeIndeterminate:普通 resume 被拒,需 force_republish(接受重复风险)或人工对账"
        ))),
        "Resumed" => {
            // 幂等:已完成。若上次在"切终态后、删 index/quarantine 前"crash,
            // index/quarantine 会残留 → 终态重入**幂等补删**(否则 marker TTL 过期后 index-only 冻结)。
            //终态重入还须**幂等补 EXPIRE**(否则上次崩在 EXPIRE 前 → marker 永驻)。
            client.del(&[&parked_index_key(layout, p)]).await?;
            client.del(&[&qkey]).await?;
            client
                .expire(&marker, std::time::Duration::from_secs(24 * 3600))
                .await?;
            let ids: Option<String> = client.h_get(&marker, "new_ids").await?;
            Ok(ids.unwrap_or_default().split(',').filter(|s| !s.is_empty()).map(String::from).collect())
        }
        other => Err(NasaRedisError::Config(format!("分区 {p} marker 状态 {other},拒绝 resume"))),
    }
}

/// 业务作用：发布步骤(resume/dlq 共用):marker 已认领进 *Publishing 态后调用。读 planned(缺失则补
/// 规划——认领后崩溃在写 planned 前的兜底),读 payload(fail-closed),逐条发布对账。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `layout`: 分区 key 布局,用于生成 stream、marker 和 lock key。
/// - `p`: 分区编号或当前协议步骤中的短名参数。
/// - `marker`: 分区处置状态 marker key。
/// - `qkey`: 业务 key 或 Redis key,用于定位数据。
/// - `park_id`: 业务标识,用于定位具体对象或记录。
/// - `target_stream`: 处置完成后要写入的目标 stream。
/// - `final_state`: 处置成功后 marker 应进入的最终状态。
/// - `indeterminate_state`: 结果不确定时 marker 应进入的保护状态。
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
) -> Result<Vec<String>> {
    let planned_str: Option<String> = client.h_get(marker, "planned").await?;
    let planned: Vec<String> = match planned_str.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => serde_json::from_str(s)
            .map_err(|e| NasaRedisError::Codec(format!("planned 解析失败: {e}")))?,
        None => {
            // 认领成功但崩溃在写 planned 之前:按当前 target last-gen 重新规划。
            //`*Publishing` 重入分支无 CAS,两 op 可能都走到这里——planned 必须用
            // **HSETNX** 独占首写;失败者**读回赢家的 planned**(禁止覆盖),保证两 op 用同一组
            // planned-ID → publish_reserved 的 reconcile 去重,**不产生重复重投**。
            let payloads = load_quarantine_payloads(client, qkey, p, park_id).await?;
            let planned = plan_ids(last_generated(client, target_stream).await?, payloads.len())?;
            let planned_json = serde_json::to_string(&planned).unwrap();
            let won: i64 = redis::cmd("HSETNX")
                .arg(marker)
                .arg("planned")
                .arg(&planned_json)
                .query_async(&mut client.conn())
                .await?;
            if won == 1 {
                planned
            } else {
                // 并发赢家已写 planned:读回它的那一份(两 op 从此对齐同一组目标 ID)
                let existing: String = client.h_get(marker, "planned").await?.unwrap_or_default();
                serde_json::from_str(&existing)
                    .map_err(|e| NasaRedisError::Codec(format!("planned 读回解析失败: {e}")))?
            }
        }
    };
    let payloads = load_quarantine_payloads_for(client, qkey, p, park_id, planned.len()).await?;
    publish_reserved(
        client,
        layout,
        p,
        marker,
        qkey,
        &planned,
        &payloads,
        target_stream,
        final_state,
        indeterminate_state,
    )
    .await
}

/// 业务作用：按 reservations 逐条发布 + 对账;全部确认才收尾到 Resumed。
/// (参数多是 resume/dlq 双形态泛化的代价:目标/标签三参显式优于隐式全局,allow)
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `layout`: 分区 key 布局,用于生成 stream、marker 和 lock key。
/// - `p`: 分区编号或当前协议步骤中的短名参数。
/// - `marker`: 分区处置状态 marker key。
/// - `qkey`: 业务 key 或 Redis key,用于定位数据。
/// - `planned`: 预先计算的 stream entry id 列表。
/// - `payloads`: parked 消息对应的 payload 列表。
/// - `泛化`: 分区匹配的泛化标记,用于允许更宽松的调度匹配。
/// - `final_state`: 处置成功后 marker 应进入的最终状态。
/// - `indeterminate_state`: 结果不确定时 marker 应进入的保护状态。
#[allow(clippy::too_many_arguments)]
async fn publish_reserved(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    marker: &str,
    qkey: &str,
    planned: &[String],
    payloads: &[Vec<u8>],
    // 泛化:resume 回源 / dlq 写 DLQ stream 共用同一 reservations 协议,
    // 仅目标 stream 与终态/不确定态标签不同(Resumed/ResumeIndeterminate vs Dlqed/DlqIndeterminate)。
    target_stream: &str,
    final_state: &str,
    indeterminate_state: &str,
) -> Result<Vec<String>> {
    let stream = target_stream.to_string();
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
                // entry 不存在且 last-gen < planned:执行显式 ID XADD
                let r: std::result::Result<String, redis::RedisError> = redis::cmd("XADD")
                    .arg(&stream)
                    .arg(pid)
                    .arg(super::DATA_FIELD)
                    .arg(payload.as_slice())
                    .query_async(&mut client.conn())
                    .await;
                match r {
                    Ok(_) => continue,
                    //写出后**断线** = 可能已执行 → `ExecutionUnknown` 上抛(留
                    // marker 在 *Publishing 可重入),**不得**把基础设施故障固化成 Indeterminate
                    Err(e) if e.is_io_error() || e.is_timeout() || e.is_connection_dropped() => {
                        return Err(NasaRedisError::ExecutionUnknown(format!(
                            "分区 {p} 发布 planned={pid} 写出后断线: {e}"
                        )));
                    }
                    // 否则视为"equal or smaller(被越过)"→ 复账;第二次 reconcile 的错误**也传播**
                    Err(_) => match reconcile_one(client, &stream, pid, payload).await {
                        Ok(true) => continue,
                        Ok(false) => {
                            uncertain.push(i);
                            continue;
                        }
                        Err(e) => return Err(e), // 协议/网络:上抛,不固化成 Indeterminate
                    },
                }
            }
            Err(e) => return Err(e), // 真实网络/协议错误:上抛(marker 留在 ResumePublishing 可重入)
        }
    }

    if !uncertain.is_empty() {
        // 进入持久不确定态:禁自动重预约
        client
            .h_set(
                marker,
                "uncertain",
                serde_json::to_string(&uncertain).unwrap().as_str(),
            )
            .await?;
        client.h_set(marker, "state", indeterminate_state).await?;
        return Err(NasaRedisError::PublishIndeterminate(format!(
            "分区 {p} 有 {} 条 reservation 结果不确定(indexes={uncertain:?}):不自动重发,需 force_republish 或人工对账",
            uncertain.len()
        )));
    }

    // 全部确认 → 收尾。**先提交稳定终态事实,再删唯一恢复数据(quarantine)**。
    // 旧顺序(先 DEL quarantine 再写终态)在两步之间 crash 会留下"*Publishing + 无 payload"——
    // 重入无法对账、永久冻结。新顺序:写 new_ids → 切终态 → 删 index → **最后**删 quarantine;
    // 任一步后 crash,重入都落到终态分支(Resumed/Dlqed)幂等补删 index+quarantine。
    // (TTL 仍最后设:终态事实在 cleanup 完成前不会被 TTL 抹掉。)
    client
        .h_set(marker, "new_ids", planned.join(",").as_str())
        .await?;
    client.h_set(marker, "state", final_state).await?;
    client.del(&[&parked_index_key(layout, p)]).await?;
    client.del(&[qkey]).await?;
    client
        .expire(marker, std::time::Duration::from_secs(24 * 3600))
        .await?;
    Ok(planned.to_vec())
}

/// 业务作用：DLQ stream key(组级一条;消费/巡检由运维侧自行 XREAD)。
///
/// # 参数
/// - `layout`: 分区组 key 布局。
pub fn dlq_stream_key(layout: &KeyLayout) -> String {
    format!("{}:dlq", layout.prefix)
}

/// 业务作用：Dlq:把 Parked 的消息按 planned-ID 协议发布到 DLQ stream,marker 终态 Dlqed。
/// (PoisonPolicy::Dlq = fenced Park 转存 + 本函数自动执行——中途崩溃回退为
///  Parked 可管理态;DlqPublishing 重入/DlqIndeterminate 语义与 resume 完全同构)
///
/// # 参数
/// - `client`: 执行 DLQ 状态机的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `lease`: 当前 owner 的锁和 fence 凭据。
pub async fn dlq_from_parked(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    lease: &OwnerLease<'_>,
) -> Result<Vec<String>> {
    verify_owner_fenced(client, lease).await?; // 入口 owner-fence
    let marker = marker_key(layout, p);
    let state: Option<String> = client.h_get(&marker, "state").await?;
    let park_id: Option<String> = client.h_get(&marker, "park_id").await?;
    let (Some(state), Some(park_id)) = (state, park_id) else {
        return Err(NasaRedisError::Config(format!("分区 {p} 无 Park 记录")));
    };
    let dlq = dlq_stream_key(layout);
    let qkey = quarantine_key(layout, &park_id);
    match state.as_str() {
        "Parked" => {
            // 先 CAS 认领 Parked → DlqPublishing（与 resume 同构），再规划发布。
            claim_transition(client, &marker, "Parked", "DlqPublishing", lease).await?;
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
            )
            .await
        }
        "DlqIndeterminate" => Err(NasaRedisError::OperationInDoubt(format!(
            "分区 {p} 处于 DlqIndeterminate:需人工对账(DLQ 重复风险评估后手工处置)"
        ))),
        "Dlqed" => {
            // 幂等:已完成。终态重入幂等补删 index+quarantine **及补 EXPIRE**
            client.del(&[&parked_index_key(layout, p)]).await?;
            client.del(&[&qkey]).await?;
            client
                .expire(&marker, std::time::Duration::from_secs(24 * 3600))
                .await?;
            let ids: Option<String> = client.h_get(&marker, "new_ids").await?;
            Ok(ids
                .unwrap_or_default()
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

/// 业务作用：ForceRepublish:仅 ResumeIndeterminate 可调;为不确定的 index 分配**全新 planned**
/// 重发——显式接受"可能重复、相对顺序改变"的风险(审计字段记录新旧 planned 对照)。
///
/// # 参数
/// - `client`: 执行 force republish 状态机的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `lease`: 当前 owner 的锁和 fence 凭据。
pub async fn force_republish(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    lease: &OwnerLease<'_>,
) -> Result<Vec<String>> {
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

    // ── **只读预检，绝不改状态**（旧实现先切 ForcePublishing 再校验，
    //    损坏 marker 会被状态污染)。任一校验失败 → Err,marker 仍停 ResumeIndeterminate ──
    let park_id: String = client.h_get(&marker, "park_id").await?.unwrap_or_default();
    if park_id.is_empty() {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "分区 {p} force: marker 缺 park_id(损坏),拒绝(fail-closed)"
        )));
    }
    let qkey = quarantine_key(layout, &park_id);
    // 损坏 JSON **绝不** `unwrap_or_default()` 当空数组
    let planned: Vec<String> = match client.h_get::<String>(&marker, "planned").await? {
        Some(s) => serde_json::from_str(&s).map_err(|e| {
            NasaRedisError::ProtocolMarker(format!("分区 {p} force: planned 损坏不可解析: {e}"))
        })?,
        None => {
            return Err(NasaRedisError::ProtocolMarker(format!(
                "分区 {p} force: marker 缺 planned 字段(损坏),拒绝(fail-closed)"
            )))
        }
    };
    let uncertain: Vec<usize> = match client.h_get::<String>(&marker, "uncertain").await? {
        Some(s) => serde_json::from_str(&s).map_err(|e| {
            NasaRedisError::ProtocolMarker(format!("分区 {p} force: uncertain 损坏不可解析: {e}"))
        })?,
        None => {
            return Err(NasaRedisError::ProtocolMarker(format!(
                "分区 {p} force: marker 缺 uncertain 字段(损坏),拒绝(fail-closed)"
            )))
        }
    };
    if uncertain.is_empty() {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "分区 {p} force: uncertain 为空,不应进入 force(marker 语义不一致)"
        )));
    }
    //**拒绝重复 uncertain index**(实测 `[0,0]` 会对同一 payload 重发两次);同时
    // 校验越界(否则 `planned[i]` panic)
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

    // 预检全部通过后才 CAS 认领 ResumeIndeterminate → ForcePublishing；重入时跳过认领。
    if st == Some("ResumeIndeterminate") {
        claim_transition(
            client,
            &marker,
            "ResumeIndeterminate",
            "ForcePublishing",
            lease,
        )
        .await?;
    }

    // 逐 index 对账并按需重发。
    //    **每个 index 在 XADD 前先持久化 `force:{i}`=target**;重入对账**已持久化的同一 target**:
    //    已发布(Ok(true))复用、RETRYABLE 同 target 幂等补发——绝不为这两种情形重新分配,杜绝
    //    "崩溃一次多一条副本"的无界重复。**唯一例外**:已持久 target 被越过且从未发布(Ok(false))=死
    //    target → **同 op 内弃死 target、重分配 `> last-gen` 的全新 ID 并发布**(/
    //    死结修复,详见下方 :1099-1106;旧 target 从未发布故不产生重复,force 语义本就显式接受重复/变序)。
    //    (历史注:此处曾写"→ ForceIndeterminate 须新 operation 再授权"的旧行为——已随 作废,
    //     `ForceIndeterminate` 全代码无任何 HSET、是死词,勿据旧注释判"死结"。)──
    let stream = layout.stream(p);
    let mut final_ids = planned.clone();
    let mut audit: Vec<(usize, String, String)> = Vec::new(); // (index, 旧 planned, 新 target)
    for (i, payload) in &payloads {
        let i = *i;
        let item_field = format!("force:{i}");
        // 重入:本 index 已提交过 target → 只对账/补发同一 target
        if let Some(target) = client.h_get::<String>(&marker, &item_field).await? {
            match reconcile_one(client, &stream, &target, payload).await {
                Ok(true) => {
                    final_ids[i] = target;
                    continue;
                }
                Err(NasaRedisError::Config(sig)) if sig == "RETRYABLE" => {
                    // 同 target 幂等补发(显式 ID;崩在首发前的续作)
                    xadd_force_target(client, &stream, &target, payload, i).await?;
                    final_ids[i] = target;
                    continue;
                }
                Ok(false) => {
                    // 已提交 target 被越过且从未发布时，它已成为不可达目标，必须重新分配。
                    // 此前直接 return PublishIndeterminate → 同 op 重入恒读此死 target、恒 Ok(false) → marker
                    // 永停 ForcePublishing、quarantine/index 不删、producer 永远拿不到终态;且 op_id 稳定
                    // (`direct:{p}:{park_id}`)→ force_takeover 覆写后仍是同 op、读同一死 target → **不自愈**。
                    // 修复:**弃死 target、重分配 > last-gen 的全新 target 并发布**(同首发 Ok(false) 路径)。
                    // 旧 target 从未发布(Ok(false)),重分配不产生重复;force 语义本就显式接受重复/变序。
                    // 崩在覆写后由下次重入据**新 target** 复账;若新 target 又被越过则下次重入再重分配 → 自愈。
                    let base = last_generated(client, &stream).await?;
                    let fresh = plan_ids(base, 1)?.remove(0);
                    client.h_set(&marker, &item_field, fresh.as_str()).await?; // 覆写死 target(先持久后 XADD)
                    xadd_force_target(client, &stream, &fresh, payload, i).await?;
                    audit.push((i, target.clone(), fresh.clone()));
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
        // **先持久化 target,再 XADD**:重入据 `force:{i}` 只对账此 target,不重分配
        client.h_set(&marker, &item_field, target.as_str()).await?;
        xadd_force_target(client, &stream, &target, payload, i).await?;
        if target != planned[i] {
            audit.push((i, planned[i].clone(), target.clone()));
        }
        final_ids[i] = target;
    }

    // 收尾必须**先写完整 terminal 事实，再删除唯一恢复数据 quarantine**。
    //    旧顺序先 DEL quarantine,crash 在写终态前 → 无 payload + ForcePublishing 永久冻结 ──
    client.h_set(&marker, "forced", "1").await?;
    client
        .h_set(
            &marker,
            "force_audit",
            serde_json::to_string(&audit).unwrap().as_str(),
        )
        .await?;
    client
        .h_set(&marker, "new_ids", final_ids.join(",").as_str())
        .await?;
    client.h_set(&marker, "state", "Resumed").await?;
    client.del(&[&parked_index_key(layout, p)]).await?;
    client.del(&[&qkey]).await?;
    client
        .expire(&marker, std::time::Duration::from_secs(24 * 3600))
        .await?;
    tracing::warn!(p, ?audit, "force_republish 完成(可能重复/变序,审计已记)");
    Ok(final_ids)
}

/// 业务作用：force 的显式-ID XADD:写**已提交的 target**。断线 → `ExecutionUnknown`(可能已执行,
/// 重入时按 `force:{i}` 复账且绝不重分配；"equal/smaller(被越过)" 会复账，确认已发布则成功，
/// 否则 `PublishIndeterminate`(target 丢失,须新 operation)。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `stream`: 显式 ID 写入的目标 Redis Stream key。
/// - `target`: 已提交的目标 stream entry id。
/// - `payload`: 待写入 entry 的 parked payload。
/// - `i`: 当前循环处理的元素序号。
async fn xadd_force_target(
    client: &Arc<RedisClient>,
    stream: &str,
    target: &str,
    payload: &[u8],
    i: usize,
) -> Result<()> {
    let r: std::result::Result<String, redis::RedisError> = redis::cmd("XADD")
        .arg(stream)
        .arg(target)
        .arg(super::DATA_FIELD)
        .arg(payload)
        .query_async(&mut client.conn())
        .await;
    match r {
        Ok(_) => Ok(()),
        Err(e) if e.is_io_error() || e.is_timeout() || e.is_connection_dropped() => {
            Err(NasaRedisError::ExecutionUnknown(format!(
                "force XADD target={target}(index {i})写出后断线: {e}"
            )))
        }
        Err(_) => match reconcile_one(client, stream, target, payload).await {
            Ok(true) => Ok(()), // 被越过但已确认发布
            Ok(false) => Err(NasaRedisError::PublishIndeterminate(format!(
                "force index {i}: target={target} 被越过且无法发布(真不确定)——marker 停 ForcePublishing,需 operator 介入(force_takeover / 人工对账)"
            ))),
            Err(e) => Err(e),
        },
    }
}

/// 业务作用：Drop:放弃 Park 的消息(payload 删除,marker 终态 Dropped 保留供审计)。
///
/// # 参数
/// - `client`: 执行 drop 状态机的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `lease`: 当前 owner 的锁和 fence 凭据。
pub async fn drop_parked(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    lease: &OwnerLease<'_>,
) -> Result<()> {
    verify_owner_fenced(client, lease).await?; // 入口 owner-fence(Dropping 重入也校验)
    let marker = marker_key(layout, p);
    let state: Option<String> = client.h_get(&marker, "state").await?;
    let park_id: Option<String> = client.h_get(&marker, "park_id").await?;
    let (Some(state), Some(park_id)) = (state, park_id) else {
        return Err(NasaRedisError::Config(format!("分区 {p} 无 Park 记录")));
    };
    //Drop 改 `Parked → Dropping(intent) → Dropped(cleanup_done)`,清理可重入。
    // 旧实现直接 CAS 进终态 Dropped,认领后崩溃在清理前 → 重入因"非 Parked"被拒,quarantine/
    // index 永久残留。现在:Parked 先 CAS 认领进 Dropping;Dropping/Dropped 都幂等续清理。
    match state.as_str() {
        "Parked" => {
            claim_transition(client, &marker, "Parked", "Dropping", lease).await?;
        }
        // 重入(认领后/清理中 crash):幂等续作。:Dropping 在途须同一 operation;Dropped 终态
        // 是幂等清理(已完成,不再校验 op,允许任何 owner 补清残留)。
        "Dropping" => assert_op_owns(client, &marker, lease.operation_id).await?,
        "Dropped" => {}
        other => {
            return Err(NasaRedisError::Config(format!(
                "分区 {p} marker 状态 {other},非 Parked,拒绝 drop"
            )))
        }
    }
    // 幂等清理(可重入):删 quarantine + index → 切终态 Dropped → 最后设 TTL
    client.del(&[&quarantine_key(layout, &park_id)]).await?;
    client.del(&[&parked_index_key(layout, p)]).await?;
    client.h_set(&marker, "state", "Dropped").await?;
    client
        .expire(&marker, std::time::Duration::from_secs(24 * 3600))
        .await?;
    tracing::warn!(p, park_id, "Parked 消息已 Drop(审计 marker 保留 24h)");
    Ok(())
}

/// 业务作用：**通用 liveness-takeover**：operator 显式接管某分区卡死的在途 `*Publishing`/`Dropping`
/// ——原 op 永久丢失(进程死)、无法自行重提,普通续作因 `assert_op_owns` 异 op → `OperationConflict`
/// 推不动,分区冻结。本函数**强制覆写** `transition_operation_id` 为本 `lease.operation_id`(显式接受
/// "可能与原 op 重复推进"的风险,取向同 `force_republish`),再按当前 state 续作到终态。
///
/// 安全边界:
///   · 仅接管**在途 `ResumePublishing`/`DlqPublishing`/`ForcePublishing`/`Dropping`**;
///   · **不接管** `Parked`(有 admin resume/drop/dlq 主体)、终态、`*Indeterminate`(不确定终态须人工对账,
///     不在此越过安全闸);
///   · **state-CAS 覆写**:仅当覆写瞬间 state 未变才覆写——若原 op 其实还活着、刚把 state 推进了,CAS 失败
///     返 `OperationConflict`,避免"接管把活 op 的推进覆盖掉"。
/// ⚠ 这是 operator 显式恢复逃生舱，仅在确认原 op 已死后使用；auto-DLQ 由接管方自动续作。
///
/// # 参数
/// - `client`: 执行强制接管状态机的 Redis 客户端。
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `lease`: 当前 owner 的锁和 fence 凭据。
pub async fn force_takeover(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    lease: &OwnerLease<'_>,
) -> Result<Vec<String>> {
    verify_owner_fenced(client, lease).await?;
    let marker = marker_key(layout, p);
    let state: Option<String> = client.h_get(&marker, "state").await?;
    let Some(state) = state else {
        return Err(NasaRedisError::Config(format!(
            "分区 {p} 无 marker,无在途转换可接管"
        )));
    };
    if !matches!(
        state.as_str(),
        "ResumePublishing" | "DlqPublishing" | "ForcePublishing" | "Dropping"
    ) {
        return Err(NasaRedisError::Config(format!(
            "分区 {p} state={state} 非可接管的在途 *Publishing/Dropping\
             (Parked 用 resume/drop/dlq;*Indeterminate 需人工对账)"
        )));
    }
    // 强制覆写 op(state-CAS:仅当 state 未变才覆写,防把活 op 刚推进的状态覆盖)
    const TAKEOVER_LUA: &str = r#"
if redis.call('HGET', KEYS[1], 'state') == ARGV[1] then
    redis.call('HSET', KEYS[1], 'transition_operation_id', ARGV[2])
    return 1
end
return 0
"#;
    let won: i64 = redis::Script::new(TAKEOVER_LUA)
        .key(&marker)
        .arg(&state)
        .arg(lease.operation_id)
        .invoke_async(&mut client.conn())
        .await?;
    if won != 1 {
        return Err(NasaRedisError::OperationConflict(format!(
            "分区 {p} 接管期间 state 已从 {state} 变更(原 op 可能仍活跃),请重试或先确认原 op 已死"
        )));
    }
    tracing::warn!(p, %state, new_op = lease.operation_id, "通用 liveness-takeover:已覆写 op,续作在途转换");
    // 覆写后 assert_op_owns 对本 lease.op 通过 → 调匹配的续作函数推进到终态
    match state.as_str() {
        "ResumePublishing" => resume(client, layout, p, lease).await,
        "DlqPublishing" => dlq_from_parked(client, layout, p, lease).await,
        "ForcePublishing" => force_republish(client, layout, p, lease).await,
        "Dropping" => {
            drop_parked(client, layout, p, lease).await?;
            Ok(Vec::new())
        }
        _ => unreachable!(),
    }
}
