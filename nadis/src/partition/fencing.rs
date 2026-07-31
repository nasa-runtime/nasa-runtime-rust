// ============================================================================
// src/partition/fencing.rs —— V2 FencingStamp / bootstrap 状态机(R4.2d)。
// (架构文档: per-tag 状态 + 全局三态 + 弱承诺版)
//
// 解决的问题:holds 双检查(V1 协议)在"检查与 ACK 之间"仍有失锁窗口——V2 用
// **单 Lua 原子校验 holder + 任期 stamp 后才 XACK**,旧 owner(更早任期)的迟到 ACK
// 被 counter 比较拒绝(STALE)。
//
// ┌─ Key 布局(单 tag 简化形态:tag = 组前缀;多 tag/Cluster = R4.2e)──────────┐
// │ 全局 bootstrap marker : nasa:fence-bootstrap:{namespace}(JSON:state         │
// │   Initializing|Activating|Ready + round + owner_term + nonce)                │
// │ per-tag fence HASH    : {prefix}:fence(fields:state/round/nonce/epoch +     │
// │   per-partition counter `c:{p}`——**stamp 的 scope = 分区**,:不同      │
// │   分区的 counter 互不可比,组级单 counter 会让任意分区的新任期废掉全组 stamp)│
// └────────────────────────────────────────────────────────────────────────┘
//
// bootstrap 三态流程(不能先全局 Ready 再激活——owner 中途崩溃即永久半激活)
//   全局 Initializing{round} → per-tag init(五规则,幂等)→ 全局 Activating{nonce}
//   → per-tag activate(同 round/nonce)→ **重验全部 tag Ready** → 全局 Ready。
//   任一阶段崩溃:重跑 bootstrap 以同 round/nonce 幂等续建。
//
// ── R4.2e:bootstrap owner TTL lease + owner_term 接管──
// 为什么需要 lease:无 lease 时两节点并发走到"Initializing→Activating"会**各自生成
// nonce 抢写 marker**——tag 可能用 A 的 nonce 激活而 marker 落 B 的 nonce,verify
// 永久失败。lease 把"推进状态机"收敛到单 owner:
//   · lease key = nasa:fence-bootstrap-owner:{ns}(SET NX PX,值 = owner token);
//   · 只有 lease holder 写 marker / 跑 init/activate;非 holder 轮询 marker 等 Ready;
//   · owner 崩溃 → lease TTL 过期 → 新 owner 抢到 lease,**owner_term+1**(接管递增,
//     round/nonce 不变,半激活续建沿用——);
//   · owner_term 是弱承诺:仅标记"第几任 owner 完成的",不参与 acquire/ACK
//     比对(那靠 round+nonce+counter);
//   · TTL(10s)≫ bootstrap 实际耗时(几个 RTT)——"写 marker 时 lease 已过期"的
//     残余窗口按弱承诺如实声明:同 round/nonce 幂等写,交错无害。
//
// acquire_stamp(锁获取成功后调用):Lua 校验 fence Ready+round+nonce → HINCRBY c:{p}
//   → FencingStamp{p, epoch, counter}。新 owner acquire 必使 counter+1 → 旧 stamp 失效。
//
// fenced ACK(V2 worker 专用):Lua 原子校验 ①分区锁 holder ②fence Ready+round+nonce
//   ③c:{p} == 我的 counter(我仍是最新任期)→ XACK ids;任一不符返回拒绝标签,绝不 ACK。
// ============================================================================

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::KeyLayout;
use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};

/// 全局 bootstrap marker key。
///
/// # 参数
/// - `namespace`: Redis 协议命名空间。
pub fn bootstrap_key(namespace: &str) -> String {
    format!("nasa:fence-bootstrap:{namespace}")
}

/// bootstrap owner lease key(R4.2e;SET NX PX 独占,值 = owner token)。
///
/// # 参数
/// - `namespace`: Redis 协议命名空间。
pub fn bootstrap_owner_key(namespace: &str) -> String {
    format!("nasa:fence-bootstrap-owner:{namespace}")
}

/// owner lease TTL:远大于 bootstrap 实际耗时(几个 RTT);过期 = owner 崩溃,
/// 新 owner 接管(owner_term+1)。
const OWNER_LEASE_TTL_MS: u64 = 10_000;
/// 非 owner 等待 Ready 的轮询间隔 / 总等待上限。
const WAIT_POLL_MS: u64 = 200;
const WAIT_DEADLINE_MS: u64 = 30_000;

/// token 匹配才 DEL 的释放 Lua(防误删新 owner 的 lease——与锁的 UNLOCK 同思路)。
const RELEASE_OWNER_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"#;

/// per-tag fence HASH key。
///
/// # 参数
/// - `layout`: 分区组 key 布局。
pub fn fence_key(layout: &KeyLayout) -> String {
    format!("{}:fence", layout.prefix)
}

/// 全局 bootstrap marker(JSON)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapMarker {
    /// bootstrap 状态:Initializing / Activating / Ready。
    pub state: String, // Initializing | Activating | Ready
    /// bootstrap 轮次,用于拒绝旧轮次迟到写入。
    pub round: u64,
    /// owner lease 任期,用于判断当前初始化者是否仍有效。
    pub owner_term: u64,
    #[serde(default)]
    /// 本轮 bootstrap 随机标识,防同轮不同 owner 混淆。
    pub nonce: String,
}

/// 任期凭据(scope = 分区;:跨 scope 禁比较)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FencingStamp {
    /// 分区编号。
    pub p: u32,
    /// 分区 epoch,每次 owner 变更递增。
    pub epoch: u64,
    /// 任期序号(per-partition HINCRBY;≤ i64::MAX, 数值边界)。
    pub counter: u64,
}

/// fence 运行参数(bootstrap 完成后缓存于 GroupRuntime;acquire/ACK Lua 逐次携带比对)。
#[derive(Debug, Clone)]
pub struct FenceMeta {
    /// 当前 bootstrap round。
    pub round: u64,
    /// 当前 bootstrap nonce。
    pub nonce: String,
    /// 当前分区 epoch。
    pub epoch: u64,
}

/// per-tag init Lua
/// ①不存在→建 Initializing;②round 相同且字段完整→幂等;③本地 round 更大→拒;
/// ④输入更大→仅覆盖 Initializing&&counter 全零的旧态(R4.2d 单 tag 简化:覆盖即重建);
/// ⑤字段残缺→CORRUPT(禁猜值修复)。
const TAG_INIT_LUA: &str = r#"
local state = redis.call('HGET', KEYS[1], 'state')
local round = tonumber(redis.call('HGET', KEYS[1], 'round') or '0')
local input = tonumber(ARGV[1])
if state == false then
    redis.call('HSET', KEYS[1], 'state', 'Initializing', 'round', ARGV[1], 'epoch', ARGV[2])
    return 'OK'
end
if round == input then
    if redis.call('HGET', KEYS[1], 'epoch') == false then return 'CORRUPT' end
    return 'OK'
end
if round > input then return 'REJECT_OLD_OWNER' end
if state == 'Initializing' then
    -- 覆盖前必须兑现"仅 counter 全零才覆盖":存在任何任期
    -- counter(c:*)就拒绝 DEL 重建(否则更高 round 会静默清掉已发任期,旧 stamp 失去比对依据)
    local keys = redis.call('HKEYS', KEYS[1])
    for _, k in ipairs(keys) do
        if string.sub(k, 1, 2) == 'c:' then return 'REJECT_HAS_COUNTERS' end
    end
    redis.call('DEL', KEYS[1])
    redis.call('HSET', KEYS[1], 'state', 'Initializing', 'round', ARGV[1], 'epoch', ARGV[2])
    return 'OK'
end
return 'REJECT_READY'
"#;

/// per-tag activate Lua(同 round 才激活;写 nonce;幂等)。
const TAG_ACTIVATE_LUA: &str = r#"
local round = redis.call('HGET', KEYS[1], 'round')
if round ~= ARGV[1] then return 'ROUND_MISMATCH' end
redis.call('HSET', KEYS[1], 'state', 'Ready', 'nonce', ARGV[2])
return 'OK'
"#;

/// acquire stamp Lua:校验 Ready+round+nonce → HINCRBY c:{p} → 返回 {epoch, counter}。
const ACQUIRE_STAMP_LUA: &str = r#"
if redis.call('HGET', KEYS[1], 'state') ~= 'Ready' then return {'FENCE_NOT_READY'} end
if redis.call('HGET', KEYS[1], 'round') ~= ARGV[1] then return {'ROUND_MISMATCH'} end
if redis.call('HGET', KEYS[1], 'nonce') ~= ARGV[2] then return {'NONCE_MISMATCH'} end
local c = redis.call('HINCRBY', KEYS[1], 'c:' .. ARGV[3], 1)
local epoch = redis.call('HGET', KEYS[1], 'epoch')
return {'OK', epoch, tostring(c)}
"#;

/// V2 fenced ACK Lua:holder + fence 三元组 + 任期 counter 全部原子校验后才 XACK。
/// KEYS=[锁key, stream, fence];ARGV=[holder, group, round, nonce, p, my_counter, id...]
const FENCED_ACK_LUA: &str = r#"
if redis.call('hexists', KEYS[1], ARGV[1]) == 0 then return 'NOT_HOLDER' end
if redis.call('HGET', KEYS[3], 'state') ~= 'Ready' then return 'FENCE_NOT_READY' end
if redis.call('HGET', KEYS[3], 'round') ~= ARGV[3] then return 'ROUND_MISMATCH' end
if redis.call('HGET', KEYS[3], 'nonce') ~= ARGV[4] then return 'NONCE_MISMATCH' end
local cur = redis.call('HGET', KEYS[3], 'c:' .. ARGV[5])
if cur ~= ARGV[6] then return 'STALE_STAMP' end
local n = 0
for i = 7, #ARGV do
    n = n + redis.call('XACK', KEYS[2], ARGV[2], ARGV[i])
end
return n
"#;

/// 执行 bootstrap(幂等可重入;任一阶段崩溃后由新 owner 同 round 续建)。
/// 返回 FenceMeta(round/nonce/epoch)供 acquire/ACK 使用。
///
/// R4.2e 流程:Ready 快路径 → 否则抢 owner lease;抢到 = 单 owner 推进状态机
/// (接管半成品时 owner_term+1);没抢到 = 轮询 marker 等 owner 完成,lease 过期
/// (owner 崩溃)后下一轮重新抢——活着的节点里总有一个能当上 owner。
///
/// # 参数
/// - `client`: 执行 bootstrap 状态机的 Redis 客户端。
/// - `layout`: 当前分区组 key 布局。
/// - `epoch`: 当前运行期 epoch,写入 fence 元数据。
pub async fn bootstrap(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    epoch: u64,
) -> Result<FenceMeta> {
    let ns = client.config().namespace.clone();
    let bkey = bootstrap_key(&ns);
    let okey = bootstrap_owner_key(&ns);
    let fkey = fence_key(layout);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(WAIT_DEADLINE_MS);

    loop {
        // ── Ready 快路径(无论是否 owner,先看一眼)──
        if let Some(raw) = client.get::<String>(&bkey).await? {
            let marker: BootstrapMarker = serde_json::from_str(&raw)
                .map_err(|e| NasaRedisError::Codec(format!("bootstrap marker 解析失败: {e}")))?;
            if marker.state == "Ready" {
                // 全局 Ready 且**本组 fence tag 已 seed** → 快返回。若本组 tag 未 seed(**多组场景**:全局
                // marker 是 namespace 级,新隔离组首次 bootstrap 时全局已 Ready,但本组 `{prefix}:fence`
                // 尚未 seed)→ 不报错,落到下方 owner 路径用**既有** round/nonce seed 本组 tag。
                if verify_tag_ready(client, &fkey, marker.round, &marker.nonce)
                    .await
                    .is_ok()
                {
                    //Ready fence **缺 epoch = 持久协议损坏**,fail-closed(不用
                    // 调用参数 `epoch` 补猜——那会让不同节点对同一 fence 得到不同 epoch)
                    let epoch_now: u64 = client.h_get(&fkey, "epoch").await?.ok_or_else(|| {
                        NasaRedisError::ProtocolMarker(format!(
                            "fence {fkey} 为 Ready 但缺 epoch 字段(协议损坏),fail-closed"
                        ))
                    })?;
                    return Ok(FenceMeta {
                        round: marker.round,
                        nonce: marker.nonce,
                        epoch: epoch_now,
                    });
                }
            }
        }

        // ── 抢 owner lease(SET NX PX;token 防误删他人 lease)──
        let token = uuid::Uuid::new_v4().simple().to_string();
        let got: Option<String> = redis::cmd("SET")
            .arg(&okey)
            .arg(&token)
            .arg("NX")
            .arg("PX")
            .arg(OWNER_LEASE_TTL_MS)
            .query_async(&mut client.conn())
            .await?;
        if got.is_some() {
            // 当上 owner:推进状态机;无论成败都释放 lease(token 匹配才 DEL,
            // 过期后被新 owner 持有时绝不误删)
            let r = bootstrap_as_owner(client, &bkey, &fkey, epoch).await;
            let _: std::result::Result<i64, _> = redis::Script::new(RELEASE_OWNER_LUA)
                .key(&okey)
                .arg(&token)
                .invoke_async(&mut client.conn())
                .await;
            return r;
        }

        // ── 没抢到:owner 正在推进,等一拍再看(过期/完成后下一轮兜住)──
        if tokio::time::Instant::now() >= deadline {
            return Err(NasaRedisError::ProtocolMarker(
                "bootstrap 等待全局 Ready 超时(owner 持续未完成)".into(),
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(WAIT_POLL_MS)).await;
    }
}

/// owner 视角的状态机推进(调用方已持 owner lease;marker 写入仅此处发生)。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `bkey`: 业务 key 或 Redis key,用于定位数据。
/// - `fkey`: 业务 key 或 Redis key,用于定位数据。
/// - `epoch`: 分区任期 epoch。
async fn bootstrap_as_owner(
    client: &Arc<RedisClient>,
    bkey: &str,
    fkey: &str,
    epoch: u64,
) -> Result<FenceMeta> {
    // ① 全局 marker:读取或建 Initializing(SET NX;已存在则沿用其 round/nonce)
    let (mut marker, fresh_created) = match client.get::<String>(bkey).await? {
        Some(raw) => {
            let m: BootstrapMarker = serde_json::from_str(&raw)
                .map_err(|e| NasaRedisError::Codec(format!("bootstrap marker 解析失败: {e}")))?;
            (m, false)
        }
        None => {
            let fresh = BootstrapMarker {
                state: "Initializing".into(),
                round: 1,
                owner_term: 1,
                nonce: String::new(),
            };
            let payload = serde_json::to_string(&fresh).unwrap();
            let created: Option<String> = redis::cmd("SET")
                .arg(bkey)
                .arg(&payload)
                .arg("NX")
                .query_async(&mut client.conn())
                .await?;
            if created.is_some() {
                (fresh, true)
            } else {
                // 理论不可达(我们持 lease,无并发写者);防御读回
                let m: BootstrapMarker =
                    serde_json::from_str(&client.get::<String>(bkey).await?.unwrap_or_default())
                        .map_err(|e| NasaRedisError::Codec(e.to_string()))?;
                (m, false)
            }
        }
    };

    // ② Ready:上任 owner 恰好刚完成 → 快路径。**多组**:全局已 Ready 但本组 tag 未 seed(新隔离组)→
    // 用全局**既有** round/nonce seed 本组 tag(global marker 不动,owner_term 不增),再返回。
    if marker.state == "Ready" {
        if verify_tag_ready(client, fkey, marker.round, &marker.nonce)
            .await
            .is_err()
        {
            seed_tag(client, fkey, marker.round, &marker.nonce, epoch).await?;
        }
        //Ready 缺 epoch = 协议损坏,fail-closed(不用调用参数补猜)
        let epoch_now: u64 = client.h_get(fkey, "epoch").await?.ok_or_else(|| {
            NasaRedisError::ProtocolMarker(format!(
                "fence {fkey} 为 Ready 但缺 epoch 字段(协议损坏),fail-closed"
            ))
        })?;
        return Ok(FenceMeta {
            round: marker.round,
            nonce: marker.nonce,
            epoch: epoch_now,
        });
    }

    //**未知全局 state 显式拒绝**(不继续 init/activate 把损坏态推成 Ready)。
    // 合法的非 Ready 半成品只有 Initializing / Activating;其余一律 fail-closed。
    if marker.state != "Initializing" && marker.state != "Activating" {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "bootstrap marker 未知/损坏 state={}(非 Initializing/Activating/Ready),fail-closed",
            marker.state
        )));
    }
    // Activating 半成品必须已有 nonce(否则下游 activate 会用空 nonce)→ 缺即损坏
    if marker.state == "Activating" && marker.nonce.is_empty() {
        return Err(NasaRedisError::ProtocolMarker(
            "bootstrap marker 为 Activating 但 nonce 为空(协议损坏),fail-closed".into(),
        ));
    }

    // ②′ 接管:已存在的非 Ready 半成品(上任 owner 崩溃)→ owner_term+1 落盘
    //    (round/nonce 不动,"round 全程不变 term 仅接管递增";term 是弱承诺,
    //     仅审计"第几任 owner 完成的",不参与 acquire/ACK 比对)
    if !fresh_created {
        marker.owner_term += 1;
        client
            .set(bkey, serde_json::to_string(&marker).unwrap().as_str())
            .await?;
        tracing::warn!(
            round = marker.round,
            owner_term = marker.owner_term,
            state = %marker.state,
            "接管半完成 bootstrap(上任 owner 未完成)"
        );
    }

    // ③ per-tag init(五规则 Lua,幂等续建)
    let r: String = redis::Script::new(TAG_INIT_LUA)
        .key(fkey)
        .arg(marker.round)
        .arg(epoch)
        .invoke_async(&mut client.conn())
        .await?;
    if r != "OK" {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "fence init 被拒: {r}"
        )));
    }

    // ④ 全局 → Activating(生成/沿用 nonce;Activating 重入沿用既有 nonce——
    //:新 owner 保留原 round/nonce 幂等完成,不重新生成)
    if marker.state == "Initializing" {
        marker.state = "Activating".into();
        marker.nonce = uuid::Uuid::new_v4().simple().to_string();
        client
            .set(bkey, serde_json::to_string(&marker).unwrap().as_str())
            .await?;
    }

    // ⑤ per-tag activate(同 round + nonce)
    let r: String = redis::Script::new(TAG_ACTIVATE_LUA)
        .key(fkey)
        .arg(marker.round)
        .arg(&marker.nonce)
        .invoke_async(&mut client.conn())
        .await?;
    if r != "OK" {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "fence activate 被拒: {r}"
        )));
    }

    // ⑥ 重验全部 tag Ready(单 tag 形态 = 一次)→ 才置全局 Ready
    verify_tag_ready(client, fkey, marker.round, &marker.nonce).await?;
    marker.state = "Ready".into();
    client
        .set(bkey, serde_json::to_string(&marker).unwrap().as_str())
        .await?;
    tracing::info!(
        round = marker.round,
        owner_term = marker.owner_term,
        "fencing bootstrap 完成(全局 Ready)"
    );
    Ok(FenceMeta {
        round: marker.round,
        nonce: marker.nonce,
        epoch,
    })
}

/// 校验 per-tag fence 已 Ready 且三元组一致(运行节点 acquire 前置条件)。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `fkey`: 业务 key 或 Redis key,用于定位数据。
/// - `round`: bootstrap 或 fencing 轮次。
/// - `nonce`: 本轮 fencing/bootstrap 的随机标识。
async fn verify_tag_ready(
    client: &Arc<RedisClient>,
    fkey: &str,
    round: u64,
    nonce: &str,
) -> Result<()> {
    let state: Option<String> = client.h_get(fkey, "state").await?;
    let r: Option<u64> = client.h_get(fkey, "round").await?;
    let n: Option<String> = client.h_get(fkey, "nonce").await?;
    if state.as_deref() != Some("Ready") || r != Some(round) || n.as_deref() != Some(nonce) {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "fence tag 未就绪/不一致: state={state:?} round={r:?}(期望 {round}) nonce 匹配={}",
            n.as_deref() == Some(nonce)
        )));
    }
    Ok(())
}

/// 把一个 per-tag fence seed 到给定 round/nonce/epoch(TAG_INIT + TAG_ACTIVATE,均幂等),并校验 Ready。
/// **多组**:全局 bootstrap marker 已 Ready(namespace 级),新隔离组用既有 round/nonce seed 自己的 tag;
/// 与 `bootstrap_as_owner` 主流程的 ③⑤ 同 Lua,只是不动全局 marker。调用方必须持 owner lease。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `fkey`: 业务 key 或 Redis key,用于定位数据。
/// - `round`: bootstrap 或 fencing 轮次。
/// - `nonce`: 本轮 fencing/bootstrap 的随机标识。
/// - `epoch`: 分区任期 epoch。
async fn seed_tag(
    client: &Arc<RedisClient>,
    fkey: &str,
    round: u64,
    nonce: &str,
    epoch: u64,
) -> Result<()> {
    let r: String = redis::Script::new(TAG_INIT_LUA)
        .key(fkey)
        .arg(round)
        .arg(epoch)
        .invoke_async(&mut client.conn())
        .await?;
    if r != "OK" {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "fence init 被拒(seed_tag): {r}"
        )));
    }
    let r: String = redis::Script::new(TAG_ACTIVATE_LUA)
        .key(fkey)
        .arg(round)
        .arg(nonce)
        .invoke_async(&mut client.conn())
        .await?;
    if r != "OK" {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "fence activate 被拒(seed_tag): {r}"
        )));
    }
    verify_tag_ready(client, fkey, round, nonce).await
}

/// 获取分区任期 stamp(锁 acquire 成功后调用;新任期使旧 stamp 失效)。
///
/// # 参数
/// - `client`: 执行 HINCRBY 任期递增的 Redis 客户端。
/// - `layout`: 当前分区组 key 布局。
/// - `meta`: bootstrap 得到的 fence 元数据。
/// - `p`: 分区编号。
pub async fn acquire_stamp(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    meta: &FenceMeta,
    p: u32,
) -> Result<FencingStamp> {
    let v: redis::Value = redis::Script::new(ACQUIRE_STAMP_LUA)
        .key(fence_key(layout))
        .arg(meta.round)
        .arg(&meta.nonce)
        .arg(p)
        .invoke_async(&mut client.conn())
        .await?;
    let redis::Value::Array(arr) = v else {
        return Err(NasaRedisError::ProtocolMarker(
            "acquire_stamp 响应异常".into(),
        ));
    };
    let tag = match arr.first() {
        Some(redis::Value::BulkString(b)) => String::from_utf8_lossy(b).into_owned(),
        Some(redis::Value::SimpleString(s)) => s.clone(),
        _ => String::new(),
    };
    if tag != "OK" {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "acquire_stamp 被拒: {tag}"
        )));
    }
    // 数值边界纪律(架构自检订正:原先 unwrap_or 猜值——解析失败静默回退会让
    // 损坏的 fence state 伪装成合法 stamp):epoch/counter 解析失败、counter 非正、
    // 超 i64::MAX 一律视为不可恢复协议错误,**不猜值、不 wrapping**。
    let epoch: u64 = match arr.get(1) {
        Some(redis::Value::BulkString(b)) => String::from_utf8_lossy(b).parse().map_err(|_| {
            NasaRedisError::ProtocolMarker(format!(
                "fence epoch 字段损坏: {:?}",
                String::from_utf8_lossy(b)
            ))
        })?,
        other => {
            return Err(NasaRedisError::ProtocolMarker(format!(
                "acquire_stamp 响应缺 epoch: {other:?}"
            )))
        }
    };
    let counter: u64 = match arr.get(2) {
        Some(redis::Value::BulkString(b)) => String::from_utf8_lossy(b).parse().map_err(|_| {
            NasaRedisError::ProtocolMarker(format!(
                "fence counter 字段损坏: {:?}",
                String::from_utf8_lossy(b)
            ))
        })?,
        other => {
            return Err(NasaRedisError::ProtocolMarker(format!(
                "acquire_stamp 响应缺 counter: {other:?}"
            )))
        }
    };
    if counter == 0 || counter > i64::MAX as u64 {
        return Err(NasaRedisError::ProtocolMarker(format!(
            "fence counter 越界(必须 1..=i64::MAX): {counter}"
        )));
    }
    Ok(FencingStamp { p, epoch, counter })
}

/// fenced ACK 的结果。
#[derive(Debug)]
pub enum FencedAck {
    /// 已原子 ACK 的条数。
    Acked(i64),
    /// 任期已被新 owner 取代 / 不再持锁 / fence 失配——一律不 ACK(调用方按 Lost 处理)。
    Rejected(String),
}

/// V2 原子 fenced ACK(worker 专用):单 Lua 校验 holder+fence+任期后 XACK。
/// (参数确实多:三类 key + 五个校验量 + ids——这是协议字段的自然数量,
///  打包结构体反而掩盖 Lua ARGV 的一一对应关系,显式 allow)
///
/// # 参数
/// - `client`: 执行 Lua ACK 的 Redis 客户端。
/// - `layout`: 当前分区组 key 布局。
/// - `meta`: bootstrap 得到的 fence 元数据。
/// - `stamp`: 当前 owner 获取锁后取得的分区任期 stamp。
/// - `lock_full_key`: DistributedLock 完整 Redis 锁 key。
/// - `holder`: 当前 owner 的锁 holder token。
/// - `ids`: 要 ACK 的 stream entry ID 列表。
#[allow(clippy::too_many_arguments)]
pub async fn fenced_ack(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    meta: &FenceMeta,
    stamp: &FencingStamp,
    lock_full_key: &str,
    holder: &str,
    ids: &[String],
) -> Result<FencedAck> {
    let lua = redis::Script::new(FENCED_ACK_LUA);
    let mut inv = lua.prepare_invoke();
    inv.key(lock_full_key)
        .key(layout.stream(stamp.p))
        .key(fence_key(layout))
        .arg(holder)
        .arg(layout.group())
        .arg(meta.round)
        .arg(&meta.nonce)
        .arg(stamp.p)
        .arg(stamp.counter);
    for id in ids {
        inv.arg(id.as_str());
    }
    let v: redis::Value = inv.invoke_async(&mut client.conn()).await?;
    Ok(match v {
        redis::Value::Int(n) => FencedAck::Acked(n),
        redis::Value::BulkString(b) => {
            FencedAck::Rejected(String::from_utf8_lossy(&b).into_owned())
        }
        redis::Value::SimpleString(s) => FencedAck::Rejected(s),
        other => FencedAck::Rejected(format!("{other:?}")),
    })
}
