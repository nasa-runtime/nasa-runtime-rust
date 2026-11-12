//! 分区锁与 fencing 身份共同约束消息确认，任期计数仅在同一物理分区内可比较。
//!
//! 全局 bootstrap marker 保存 Initializing、Activating 或 Ready 状态及 round、owner_term、nonce；
//! 每个物理组另有 fence HASH，保存状态、轮次、激活身份、epoch 和各分区的 c:{p} 计数。
//! 初始化先准备组状态，再发布全局 Ready；后续新增组沿用全局身份补建自己的 HASH。
//!
//! 初始化者通过带 TTL 的独占租约协调，释放只接受原 token，避免删除后来者的租约。
//! 初始化过程中不续期，每次全局发布比较完整旧 marker；迟到写入不能覆盖已提交的接管结果。
//! owner_term 随接管递增并参与 marker 比较，不参与 acquire 或 ACK 的任期凭据检查。
//! 实际取得任期凭据时复验组身份并递增分区计数；ACK 在同一 Lua 中再次检查锁 holder、
//! Ready、round、nonce、epoch 与计数，任何权威不一致均不能确认消息。

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::KeyLayout;
use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};

/// 业务作用：为同一命名空间的所有分区组定位共享初始化标记。
///
/// 参数说明：
/// - `namespace`: 已由客户端配置校验的 Redis 协议命名空间。
///
/// 返回：保存 bootstrap 状态、轮次、任期和 nonce 的全局键名，不执行 Redis 读写。
pub fn bootstrap_key(namespace: &str) -> String {
    format!("nasa:fence-bootstrap:{namespace}")
}

/// 业务作用：定位协调同一命名空间初始化者的限时租约。
///
/// 参数说明：
/// - `namespace`: 已由客户端配置校验的 Redis 协议命名空间。
///
/// 返回：通过 SET NX PX 写入 owner token 的租约键名，键名本身不代表已取得租约。
pub fn bootstrap_owner_key(namespace: &str) -> String {
    format!("nasa:fence-bootstrap-owner:{namespace}")
}

/// 初始化者租约存活时间；崩溃、停顿或网络等待都可能使租约到期，不能把到期仅解释为崩溃。
const OWNER_LEASE_TTL_MS: u64 = 10_000;
/// 非 owner 等待 Ready 的轮询间隔 / 总等待上限。
const WAIT_POLL_MS: u64 = 200;
const WAIT_DEADLINE_MS: u64 = 30_000;

/// 仅原 token 匹配时释放初始化租约，避免删除后继初始化者的租约。
const RELEASE_OWNER_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"#;

/// 单键比较原文后发布全局状态；租约到期后，旧初始化者不能覆盖接管者已提交的标记。
const COMPARE_MARKER_LUA: &str = r#"
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
redis.call('SET', KEYS[1], ARGV[2])
return 1
"#;

/// 业务作用：定位当前物理分区组的共享 fencing 元数据与各分区任期计数。
///
/// 参数说明：
/// - `layout`: 已冻结的分区组物理键布局，RustV2 保持本组相关键同槽。
///
/// 返回：在组前缀后追加 fence 的 HASH 键名，不读取或创建元数据。
pub fn fence_key(layout: &KeyLayout) -> String {
    format!("{}:fence", layout.prefix)
}

/// 全局初始化的持久状态与发布身份，以 JSON 保存。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapMarker {
    /// 初始化状态：Initializing、Activating 或 Ready。
    pub state: String,
    /// 正整数初始化轮次，用于拒绝旧轮次迟到写入。
    pub round: u64,
    /// 从一开始的接管任期，参与全局 marker 原文比较，但不证明当前租约仍由该初始化者持有。
    pub owner_term: u64,
    #[serde(default)]
    /// 本轮激活身份，后续初始化者需要沿用，消费确认据此拒绝其它激活身份。
    pub nonce: String,
}

/// 同一物理分区内可比较的任期凭据，不得跨组或跨分区比较计数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FencingStamp {
    /// 分区编号。
    pub p: u32,
    /// 组初始化时持久化的 epoch；普通分区持锁者更替只递增 counter。
    pub epoch: u64,
    /// 当前物理分区通过 HINCRBY 发出的正整数任期序号，上限为 i64::MAX。
    pub counter: u64,
}

/// 初始化完成后缓存于组运行时的身份，取得任期与确认消息时仍需逐次比对持久状态。
#[derive(Debug, Clone)]
pub struct FenceMeta {
    /// 当前 bootstrap round。
    pub round: u64,
    /// 当前 bootstrap nonce。
    pub nonce: String,
    /// 当前组持久化的初始化 epoch。
    pub epoch: u64,
}

/// 业务作用：读取全局初始化标记原文，区分键缺失与无法作为状态依据的协议响应。
///
/// 参数说明：
/// - `client`: 执行 marker 读取的 Redis 客户端。
/// - `bkey`: 当前命名空间的全局初始化标记键。
///
/// 返回：键缺失时返回 None；合法 UTF-8 bulk string 返回完整原文；异常类型或编码拒绝且不回显正文。
async fn read_marker(client: &Arc<RedisClient>, bkey: &str) -> Result<Option<String>> {
    let response: redis::Value = redis::cmd("GET")
        .arg(bkey)
        .query_async(&mut client.conn())
        .await?;
    // GET 只能以 bulk string 或 Nil 提供状态证据，通用字符串转换会把畸形结构附入错误文本。
    match response {
        redis::Value::Nil => Ok(None),
        redis::Value::BulkString(raw) => String::from_utf8(raw)
            .map(Some)
            .map_err(|_| NasaRedisError::Codec("bootstrap marker UTF-8 编码非法".into())),
        _ => Err(NasaRedisError::ProtocolMarker(
            "bootstrap marker 读取响应非法".into(),
        )),
    }
}

/// 业务作用：将全局初始化标记解码为可用于推进状态机的完整身份。
///
/// 参数说明：
/// - `raw`: Redis 返回的完整 marker 原文，调用方仍需保留它用于比较更新。
///
/// 返回：轮次、任期和状态所需 nonce 全部合法时返回标记；否则拒绝继续，错误不携带原文。
fn parse_bootstrap_marker(raw: &str) -> Result<BootstrapMarker> {
    let marker: BootstrapMarker = serde_json::from_str(raw)
        .map_err(|_| NasaRedisError::Codec("bootstrap marker JSON 格式非法".into()))?;
    // 零身份或状态与 nonce 不一致不能作为接管依据，避免将持久损坏解释成未完成初始化。
    if marker.round == 0
        || marker.owner_term == 0
        || match marker.state.as_str() {
            "Initializing" => !marker.nonce.is_empty(),
            "Activating" | "Ready" => marker.nonce.is_empty(),
            _ => true,
        }
    {
        return Err(NasaRedisError::ProtocolMarker(
            "bootstrap marker 状态或身份非法".into(),
        ));
    }
    Ok(marker)
}

/// 业务作用：仅在全局标记仍与本次观察相同时发布下一状态，约束租约过期后的迟到写入。
///
/// 参数说明：
/// - `client`: 执行单键原子比较更新的 Redis 客户端。
/// - `bkey`: 当前命名空间的全局初始化标记键。
/// - `expected`: 本次推进所依据的完整 marker 原文。
/// - `next`: 需要持久化的下一状态及完整身份。
///
/// 返回：原文匹配并完成写入时返回新原文；冲突、异常回包或网络错误均返回失败，调用方必须停止推进。
async fn compare_marker(
    client: &Arc<RedisClient>,
    bkey: &str,
    expected: &str,
    next: &BootstrapMarker,
) -> Result<String> {
    let payload = serde_json::to_string(next)
        .map_err(|_| NasaRedisError::Codec("bootstrap marker 编码失败".into()))?;
    // marker 自身的原文是状态发布依据，比较只涉及一个键，不依赖租约键与 marker 同槽。
    let result: redis::Value = redis::Script::new(COMPARE_MARKER_LUA)
        .key(bkey)
        .arg(expected)
        .arg(&payload)
        .invoke_async(&mut client.conn())
        .await?;
    match result {
        redis::Value::Int(1) => Ok(payload),
        redis::Value::Int(0) => Err(NasaRedisError::ProtocolMarker(
            "bootstrap marker 已变化，当前初始化者停止推进".into(),
        )),
        _ => Err(NasaRedisError::ProtocolMarker(
            "bootstrap marker 比较更新响应非法".into(),
        )),
    }
}

/// 业务作用：将组初始化或激活脚本的固定响应转换为可继续推进状态机的证据。
///
/// 参数说明：
/// - `reply`: TAG_INIT 或 TAG_ACTIVATE 的原始 Redis 响应。
///
/// 返回：精确 OK 才成功；已知拒绝返回固定原因，其它形态或文本一律拒绝且不回显内容。
fn check_tag_reply(reply: redis::Value) -> Result<()> {
    let tag = match &reply {
        redis::Value::Okay => Some("OK"),
        redis::Value::SimpleString(value) => Some(value.as_str()),
        redis::Value::BulkString(value) => std::str::from_utf8(value).ok(),
        _ => None,
    };
    if tag == Some("OK") {
        return Ok(());
    }
    // 只公开协议固定标签，损坏数据或代理回包不能把任意正文带入初始化错误。
    let reason = tag
        .filter(|reason| {
            matches!(
                *reason,
                "INVALID_INPUT"
                    | "CORRUPT"
                    | "REJECT_OLD_OWNER"
                    | "REJECT_HAS_COUNTERS"
                    | "REJECT_READY"
                    | "ROUND_MISMATCH"
                    | "NONCE_MISMATCH"
            )
        })
        .unwrap_or("INVALID_RESPONSE");
    Err(NasaRedisError::ProtocolMarker(format!(
        "fence 状态推进被拒: {reason}"
    )))
}

/// 仅从不存在或完整的 Initializing 状态准备组；任何已有计数字段都禁止重建未就绪组。
/// round 按完整十进制比较，Ready 只接受同轮幂等读取，字段残缺不得猜测或覆盖。
const TAG_INIT_LUA: &str = r#"
-- 输入不可表示为协议数字时不得创建任何持久状态。
if #ARGV ~= 2 then
    return 'INVALID_INPUT'
end
for _, value in ipairs({ARGV[1], ARGV[2]}) do
    if string.match(value, '^%d+$') == nil then
        return 'INVALID_INPUT'
    end
    local normalized = string.gsub(value, '^0+', '')
    if string.len(normalized) > 20 or
        (string.len(normalized) == 20 and normalized > '18446744073709551615') then
        return 'INVALID_INPUT'
    end
end
if string.sub(ARGV[1], 1, 1) == '0' then
    return 'INVALID_INPUT'
end
if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('HSET', KEYS[1], 'state', 'Initializing', 'round', ARGV[1], 'epoch', ARGV[2])
    return 'OK'
end
local state = redis.call('HGET', KEYS[1], 'state')
local round = redis.call('HGET', KEYS[1], 'round')
local epoch = redis.call('HGET', KEYS[1], 'epoch')
local nonce = redis.call('HGET', KEYS[1], 'nonce')
-- 存在 HASH 却缺身份不能解释为新组，否则会掩盖既有任期或改变恢复依据。
if state ~= 'Initializing' and state ~= 'Ready' then
    return 'CORRUPT'
end
for _, value in ipairs({round, epoch}) do
    if value == false or string.match(value, '^%d+$') == nil then
        return 'CORRUPT'
    end
    local normalized = string.gsub(value, '^0+', '')
    if string.len(normalized) > 20 or
        (string.len(normalized) == 20 and normalized > '18446744073709551615') then
        return 'CORRUPT'
    end
end
if string.sub(round, 1, 1) == '0' or
    (state == 'Ready' and (nonce == false or nonce == '')) or
    (state == 'Initializing' and nonce ~= false and nonce ~= '') then
    return 'CORRUPT'
end
-- 先按位数再按字典序比较，不能把相邻大整数折叠成同一轮次。
local input = ARGV[1]
if string.len(round) > string.len(input) or
    (string.len(round) == string.len(input) and round > input) then
    return 'REJECT_OLD_OWNER'
end
if state == 'Ready' then
    if round == input then
        return 'OK'
    end
    return 'REJECT_READY'
end
-- 未就绪却已有任期表示状态不一致；即使同轮重入也不能继续激活或清空计数。
for _, key in ipairs(redis.call('HKEYS', KEYS[1])) do
    if string.sub(key, 1, 2) == 'c:' then
        return 'REJECT_HAS_COUNTERS'
    end
end
if round == input then
    return 'OK'
end
-- 仅未发出任期的旧 Initializing 可以重建，已就绪凭据不会经过这条路径。
redis.call('DEL', KEYS[1])
redis.call('HSET', KEYS[1], 'state', 'Initializing', 'round', ARGV[1], 'epoch', ARGV[2])
return 'OK'
"#;

/// 完整的同轮 Initializing 才能激活；Ready 只接受相同 nonce 的只读幂等重入。
const TAG_ACTIVATE_LUA: &str = r#"
-- 激活身份必须完整，错误参数不得写成可获取任期的 Ready 状态。
if #ARGV ~= 2 or ARGV[2] == '' or string.match(ARGV[1], '^%d+$') == nil or
    string.sub(ARGV[1], 1, 1) == '0' or string.len(ARGV[1]) > 20 or
    (string.len(ARGV[1]) == 20 and ARGV[1] > '18446744073709551615') then
    return 'INVALID_INPUT'
end
local state = redis.call('HGET', KEYS[1], 'state')
local round = redis.call('HGET', KEYS[1], 'round')
local epoch = redis.call('HGET', KEYS[1], 'epoch')
local nonce = redis.call('HGET', KEYS[1], 'nonce')
-- 不能利用激活覆盖残缺元数据，否则后续 owner 无法判断既有凭据的归属。
if state ~= 'Initializing' and state ~= 'Ready' then
    return 'CORRUPT'
end
for _, value in ipairs({round, epoch}) do
    if value == false or string.match(value, '^%d+$') == nil then
        return 'CORRUPT'
    end
    local normalized = string.gsub(value, '^0+', '')
    if string.len(normalized) > 20 or
        (string.len(normalized) == 20 and normalized > '18446744073709551615') then
        return 'CORRUPT'
    end
end
if string.sub(round, 1, 1) == '0' or
    (state == 'Ready' and (nonce == false or nonce == '')) or
    (state == 'Initializing' and nonce ~= false and nonce ~= '') then
    return 'CORRUPT'
end
if round ~= ARGV[1] then
    return 'ROUND_MISMATCH'
end
-- 已发布的 nonce 不可替换；迟到初始化者只能沿用同一激活身份。
if state == 'Ready' then
    if nonce ~= ARGV[2] then
        return 'NONCE_MISMATCH'
    end
    return 'OK'
end
for _, key in ipairs(redis.call('HKEYS', KEYS[1])) do
    if string.sub(key, 1, 2) == 'c:' then
        return 'REJECT_HAS_COUNTERS'
    end
end
-- 元数据完整且从未发出任期时，一次原子写入发布组就绪身份。
redis.call('HSET', KEYS[1], 'state', 'Ready', 'nonce', ARGV[2])
return 'OK'
"#;

/// 取得任期前验证组身份和 epoch，递增后直接读取十进制计数，避免 Lua 数值转换损失精度。
const ACQUIRE_STAMP_LUA: &str = r#"
if redis.call('HGET', KEYS[1], 'state') ~= 'Ready' then
    return {'FENCE_NOT_READY'}
end
if redis.call('HGET', KEYS[1], 'round') ~= ARGV[1] then
    return {'ROUND_MISMATCH'}
end
if redis.call('HGET', KEYS[1], 'nonce') ~= ARGV[2] then
    return {'NONCE_MISMATCH'}
end
local epoch = redis.call('HGET', KEYS[1], 'epoch')
-- 无法建立完整凭据时不得先推进任期；十进制比较保持 u64 全宽精度。
if epoch == false or string.match(epoch, '^%d+$') == nil then
    return {'INVALID_EPOCH'}
end
local normalized_epoch = string.gsub(epoch, '^0+', '')
if string.len(normalized_epoch) > 20 or
    (string.len(normalized_epoch) == 20 and normalized_epoch > '18446744073709551615') then
    return {'INVALID_EPOCH'}
end
local field = 'c:' .. ARGV[3]
local previous = redis.call('HGET', KEYS[1], field)
-- 负计数表示持久权威已损坏，递增不能把它重新解释成合法任期。
if previous ~= false and string.sub(previous, 1, 1) == '-' then
    return {'INVALID_COUNTER'}
end
redis.call('HINCRBY', KEYS[1], field, 1)
return {'OK', epoch, redis.call('HGET', KEYS[1], field)}
"#;

/// 原子复验锁 holder、组激活身份、任期计数与 epoch 后确认记录。
/// KEYS 依次为锁、Stream、fence；ARGV 依次为 holder、group、round、nonce、p、counter、epoch 和完整 ID 列表。
const FENCED_ACK_LUA: &str = r#"
if redis.call('hexists', KEYS[1], ARGV[1]) == 0 then
    return 'NOT_HOLDER'
end
if redis.call('HGET', KEYS[3], 'state') ~= 'Ready' then
    return 'FENCE_NOT_READY'
end
if redis.call('HGET', KEYS[3], 'round') ~= ARGV[3] then
    return 'ROUND_MISMATCH'
end
if redis.call('HGET', KEYS[3], 'nonce') ~= ARGV[4] then
    return 'NONCE_MISMATCH'
end
local cur = redis.call('HGET', KEYS[3], 'c:' .. ARGV[5])
if cur ~= ARGV[6] then
    return 'STALE_STAMP'
end
-- epoch 与计数共同组成凭据；缺失、损坏或其它 epoch 都不能在当前消息上行使确认权。
local epoch = redis.call('HGET', KEYS[3], 'epoch')
if epoch == false or string.match(epoch, '^%d+$') == nil then
    return 'EPOCH_MISMATCH'
end
local normalized_epoch = string.gsub(epoch, '^0+', '')
if normalized_epoch == '' then
    normalized_epoch = '0'
end
if normalized_epoch ~= ARGV[7] then
    return 'EPOCH_MISMATCH'
end
local n = 0
for i = 8, #ARGV do
    n = n + redis.call('XACK', KEYS[2], ARGV[2], ARGV[i])
end
return n
"#;

/// 业务作用：复用已就绪的组凭据，或取得初始化租约后为当前物理组推进 fencing 准备。
///
/// 参数说明：
/// - `client`: 提供命名空间配置并执行初始化命令的共享 Redis 客户端。
/// - `layout`: 当前物理分区组布局，用于定位该组独立的 fence HASH。
/// - `epoch`: 新建组元数据时写入的 epoch；已存在的就绪组读取其持久 epoch。
///
/// 返回：当前组通过就绪检查后返回 round、nonce 与 epoch；协议或网络错误返回失败。
/// 未取得租约时按固定间隔等待，等待上限不包含正在进行的网络请求；租约仅在获取时证明持有，
/// 后续全局发布通过 marker 比较更新约束迟到写入，调用被取消时租约依靠 TTL 回收。
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
        // 已完成的全局状态允许直接读取本组凭据，但损坏 marker 不能绕过身份校验。
        if let Some(raw) = read_marker(client, &bkey).await? {
            let marker = parse_bootstrap_marker(&raw)?;
            if marker.state == "Ready" {
                match verify_tag_ready(client, &fkey, marker.round, &marker.nonce).await {
                    Ok(epoch_now) => {
                        return Ok(FenceMeta {
                            round: marker.round,
                            nonce: marker.nonce,
                            epoch: epoch_now,
                        });
                    }
                    // 新组或未完成的组只能进入带完整校验的补建脚本；残缺 HASH 仍会被拒绝。
                    Err(NasaRedisError::ProtocolMarker(_)) => {}
                    // 网络故障不能解释成组未创建，否则会在无法确认状态时发起新的写入。
                    Err(error) => return Err(error),
                }
            }
        }

        // 租约用于协调初始化频率，精确成功响应才允许推进，token 用于防止释放后来者的租约。
        let token = uuid::Uuid::new_v4().simple().to_string();
        let reply: redis::Value = redis::cmd("SET")
            .arg(&okey)
            .arg(&token)
            .arg("NX")
            .arg("PX")
            .arg(OWNER_LEASE_TTL_MS)
            .query_async(&mut client.conn())
            .await?;
        let acquired = match reply {
            redis::Value::Okay => true,
            redis::Value::SimpleString(value) if value == "OK" => true,
            redis::Value::BulkString(value) if value == b"OK" => true,
            redis::Value::Nil => false,
            _ => {
                return Err(NasaRedisError::ProtocolMarker(
                    "bootstrap owner 租约响应非法".into(),
                ));
            }
        };
        if acquired {
            // 无论推进成功还是失败都按 token 释放；已到期并被替换的租约绝不删除。
            let r = bootstrap_as_owner(client, &bkey, &fkey, epoch).await;
            let _: std::result::Result<i64, _> = redis::Script::new(RELEASE_OWNER_LUA)
                .key(&okey)
                .arg(&token)
                .invoke_async(&mut client.conn())
                .await;
            return r;
        }

        // 未取得租约时不争抢组写入，按截止和固定间隔等待持有者完成或租约到期。
        if tokio::time::Instant::now() >= deadline {
            return Err(NasaRedisError::ProtocolMarker(
                "bootstrap 等待全局 Ready 超时(owner 持续未完成)".into(),
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(WAIT_POLL_MS)).await;
    }
}

/// 业务作用：在调用方取得初始化租约后推进当前组准备，并沿用全局已有轮次与激活身份。
///
/// 参数说明：
/// - `client`: 执行 marker 与组 HASH 操作的共享 Redis 客户端。
/// - `bkey`: 当前命名空间的全局 bootstrap marker 键。
/// - `fkey`: 本次准备的物理分区组 fence HASH 键。
/// - `epoch`: 新建组 HASH 的候选 epoch，恢复已有组时沿用其持久 epoch。
///
/// 返回：已有 Ready 标记时补建当前组并读取持久 epoch；否则依次推进组初始化、
/// 全局 Activating、组激活与全局 Ready，再返回持久凭据；每次全局发布都比较完整旧标记。
/// 任一步失败立即停止推进并保留已发生的持久副作用，后续初始化者可沿已提交身份继续处理。
async fn bootstrap_as_owner(
    client: &Arc<RedisClient>,
    bkey: &str,
    fkey: &str,
    epoch: u64,
) -> Result<FenceMeta> {
    // 租约可能在网络等待时到期；保留准确原文，后续写入只能基于仍未被接管者改变的状态。
    let (mut marker, mut raw, fresh_created) = match read_marker(client, bkey).await? {
        Some(raw) => {
            let marker = parse_bootstrap_marker(&raw)?;
            (marker, raw, false)
        }
        None => {
            let fresh = BootstrapMarker {
                state: "Initializing".into(),
                round: 1,
                owner_term: 1,
                nonce: String::new(),
            };
            let payload = serde_json::to_string(&fresh)
                .map_err(|_| NasaRedisError::Codec("bootstrap marker 编码失败".into()))?;
            let reply: redis::Value = redis::cmd("SET")
                .arg(bkey)
                .arg(&payload)
                .arg("NX")
                .query_async(&mut client.conn())
                .await?;
            let created = match reply {
                redis::Value::Okay => true,
                redis::Value::SimpleString(value) if value == "OK" => true,
                redis::Value::BulkString(value) if value == b"OK" => true,
                redis::Value::Nil => false,
                _ => {
                    return Err(NasaRedisError::ProtocolMarker(
                        "bootstrap marker 创建响应非法".into(),
                    ));
                }
            };
            if created {
                (fresh, payload, true)
            } else {
                // 创建期间允许租约到期并发生并发初始化，必须重新读取真实持久身份。
                let raw = read_marker(client, bkey).await?.ok_or_else(|| {
                    NasaRedisError::ProtocolMarker("bootstrap marker 创建竞争后缺失".into())
                })?;
                let marker = parse_bootstrap_marker(&raw)?;
                (marker, raw, false)
            }
        }
    };

    // 全局已经就绪时只补建或验证当前组，不改变全局身份和接管次数。
    if marker.state == "Ready" {
        let epoch_now = seed_tag(client, fkey, marker.round, &marker.nonce, epoch).await?;
        return Ok(FenceMeta {
            round: marker.round,
            nonce: marker.nonce,
            epoch: epoch_now,
        });
    }

    // 非就绪标记的接管先以检查加法取得新任期，再比较发布；耗尽或失去当前状态即停止。
    if !fresh_created {
        marker.owner_term = marker
            .owner_term
            .checked_add(1)
            .ok_or_else(|| NasaRedisError::ProtocolMarker("bootstrap owner_term 已耗尽".into()))?;
        raw = compare_marker(client, bkey, &raw, &marker).await?;
        tracing::warn!(
            round = marker.round,
            owner_term = marker.owner_term,
            state = %marker.state,
            "接管半完成 bootstrap(上任 owner 未完成)"
        );
    }

    // 组元数据必须先完整准备，才允许全局进入激活阶段；已有 epoch 不用调用参数替换。
    let reply = redis::Script::new(TAG_INIT_LUA)
        .key(fkey)
        .arg(marker.round)
        .arg(epoch)
        .invoke_async(&mut client.conn())
        .await?;
    check_tag_reply(reply)?;

    // nonce 先经全局比较提交再用于组激活；竞争失败者不能发布自己生成的另一身份。
    if marker.state == "Initializing" {
        marker.state = "Activating".into();
        marker.nonce = uuid::Uuid::new_v4().simple().to_string();
        raw = compare_marker(client, bkey, &raw, &marker).await?;
    }

    // 已进入 Activating 的接管者沿用已提交 nonce；组脚本同时禁止改写已有 Ready 身份。
    let reply = redis::Script::new(TAG_ACTIVATE_LUA)
        .key(fkey)
        .arg(marker.round)
        .arg(&marker.nonce)
        .invoke_async(&mut client.conn())
        .await?;
    check_tag_reply(reply)?;

    // 当前组身份完整后才发布全局 Ready；最终比较失败不能覆盖接管者，返回值也必须取持久 epoch。
    let epoch_now = verify_tag_ready(client, fkey, marker.round, &marker.nonce).await?;
    marker.state = "Ready".into();
    compare_marker(client, bkey, &raw, &marker).await?;
    tracing::info!(
        round = marker.round,
        owner_term = marker.owner_term,
        "fencing bootstrap 完成(全局 Ready)"
    );
    Ok(FenceMeta {
        round: marker.round,
        nonce: marker.nonce,
        epoch: epoch_now,
    })
}

/// 业务作用：从同一次 Redis 读取确认当前组的就绪身份，并取得持久 epoch。
///
/// 参数说明：
/// - `client`: 执行组 HASH 读取的共享 Redis 客户端。
/// - `fkey`: 当前物理分区组的 fence HASH 键。
/// - `round`: 全局 marker 所声明的预期初始化轮次。
/// - `nonce`: 全局 marker 所声明的预期激活身份。
///
/// 返回：四个字段完整、身份一致且 epoch 可表示时返回持久 epoch；缺失、损坏或网络错误时失败。
/// 此检查不授予写入权，实际 acquire 与 ACK 仍须在 Lua 中重新检查当前状态。
async fn verify_tag_ready(
    client: &Arc<RedisClient>,
    fkey: &str,
    round: u64,
    nonce: &str,
) -> Result<u64> {
    // 就绪身份和 epoch 必须来自同一快照，不能拼接不同命令之间发生变化的元数据。
    let response: redis::Value = redis::cmd("HMGET")
        .arg(fkey)
        .arg(&["state", "round", "nonce", "epoch"])
        .query_async(&mut client.conn())
        .await?;
    let invalid = || NasaRedisError::ProtocolMarker("fence tag 未就绪或元数据非法".into());
    let redis::Value::Array(fields) = response else {
        return Err(invalid());
    };
    let [state, stored_round, stored_nonce, epoch] = fields.as_slice() else {
        return Err(invalid());
    };
    let (
        redis::Value::BulkString(state),
        redis::Value::BulkString(stored_round),
        redis::Value::BulkString(stored_nonce),
        redis::Value::BulkString(epoch),
    ) = (state, stored_round, stored_nonce, epoch)
    else {
        return Err(invalid());
    };
    // 空 nonce、零轮次或非规范轮次不能形成凭据；epoch 保留合法前导零，但拒绝符号和越界。
    if round == 0
        || nonce.is_empty()
        || state != b"Ready"
        || *stored_round != round.to_string().as_bytes()
        || stored_nonce != nonce.as_bytes()
        || epoch.is_empty()
        || !epoch.iter().all(u8::is_ascii_digit)
    {
        return Err(invalid());
    }
    std::str::from_utf8(epoch)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(invalid)
}

/// 业务作用：在全局 marker 已 Ready 时为当前物理组补建 fence，保持全局已有激活身份。
///
/// 参数说明：
/// - `client`: 执行初始化、激活和就绪读取的共享 Redis 客户端。
/// - `fkey`: 待补建物理组的 fence HASH 键。
/// - `round`: 从全局就绪 marker 取得的现有轮次，不为新增组另起轮次。
/// - `nonce`: 从同一全局 marker 取得的激活身份。
/// - `epoch`: 新建组 HASH 时写入的候选 epoch。
///
/// 返回：两步 Lua 均接受且原子读到 Ready 与预期身份时返回持久 epoch；否则返回协议或网络错误，
/// 已完成的组写入不回滚，也不改写全局 marker；调用方负责先取得初始化租约。
async fn seed_tag(
    client: &Arc<RedisClient>,
    fkey: &str,
    round: u64,
    nonce: &str,
    epoch: u64,
) -> Result<u64> {
    // 组状态只沿全局已有身份准备，异常回包不能作为继续激活的证据。
    let reply = redis::Script::new(TAG_INIT_LUA)
        .key(fkey)
        .arg(round)
        .arg(epoch)
        .invoke_async(&mut client.conn())
        .await?;
    check_tag_reply(reply)?;
    // 已有 Ready 只允许同 nonce 重入，新增组则在完整 Initializing 上原子发布身份。
    let reply = redis::Script::new(TAG_ACTIVATE_LUA)
        .key(fkey)
        .arg(round)
        .arg(nonce)
        .invoke_async(&mut client.conn())
        .await?;
    check_tag_reply(reply)?;
    verify_tag_ready(client, fkey, round, nonce).await
}

/// 业务作用：获取分区任期 stamp(锁 acquire 成功后调用;新任期使旧 stamp 失效)。
///
/// 参数说明：
/// - `client`: 执行 HINCRBY 任期递增的 Redis 客户端。
/// - `layout`: 当前物理分区组布局，决定任期计数所在的 fence HASH。
/// - `meta`: bootstrap 返回的轮次与 nonce，Lua 在递增前复验这两个字段及 Ready 状态。
/// - `p`: 已由调用方取得分区锁的分区编号，计数与其它分区相互独立。
///
/// 返回：成功响应恰含状态、ASCII 十进制 epoch 与正的有符号 64 位计数时创建 stamp；
/// 协议拒绝、非法响应或 Redis 错误时失败。调用方仍须维持锁持有权，失败也不回退已发生的计数递增。
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
        Some(redis::Value::BulkString(bytes)) => std::str::from_utf8(bytes).ok(),
        Some(redis::Value::SimpleString(value)) => Some(value.as_str()),
        Some(redis::Value::Okay) => Some("OK"),
        _ => None,
    };
    if tag != Some("OK") {
        // 服务端回包不是可信日志正文，只暴露已知拒绝标签，不把未知文本或二进制带入错误摘要。
        let reason = tag
            .filter(|reason| {
                matches!(
                    *reason,
                    "FENCE_NOT_READY"
                        | "ROUND_MISMATCH"
                        | "NONCE_MISMATCH"
                        | "INVALID_EPOCH"
                        | "INVALID_COUNTER"
                )
            })
            .unwrap_or("INVALID_RESPONSE");
        return Err(NasaRedisError::ProtocolMarker(format!(
            "acquire_stamp 被拒: {reason}"
        )));
    }
    // 凭据必须完整且没有多余字段；带符号、缺失或越界数值不能经宽松解析转成可用权威。
    if arr.len() != 3 {
        return Err(NasaRedisError::ProtocolMarker(
            "acquire_stamp 响应字段数异常".into(),
        ));
    }
    let number = |value: &redis::Value| -> Option<u64> {
        let redis::Value::BulkString(bytes) = value else {
            return None;
        };
        if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
            return None;
        }
        std::str::from_utf8(bytes).ok()?.parse().ok()
    };
    let epoch = number(&arr[1])
        .ok_or_else(|| NasaRedisError::ProtocolMarker("fence epoch 字段损坏".into()))?;
    let counter = number(&arr[2])
        .filter(|counter| *counter > 0 && *counter <= i64::MAX as u64)
        .ok_or_else(|| NasaRedisError::ProtocolMarker("fence counter 字段损坏或越界".into()))?;
    Ok(FencingStamp {
        p,
        epoch,
        counter,
    })
}

/// fenced ACK 的结果。
#[derive(Debug)]
pub enum FencedAck {
    /// 已原子 ACK 的条数。
    Acked(i64),
    /// 任期已被新 owner 取代 / 不再持锁 / fence 失配——一律不 ACK(调用方按 Lost 处理)。
    Rejected(String),
}

/// 业务作用：在单次 Lua 内复验锁 holder、fence 身份与分区任期后确认已处理的消息。
///
/// 参数说明：
/// - `client`: 执行 Lua ACK 的 Redis 客户端。
/// - `layout`: 当前组的物理布局，Stream、消费组及 fence 均从同一布局推导。
/// - `meta`: bootstrap 返回的 round、nonce 与 epoch，确认前必须仍与持久状态一致。
/// - `stamp`: 当前持锁者取得的分区编号、epoch 与任期计数，不能跨分区或 epoch 复用。
/// - `lock_full_key`: 与当前分区对应的 DistributedLock 完整 Redis 键。
/// - `holder`: 需要在锁 HASH 中仍然存在的当前持锁者 token。
/// - `ids`: 已处理记录的完整 Stream ID 列表，每项必须由两个 u64 十进制分量组成；空列表也执行权威检查。
///
/// 返回：零到请求条数之间的整数回包转换为 Acked，已知权威拒绝标签转换为 Rejected；
/// 非法计数、未知回包、网络或脚本错误返回失败。只有脚本内全部权威检查通过才执行 XACK，
/// 不主动释放分区锁或删除消息正文，也不把原始异常回包带入协议错误摘要。
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
    // 先拒绝本地不完整的权威和整批非法 ID，避免 Lua 确认前缀后才遇到后段参数错误而留下部分确认。
    if stamp.counter == 0 || stamp.counter > i64::MAX as u64 || stamp.epoch != meta.epoch {
        return Err(NasaRedisError::ProtocolMarker(
            "fenced ACK 任期计数或 epoch 不合法".into(),
        ));
    }
    let complete_ids = ids.iter().all(|id| {
        let Some((millis, sequence)) = id.split_once('-') else {
            return false;
        };
        !millis.is_empty()
            && !sequence.is_empty()
            && millis.bytes().all(|byte| byte.is_ascii_digit())
            && sequence.bytes().all(|byte| byte.is_ascii_digit())
            && millis.parse::<u64>().is_ok()
            && sequence.parse::<u64>().is_ok()
    });
    if !complete_ids {
        return Err(NasaRedisError::ProtocolMarker(
            "fenced ACK 记录 ID 不完整或越界".into(),
        ));
    }
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
        .arg(stamp.counter)
        .arg(stamp.epoch);
    for id in ids {
        inv.arg(id.as_str());
    }
    let v: redis::Value = inv.invoke_async(&mut client.conn()).await?;
    let reason = match &v {
        redis::Value::Int(n) => {
            // 确认数只能覆盖本次提交的 ID，异常整数不能成为解除消息责任的成功证据。
            if usize::try_from(*n).is_ok_and(|count| count <= ids.len()) {
                return Ok(FencedAck::Acked(*n));
            }
            return Err(NasaRedisError::ProtocolMarker(
                "fenced ACK 确认数超出请求范围".into(),
            ));
        }
        redis::Value::BulkString(bytes) => std::str::from_utf8(bytes).ok(),
        redis::Value::SimpleString(value) => Some(value.as_str()),
        _ => None,
    };
    // 只有协议定义的标签能证明失去权威；未知数据属于响应异常，不能冒充某个拒绝分支或泄露正文。
    match reason {
        Some(
            reason @ ("NOT_HOLDER" | "FENCE_NOT_READY" | "ROUND_MISMATCH" | "NONCE_MISMATCH"
            | "STALE_STAMP" | "EPOCH_MISMATCH"),
        ) => Ok(FencedAck::Rejected(reason.to_owned())),
        _ => Err(NasaRedisError::ProtocolMarker(
            "fenced ACK 响应形态或拒绝标签异常".into(),
        )),
    }
}
