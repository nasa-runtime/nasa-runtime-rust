//! 幂等 store 的 Redis 后端。
//!
//! 实现 [`naidempotency::IdempotencyStore`],**response-cache 语义**(:非资金/幂等重放场景;
//! 资金强幂等应落业务 DB 唯一键同事务,见 `naidempotency-mysql`)。
//!
//! - `begin`:`SET key <inflight> NX PX <ttl>` **原子占位**——成功=首次;已存在则 `GET` 现有值裁决
//!   重放/并发/指纹冲突。in-flight 键带较短 TTL,进程崩溃后自动过期使重试可继续。
//! - `complete`:二进制 `EVAL` **原子 check-and-set**——仅当现值仍 in-flight 才覆写为 completed(保留
//!   原 fingerprint),带较长 TTL 作重放窗口。避免 GET-then-SET 的覆写竞态。
//!
//! 所有键经 [`RedisClient::execute_raw`] 直发(不走 nadis namespace 前缀,SET/GET/EVAL 完全一致);
//! 底层错误一律脱敏为 [`IdempotencyError`](不回显命令/凭据/请求体)。
//!
//! 值编码(二进制,Redis 值二进制安全):in-flight 为
//! `[state:1][fingerprint:32][lease:16]`；completed 保存指纹与 status/header/body，响应上限由调用方约束。

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nadis::RedisClient;
use naidempotency::{
    ExecutionLease, IdempotencyError, IdempotencyKey, IdempotencyOutcome, IdempotencyStore,
    RequestFingerprint, StoredResponse,
};
use sha2::{Digest as _, Sha256};

/// 记录状态:进行中。
const STATE_IN_FLIGHT: u8 = 0;
/// 记录状态:已完成。
const STATE_COMPLETED: u8 = 1;

/// in-flight 占位默认 TTL(应大于最长请求处理时间;崩溃后自动过期解锁重试)。
const DEFAULT_INFLIGHT_TTL: Duration = Duration::from_secs(5 * 60);
/// completed 记录默认 TTL(重放窗口)。
const DEFAULT_COMPLETED_TTL: Duration = Duration::from_secs(24 * 3600);

/// complete 的原子 Lua：现值必须与调用方持有的完整 in-flight(fingerprint + lease)逐字节一致。
const COMPLETE_LUA: &str = "\
local v = redis.call('GET', KEYS[1]) \
if not v then return 0 end \
if v ~= ARGV[1] then return 0 end \
local fp = string.sub(v, 2, 33) \
redis.call('SET', KEYS[1], string.char(1) .. fp .. ARGV[2], 'PX', ARGV[3]) \
return 1";

/// 只删除仍属于调用方的 in-flight 租约。
const ABORT_LUA: &str = "\
local v = redis.call('GET', KEYS[1]) \
if not v or v ~= ARGV[1] then return 0 end \
return redis.call('DEL', KEYS[1])";

/// Redis 幂等 store。持注入的 [`RedisClient`];可配 in-flight / completed 两档 TTL。
#[derive(Clone)]
pub struct RedisIdempotencyStore {
    client: Arc<RedisClient>,
    inflight_ttl: Duration,
    completed_ttl: Duration,
}

impl RedisIdempotencyStore {
    /// 业务作用：用默认 TTL(in-flight 5min / completed 24h)构造。
    ///
    /// 参数说明：
    /// - `client`：用于直发幂等 SET、GET 和 EVAL 的共享 Redis 客户端。
    ///
    /// 返回：返回持有该客户端且使用默认两档 TTL 的 store，不发送 Redis 命令或验证连接。
    pub fn new(client: Arc<RedisClient>) -> Self {
        Self {
            client,
            inflight_ttl: DEFAULT_INFLIGHT_TTL,
            completed_ttl: DEFAULT_COMPLETED_TTL,
        }
    }

    /// 业务作用：用自定义 TTL 构造(in-flight 应大于最长处理时间;completed 为重放窗口)。
    ///
    /// 参数说明：
    /// - `client`：用于直发命令的共享 Redis 客户端。
    /// - `inflight_ttl`：在途占位过期时间，调用方应覆盖最长处理时间。
    /// - `completed_ttl`：完成响应的重放保留窗口。
    ///
    /// 返回：返回保存客户端与 TTL 原值的 store；构造时不校验 TTL，首次相关操作才验证可转换的毫秒范围。
    pub fn with_ttls(
        client: Arc<RedisClient>,
        inflight_ttl: Duration,
        completed_ttl: Duration,
    ) -> Self {
        Self {
            client,
            inflight_ttl,
            completed_ttl,
        }
    }

    /// 业务作用：按完整幂等命名空间构造 Redis 键；普通段采用 U+001F 分隔，含分隔符的输入采用长度定界摘要避免分段歧义。
    ///
    /// 参数说明：
    /// - `key`：需要隔离的租户、主体、路由与客户端幂等键。
    ///
    /// 返回：无分隔符时返回 idem: 与四段原文的分隔组合；任一段含 U+001F 时返回 idem:v2: 与带长度输入的 SHA-256 摘要。不会套用客户端 namespace，也不验证各段业务合法性。
    fn redis_key(key: &IdempotencyKey) -> String {
        const SEPARATOR: char = '\u{1f}';
        let components = [
            key.tenant.as_str(),
            key.subject.as_str(),
            key.route_id.as_str(),
            key.client_key.as_str(),
        ];
        if components
            .iter()
            .all(|component| !component.contains(SEPARATOR))
        {
            return format!(
                "idem:{}{SEPARATOR}{}{SEPARATOR}{}{SEPARATOR}{}",
                key.tenant, key.subject, key.route_id, key.client_key
            );
        }
        let mut digest = Sha256::new();
        for component in components.map(str::as_bytes) {
            digest.update((component.len() as u64).to_be_bytes());
            digest.update(component);
        }
        let digest = digest.finalize();
        let mut encoded = String::with_capacity("idem:v2:".len() + digest.len() * 2);
        encoded.push_str("idem:v2:");
        for byte in digest {
            use std::fmt::Write as _;
            let _ = write!(encoded, "{byte:02x}");
        }
        encoded
    }
}

/// 业务作用：把过期时长转为 Redis 正 i64 毫秒范围，拒绝零毫秒与数值溢出。
///
/// 参数说明：
/// - `ttl`：待写入 Redis PX 参数的持续时间。
///
/// 返回：向下取整为毫秒后处于 1..=i64::MAX 时成功；不足一毫秒、零或超出范围时报错。小于一毫秒的余数会舍弃。
fn ttl_millis(ttl: Duration) -> Result<u64, IdempotencyError> {
    let millis =
        u64::try_from(ttl.as_millis()).map_err(|_| IdempotencyError::new("invalid ttl"))?;
    if millis == 0 || millis > i64::MAX as u64 {
        return Err(IdempotencyError::new("invalid ttl"));
    }
    Ok(millis)
}

/// 业务作用：in-flight 值编码:`[0][fingerprint:32][lease:16]`。
///
/// 参数说明：
/// - `fingerprint`：本次请求的 32 字节等价类指纹。
/// - `lease`：调用方生成的 16 字节执行租约。
///
/// 返回：返回恰好 49 字节的状态、指纹与租约编码，不生成随机值或取得 Redis 占位。
fn encode_inflight(fingerprint: RequestFingerprint, lease: ExecutionLease) -> Vec<u8> {
    let mut value = Vec::with_capacity(49);
    value.push(STATE_IN_FLIGHT);
    value.extend_from_slice(&fingerprint.0);
    value.extend_from_slice(&lease.0);
    value
}

/// 业务作用：编码幂等请求已完成响应的二进制后缀，供状态记录持久保存。
///
/// 参数说明：
/// - `response`：需要重放的状态码、响应头和正文。
///
/// 返回：成功返回依次含 2 字节小端状态码、4 字节小端头部 JSON 长度、头部 JSON 与原正文的字节；头部序列化或长度超出 u32 时失败，不写入 Redis。
fn encode_completed_suffix(response: &StoredResponse) -> Result<Vec<u8>, IdempotencyError> {
    let headers = serde_json::to_vec(&response.headers).map_err(map_err)?;
    let header_len =
        u32::try_from(headers.len()).map_err(|_| IdempotencyError::new("headers too large"))?;
    let mut suffix = Vec::with_capacity(6 + headers.len() + response.body.len());
    suffix.extend_from_slice(&response.status.to_le_bytes());
    suffix.extend_from_slice(&header_len.to_le_bytes());
    suffix.extend_from_slice(&headers);
    suffix.extend_from_slice(&response.body);
    Ok(suffix)
}

/// 解出的现有记录。
struct Decoded {
    state: u8,
    fingerprint: RequestFingerprint,
    response: Option<StoredResponse>,
}

/// 业务作用：解码存储值;长度不足视为损坏返回 `None`。
///
/// 参数说明：
/// - `bytes`：Redis GET 得到的完整二进制记录。
///
/// 返回：有效在途长度或可解码完成状态时返回指纹与可选响应；未知状态、长度、状态码或头部 JSON 不合法时返回 None。无正文容量或 header 白名单检查，不验证租约内容。
fn decode(bytes: &[u8]) -> Option<Decoded> {
    if bytes.len() < 33 {
        return None;
    }
    let state = bytes[0];
    if !matches!(state, STATE_IN_FLIGHT | STATE_COMPLETED) {
        return None;
    }
    if state == STATE_IN_FLIGHT && bytes.len() != 49 {
        return None;
    }
    let fingerprint = RequestFingerprint(bytes[1..33].try_into().ok()?);
    let response = if state == STATE_COMPLETED {
        if bytes.len() < 39 {
            return None;
        }
        let status = u16::from_le_bytes(bytes[33..35].try_into().ok()?);
        if !(100..=599).contains(&status) {
            return None;
        }
        let headers_len = u32::from_le_bytes(bytes[35..39].try_into().ok()?) as usize;
        let body_start = 39usize.checked_add(headers_len)?;
        if body_start > bytes.len() {
            return None;
        }
        let headers = serde_json::from_slice(&bytes[39..body_start]).ok()?;
        Some(StoredResponse {
            status,
            body: bytes[body_start..].to_vec(),
            headers,
        })
    } else {
        None
    };
    Some(Decoded {
        state,
        fingerprint,
        response,
    })
}

#[async_trait]
impl IdempotencyStore for RedisIdempotencyStore {
    /// 业务作用：用 `SET NX PX` 原子占位；已有值按二进制状态裁决重放、在途或指纹冲突。
    ///
    /// 参数说明：
    /// - `key`：租户、主体、路由与客户端幂等键组成的完整命名空间。
    /// - `fingerprint`：本次请求等价类的固定指纹。
    /// - `lease`：调用方生成并在完成或放弃时原样携带的执行租约。
    ///
    /// 返回：占位成功返回首次执行；已有值按指纹与状态裁决，SET 与 GET 之间过期时保守返回在途。TTL、记录损坏或 I/O 失败返回错误，I/O 失败可能留下已执行的占位，不保证后端持久化或业务 exactly-once。
    async fn begin(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
    ) -> Result<IdempotencyOutcome, IdempotencyError> {
        let redis_key = Self::redis_key(key);

        // 原子占位:SET key <inflight> NX PX ttl。成功=首次。
        let mut set = redis::cmd("SET");
        set.arg(&redis_key)
            .arg(encode_inflight(fingerprint, lease).as_slice())
            .arg("NX")
            .arg("PX")
            .arg(ttl_millis(self.inflight_ttl)?);
        let claimed: Option<String> = self.client.execute_raw(&set).await.map_err(map_err)?;
        if claimed.is_some() {
            return Ok(IdempotencyOutcome::FirstExecution);
        }

        // 首次占位未取得时只能读取现值裁决；不从相同请求指纹推导当前执行权。
        let mut get = redis::cmd("GET");
        get.arg(&redis_key);
        let existing: Option<Vec<u8>> = self.client.execute_raw(&get).await.map_err(map_err)?;

        // SET-NX 与 GET 之间键过期(极罕见):保守判并发,让上层重试(下次占位即首次)。
        let Some(bytes) = existing else {
            return Ok(IdempotencyOutcome::ConcurrentInFlight);
        };
        let Some(decoded) = decode(&bytes) else {
            return Err(IdempotencyError::new("corrupt idempotency record"));
        };
        if decoded.fingerprint != fingerprint {
            return Ok(IdempotencyOutcome::FingerprintConflict);
        }
        if decoded.state == STATE_COMPLETED {
            let response = decoded
                .response
                .ok_or_else(|| IdempotencyError::new("corrupt idempotency record"))?;
            Ok(IdempotencyOutcome::Replay(response))
        } else {
            Ok(IdempotencyOutcome::ConcurrentInFlight)
        }
    }

    /// 业务作用：通过 Lua compare-and-set 将仍匹配 fingerprint/lease 的在途值替换为完成值。
    ///
    /// 参数说明：
    /// - `key`：租户、主体、路由与客户端幂等键组成的完整命名空间。
    /// - `fingerprint`：本次请求等价类的固定指纹。
    /// - `lease`：调用方生成并在完成或放弃时原样携带的执行租约。
    /// - `response`：需要保存重放的响应；大小、状态码和 header 白名单由调用方限制。
    ///
    /// 返回：原子比较整段 InFlight 编码后写入完成记录并重设 TTL，脚本返回 1 时 true，其它数值 false；编码、TTL 或 I/O 失败返回错误，I/O 失败不能证明远端未写入。
    async fn complete(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
        response: StoredResponse,
    ) -> Result<bool, IdempotencyError> {
        let redis_key = Self::redis_key(key);
        // 必须同时匹配指纹和租约，防止过期后重新占位的新请求被旧持有者覆盖。
        // 二进制 ARGV 经 execute_raw 直发(nadis eval 只收 &str,承载不了任意 body 字节)。
        let mut eval = redis::cmd("EVAL");
        eval.arg(COMPLETE_LUA)
            .arg(1)
            .arg(&redis_key)
            .arg(encode_inflight(fingerprint, lease).as_slice())
            .arg(encode_completed_suffix(&response)?.as_slice())
            .arg(ttl_millis(self.completed_ttl)?);
        let completed: i64 = self.client.execute_raw(&eval).await.map_err(map_err)?;
        Ok(completed == 1)
    }

    /// 业务作用：通过 Lua compare-and-delete 释放仍属于调用方的在途占位。
    ///
    /// 参数说明：
    /// - `key`：租户、主体、路由与客户端幂等键组成的完整命名空间。
    /// - `fingerprint`：本次请求等价类的固定指纹。
    /// - `lease`：调用方生成并在完成或放弃时原样携带的执行租约。
    ///
    /// 返回：脚本原子删除完全匹配的 InFlight，返回 1 时 true，其它数值 false；I/O 错误返回固定 store 错误，远端是否已删除可能未知，不撤销业务副作用。
    async fn abort(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
    ) -> Result<bool, IdempotencyError> {
        let redis_key = Self::redis_key(key);
        // 放弃只能删除本次完整租约仍持有的占位，不能清除重新占位者或完成响应。
        let mut eval = redis::cmd("EVAL");
        eval.arg(ABORT_LUA)
            .arg(1)
            .arg(&redis_key)
            .arg(encode_inflight(fingerprint, lease).as_slice());
        let deleted: i64 = self.client.execute_raw(&eval).await.map_err(map_err)?;
        Ok(deleted == 1)
    }
}

/// 业务作用：把任意底层错误映射为脱敏的 [`IdempotencyError`](绝不回显命令/凭据/请求体)。
///
/// 参数说明：
/// - `_error`：仅用于触发错误分类、不向公开错误转存的底层值。
///
/// 返回：丢弃输入错误并返回固定 redis error，不保留后端文本。
fn map_err<E>(_error: E) -> IdempotencyError {
    IdempotencyError::new("redis error")
}
