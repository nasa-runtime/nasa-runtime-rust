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
//! `[state:1][fingerprint:32][lease:16]`;completed 追加有界 status/header/body。

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
    pub fn new(client: Arc<RedisClient>) -> Self {
        Self {
            client,
            inflight_ttl: DEFAULT_INFLIGHT_TTL,
            completed_ttl: DEFAULT_COMPLETED_TTL,
        }
    }

    /// 业务作用：用自定义 TTL 构造(in-flight 应大于最长处理时间;completed 为重放窗口)。
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

    /// 业务作用：组合命名空间键。普通输入继续使用 v1 形式以保留滚动升级期间的已有 replay；只有任一段含旧
    /// 分隔符时才切换到长度定界的 v2 摘要，堵住分隔符注入碰撞且不打断正常历史记录。
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

/// 业务作用：将 TTL 转为 Redis 可接受的正 i64 毫秒范围，拒绝零值与截断。
fn ttl_millis(ttl: Duration) -> Result<u64, IdempotencyError> {
    let millis =
        u64::try_from(ttl.as_millis()).map_err(|_| IdempotencyError::new("invalid ttl"))?;
    if millis == 0 || millis > i64::MAX as u64 {
        return Err(IdempotencyError::new("invalid ttl"));
    }
    Ok(millis)
}

/// 业务作用：in-flight 值编码:`[0][fingerprint:32][lease:16]`。
fn encode_inflight(fingerprint: RequestFingerprint, lease: ExecutionLease) -> Vec<u8> {
    let mut value = Vec::with_capacity(49);
    value.push(STATE_IN_FLIGHT);
    value.extend_from_slice(&fingerprint.0);
    value.extend_from_slice(&lease.0);
    value
}

/// 业务作用：completed suffix:`[status:2 LE][headers-len:4 LE][headers-json][body]`。
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

        // 已存在:读现值裁决。
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
    async fn complete(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
        response: StoredResponse,
    ) -> Result<bool, IdempotencyError> {
        let redis_key = Self::redis_key(key);
        // 原子 check-and-set:仅当仍 in-flight 才覆写为 completed(保留 fingerprint);否则忽略。
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
    async fn abort(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
    ) -> Result<bool, IdempotencyError> {
        let redis_key = Self::redis_key(key);
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
fn map_err<E>(_error: E) -> IdempotencyError {
    IdempotencyError::new("redis error")
}
