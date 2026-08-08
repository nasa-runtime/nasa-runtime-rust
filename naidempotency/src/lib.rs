//! NASA 业务幂等核心。
//!
//! 实现 `Idempotency-Key` 工作草案(**非 RFC**)的核心正确性合同:首次执行、完成后重放、并发冲突、
//! 相同 key 不同 payload 指纹冲突。**不**由 Redis response cache 冒充 exactly-once——强资金幂等应把
//! 记录落到业务 DB 唯一键/同事务,此处提供 provider-neutral 状态机与进程内 store 供
//! 临时、非资金场景复用；持久化 DB/Redis store 实现同一 [`IdempotencyStore`] trait。
//!
//! key namespace 至少含 tenant、subject/client、route_id、client key;指纹取请求体等价类。

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Mutex;

/// 请求等价类的密码学指纹(SHA-256)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestFingerprint(pub [u8; 32]);

/// 一次 `begin` 占位的随机租约。完成/放弃必须同时匹配 fingerprint + lease，防止旧请求在
/// in-flight TTL 过期、另一请求重新占位后覆盖新 owner。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecutionLease(pub [u8; 16]);

/// 幂等 key 的命名空间:同一 (tenant, subject, route, client_key) 才视为同一幂等请求。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey {
    /// 租户。
    pub tenant: String,
    /// 主体 / client id。
    pub subject: String,
    /// 路由标识(编译期稳定 route_id)。
    pub route_id: String,
    /// 客户端提供的 `Idempotency-Key` 值。
    pub client_key: String,
}

/// 保存的响应(重放时原样返回);大小/状态白名单由调用方在写入前限制。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredResponse {
    /// HTTP 状态码。
    pub status: u16,
    /// 响应体字节(应受最大响应体上限约束)。
    pub body: Vec<u8>,
    /// 允许重放的有界响应 header 白名单。
    pub headers: Vec<StoredHeader>,
}

/// 可安全重放的单个响应 header。调用方只应存框架白名单内的低风险 header。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredHeader {
    /// 小写 header 名。
    pub name: String,
    /// 经 `HeaderValue::to_str` 验证的文本值。
    pub value: String,
}

/// 一次 `begin` 的裁决。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdempotencyOutcome {
    /// 首次:已占位 InFlight,调用方应执行业务并随后 `complete`。
    FirstExecution,
    /// 完成后重放:返回先前保存的结果(不再执行业务)。
    Replay(StoredResponse),
    /// 同 key、同指纹但仍在执行中:并发冲突(建议 409)。
    ConcurrentInFlight,
    /// 同 key、不同指纹:相同 key 不同 payload(建议 422)。
    FingerprintConflict,
}

/// 幂等记录状态。
#[derive(Debug, Clone)]
enum RecordState {
    InFlight {
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
    },
    Completed {
        fingerprint: RequestFingerprint,
        response: StoredResponse,
    },
}

/// store I/O 失败(不含业务裁决;仅底层持久层错误)。原因文本不得含凭据或请求体内容。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyError {
    /// 稳定、脱敏的失败原因(如 "database error" / "redis error")。
    pub reason: String,
}

impl IdempotencyError {
    /// 业务作用：用脱敏原因构造。
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for IdempotencyError {
    /// 业务作用：输出稳定脱敏原因，不包含 key、指纹、响应体或后端连接信息。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "idempotency store error: {}", self.reason)
    }
}

impl std::error::Error for IdempotencyError {}

/// provider-neutral 幂等 store。
///
/// 方法为 `async` 以容纳 DB/Redis 等异步后端;返回 `Result` 以显式区分**业务裁决**
/// ([`IdempotencyOutcome`])与**持久层故障**([`IdempotencyError`])。中间件对 `begin` 故障应
/// fail-closed(幂等不可用时不放行,以免重复副作用)。
#[async_trait::async_trait]
pub trait IdempotencyStore {
    /// 业务作用：尝试开始一次幂等请求,返回裁决(见 [`IdempotencyOutcome`])。
    async fn begin(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
    ) -> Result<IdempotencyOutcome, IdempotencyError>;

    /// 业务作用：完成：仅当 fingerprint + lease 仍属于调用方时落为 Completed。返回是否成功持有并完成租约。
    async fn complete(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
        response: StoredResponse,
    ) -> Result<bool, IdempotencyError>;

    /// 业务作用：放弃仍属于调用方的 InFlight 占位。已完成、已换 owner 或不存在均返回 `false`。
    async fn abort(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
    ) -> Result<bool, IdempotencyError>;
}

/// 进程内幂等 store，适用于允许进程重启后丢失重放记录的非持久场景。
#[derive(Default)]
pub struct InMemoryIdempotencyStore {
    records: Mutex<HashMap<IdempotencyKey, RecordState>>,
}

impl InMemoryIdempotencyStore {
    /// 业务作用：创建空 store。
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl IdempotencyStore for InMemoryIdempotencyStore {
    /// 业务作用：原子占位或读取现有进程内记录，区分首次、在途、重放与指纹冲突。
    async fn begin(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
    ) -> Result<IdempotencyOutcome, IdempotencyError> {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let outcome = match records.get(key) {
            None => {
                records.insert(key.clone(), RecordState::InFlight { fingerprint, lease });
                IdempotencyOutcome::FirstExecution
            }
            Some(RecordState::InFlight {
                fingerprint: existing,
                ..
            }) => {
                if *existing == fingerprint {
                    IdempotencyOutcome::ConcurrentInFlight
                } else {
                    IdempotencyOutcome::FingerprintConflict
                }
            }
            Some(RecordState::Completed {
                fingerprint: existing,
                response,
            }) => {
                if *existing == fingerprint {
                    IdempotencyOutcome::Replay(response.clone())
                } else {
                    IdempotencyOutcome::FingerprintConflict
                }
            }
        };
        Ok(outcome)
    }

    /// 业务作用：仅由仍持有 fingerprint 与 lease 的 owner 将在途记录转换为完成记录。
    async fn complete(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
        response: StoredResponse,
    ) -> Result<bool, IdempotencyError> {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(
            records.get(key),
            Some(RecordState::InFlight {
                fingerprint: existing,
                lease: existing_lease,
            }) if *existing == fingerprint && *existing_lease == lease
        ) {
            records.insert(
                key.clone(),
                RecordState::Completed {
                    fingerprint,
                    response,
                },
            );
            return Ok(true);
        }
        Ok(false)
    }

    /// 业务作用：仅删除仍属于调用方 lease 的在途占位，避免旧 owner 清除新请求。
    async fn abort(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
    ) -> Result<bool, IdempotencyError> {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let owned = matches!(
            records.get(key),
            Some(RecordState::InFlight {
                fingerprint: existing,
                lease: existing_lease,
            }) if *existing == fingerprint && *existing_lease == lease
        );
        if owned {
            records.remove(key);
        }
        Ok(owned)
    }
}
