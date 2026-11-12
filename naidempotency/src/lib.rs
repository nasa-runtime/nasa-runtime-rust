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
    /// 业务作用：构造仅描述 store 故障的错误，保留业务裁决与持久层失败的区分。
    ///
    /// 参数说明：
    /// - `reason`：不含凭据、请求体或后端敏感信息的原因文本。
    ///
    /// 返回：返回保存原文的错误，不自动脱敏或验证原因，调用方必须只传入可公开的稳定说明。
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for IdempotencyError {
    /// 业务作用：按固定 store 错误前缀展示调用方提供的原因。
    ///
    /// 参数说明：
    /// - `formatter`：用于写入稳定前缀与原因文本的格式器。
    ///
    /// 返回：格式器写入成功时成功，写入失败传播格式错误；输出原 reason，不能替代调用方的脱敏。
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
    ///
    /// 参数说明：
    /// - `key`：租户、主体、路由与客户端幂等键组成的完整命名空间。
    /// - `fingerprint`：本次请求等价类的固定指纹。
    /// - `lease`：调用方生成并在完成或放弃时原样携带的执行租约。
    ///
    /// 返回：不存在时建立 InFlight 并返回首次执行；相同指纹返回在途或完成重放，不同指纹返回冲突。后端故障返回错误，调用方应关闭请求；首次占位与业务副作用的原子性由具体 store 和事务装配决定。
    async fn begin(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
    ) -> Result<IdempotencyOutcome, IdempotencyError>;

    /// 业务作用：完成：仅当 fingerprint + lease 仍属于调用方时落为 Completed。返回是否成功持有并完成租约。
    ///
    /// 参数说明：
    /// - `key`：租户、主体、路由与客户端幂等键组成的完整命名空间。
    /// - `fingerprint`：本次请求等价类的固定指纹。
    /// - `lease`：调用方生成并在完成或放弃时原样携带的执行租约。
    /// - `response`：已由调用方限制大小与重放范围的响应副本。
    ///
    /// 返回：仍为同 fingerprint 与 lease 的 InFlight 时保存响应并返回 true，其它状态返回 false。存储故障返回错误；响应的大小、状态码与可重放 header 由调用方约束。
    async fn complete(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
        response: StoredResponse,
    ) -> Result<bool, IdempotencyError>;

    /// 业务作用：放弃仍属于调用方的 InFlight 占位。已完成、已换 owner 或不存在均返回 `false`。
    ///
    /// 参数说明：
    /// - `key`：租户、主体、路由与客户端幂等键组成的完整命名空间。
    /// - `fingerprint`：本次请求等价类的固定指纹。
    /// - `lease`：调用方生成并在完成或放弃时原样携带的执行租约。
    ///
    /// 返回：仅删除同 fingerprint 与 lease 的 InFlight，删除时 true，不存在、已完成或换 owner 时 false。存储故障返回错误，不代表业务副作用已经撤销。
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回空的进程内 store；没有 TTL、容量上限或重启后的重放保证。
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl IdempotencyStore for InMemoryIdempotencyStore {
    /// 业务作用：原子占位或读取现有进程内记录，区分首次、在途、重放与指纹冲突。
    ///
    /// 参数说明：
    /// - `key`：租户、主体、路由与客户端幂等键组成的完整命名空间。
    /// - `fingerprint`：本次请求等价类的固定指纹。
    /// - `lease`：调用方生成并在完成或放弃时原样携带的执行租约。
    ///
    /// 返回：不存在时建立 InFlight 并返回首次执行；相同指纹返回在途或完成重放，不同指纹返回冲突。锁中原子裁决，毒化锁仍恢复内部状态；本实现没有 TTL、清理或持久性，也不生成租约。
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
                records.insert(
                    key.clone(),
                    RecordState::InFlight {
                        fingerprint,
                        lease,
                    },
                );
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
    ///
    /// 参数说明：
    /// - `key`：租户、主体、路由与客户端幂等键组成的完整命名空间。
    /// - `fingerprint`：本次请求等价类的固定指纹。
    /// - `lease`：调用方生成并在完成或放弃时原样携带的执行租约。
    /// - `response`：已由调用方限制大小与重放范围的响应副本。
    ///
    /// 返回：仍为同 fingerprint 与 lease 的 InFlight 时保存响应并返回 true，其它状态返回 false。进程内锁中完成，不验证响应大小、状态码或 header 白名单。
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
    ///
    /// 参数说明：
    /// - `key`：租户、主体、路由与客户端幂等键组成的完整命名空间。
    /// - `fingerprint`：本次请求等价类的固定指纹。
    /// - `lease`：调用方生成并在完成或放弃时原样携带的执行租约。
    ///
    /// 返回：仅删除同 fingerprint 与 lease 的 InFlight，删除时 true，不存在、已完成或换 owner 时 false。本实现只改变进程内记录，不撤销业务副作用。
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
