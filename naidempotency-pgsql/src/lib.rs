//! PostgreSQL 幂等持久层。
//!
//! 本 crate 原样实现 `naidempotency::IdempotencyStore`，以目标复合主键裁决首次执行，以 fingerprint、
//! 随机 lease 与递增 generation 阻止过期 owner 覆盖新 owner。连接固定来自 `natx-pgsql` 命名
//! datasource；ambient transaction 内的记录与业务事实同提交。

#![forbid(unsafe_code)]

use async_trait::async_trait;
use naidempotency::{
    ExecutionLease, IdempotencyError, IdempotencyKey, IdempotencyOutcome, IdempotencyStore,
    RequestFingerprint, StoredResponse,
};
use natx_pgsql::{TxDecision, TxRunError};
use sqlx::Row as _;

const STATE_IN_FLIGHT: i16 = 0;
const STATE_COMPLETED: i16 = 1;
const LEASE_MILLIS: i64 = 5 * 60 * 1_000;

const CREATE_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS idempotency_record_v2 (
    tenant TEXT COLLATE "C" NOT NULL CHECK (octet_length(tenant) BETWEEN 1 AND 128),
    subject TEXT COLLATE "C" NOT NULL CHECK (octet_length(subject) BETWEEN 1 AND 190),
    route_id TEXT COLLATE "C" NOT NULL CHECK (octet_length(route_id) BETWEEN 1 AND 190),
    client_key TEXT COLLATE "C" NOT NULL CHECK (octet_length(client_key) BETWEEN 1 AND 190),
    fingerprint BYTEA NOT NULL CHECK (octet_length(fingerprint) = 32),
    lease BYTEA NOT NULL CHECK (octet_length(lease) = 16),
    generation BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
    state SMALLINT NOT NULL CHECK (state IN (0, 1)),
    status INTEGER CHECK (status BETWEEN 100 AND 599),
    body BYTEA,
    headers BYTEA,
    lease_expires_at_ms BIGINT NOT NULL CHECK (lease_expires_at_ms >= 0),
    created_at_ms BIGINT NOT NULL CHECK (created_at_ms >= 0),
    updated_at_ms BIGINT NOT NULL CHECK (updated_at_ms >= 0),
    CONSTRAINT idempotency_record_v2_pkey PRIMARY KEY (tenant, subject, route_id, client_key)
)"#;

/// PostgreSQL 幂等 store；句柄固定 datasource，事务内外都不会回退到其它连接源。
#[derive(Debug, Clone)]
pub struct PgIdempotencyStore {
    datasource: natx_pgsql::DatasourceRef,
}

impl Default for PgIdempotencyStore {
    /// 业务作用: 创建绑定默认 PostgreSQL datasource 的轻量幂等句柄。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 与 [`PgIdempotencyStore::new`] 相同的句柄。
    fn default() -> Self {
        Self::new()
    }
}

/// 幂等事务闭包的封闭结束阶段。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgIdempotencyTransactionError<E> {
    /// 业务闭包明确要求回滚，数据库未提交本轮事实。
    Business(E),
    /// 嵌套调用已经把外层事务标记为只能回滚。
    RollbackOnly,
    /// 数据库明确拒绝 COMMIT。
    CommitRejected,
    /// COMMIT 已发出但应答不确定，禁止重新执行业务闭包。
    OutcomeUnknown,
    /// 回滚本身失败，不能声称本轮没有副作用。
    RollbackFailed,
    /// 事务开始、连接或所有权基础设施失败。
    Infrastructure,
}

impl<E> PgIdempotencyTransactionError<E> {
    /// 业务作用: 判断提交结果是否不确定，供调用方选择持久事实重查而不是重执行业务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 仅 `OutcomeUnknown` 返回 `true`。
    pub fn is_outcome_unknown(&self) -> bool {
        matches!(self, Self::OutcomeUnknown)
    }
}

impl PgIdempotencyStore {
    /// 业务作用: 创建绑定默认 PostgreSQL datasource 的幂等 store，不提前建立连接。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 后续操作固定经 default registry 获取连接的轻量句柄。
    pub fn new() -> Self {
        Self {
            datasource: natx_pgsql::DatasourceRef::default(),
        }
    }

    /// 业务作用: 创建绑定命名 PostgreSQL datasource 的幂等 store。
    ///
    /// 参数说明：
    /// - `datasource`: 启动期已登记的稳定 datasource 名称。
    ///
    /// 返回: 名称合法时返回固定绑定句柄；非法名称在网络 I/O 前失败。
    pub fn with_datasource(datasource: impl AsRef<str>) -> Result<Self, IdempotencyError> {
        Ok(Self {
            datasource: natx_pgsql::DatasourceRef::new(datasource)
                .map_err(|_| IdempotencyError::new("invalid datasource name"))?,
        })
    }

    /// 业务作用: 返回当前 store 的不可变 PostgreSQL datasource 身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 不含 endpoint 或凭据的 qualifier 引用。
    pub fn datasource_ref(&self) -> &natx_pgsql::DatasourceRef {
        &self.datasource
    }

    /// 业务作用: 为显式自举创建默认 datasource 的幂等表。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 表已存在或创建成功时完成；连接与 DDL 失败返回脱敏错误。
    pub async fn ensure_schema() -> Result<(), IdempotencyError> {
        Self::ensure_schema_for(natx_pgsql::DEFAULT_DATASOURCE).await
    }

    /// 业务作用: 为显式自举在命名 datasource 创建幂等表。
    ///
    /// 参数说明：
    /// - `datasource`: 已登记的 PostgreSQL datasource 名称。
    ///
    /// 返回: 表已存在或创建成功时完成；名称、连接或 DDL 失败返回脱敏错误。
    pub async fn ensure_schema_for(datasource: impl AsRef<str>) -> Result<(), IdempotencyError> {
        let datasource = natx_pgsql::DatasourceRef::new(datasource).map_err(map_err)?;
        let mut connection = natx_pgsql::conn_for(&datasource).await.map_err(map_err)?;
        sqlx::raw_sql(CREATE_TABLE_SQL)
            .execute(connection.as_mut())
            .await
            .map_err(map_err)?;
        Ok(())
    }

    /// 业务作用: 在固定 datasource 上运行显式事务裁决，并保留提交不确定的独立分类。
    ///
    /// 参数说明：
    /// - `body`: 返回 `TxDecision` 的业务 future；只有 `Commit` 才请求数据库提交。
    ///
    /// 返回: 明确提交时返回业务值；回滚、提交拒绝、结果不确定或基础设施失败返回封闭阶段。
    pub async fn run_decided<T, E, F>(&self, body: F) -> Result<T, PgIdempotencyTransactionError<E>>
    where
        F: std::future::Future<Output = TxDecision<T, E>>,
    {
        natx_pgsql::run_decided_for(&self.datasource, body)
            .await
            .map_err(map_transaction_error)
    }

    /// 业务作用: 读取目标主键的既有记录并裁决重放、在途或指纹冲突。
    ///
    /// 参数说明：
    /// - `key`: 已通过应用侧字节边界校验的幂等复合身份。
    /// - `fingerprint`: 当前请求等价类指纹。
    ///
    /// 返回: 已完成同指纹记录可重放；在途与异指纹返回对应业务裁决；损坏记录返回错误。
    async fn decide_existing(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
    ) -> Result<IdempotencyOutcome, IdempotencyError> {
        let mut connection = natx_pgsql::conn_for(&self.datasource)
            .await
            .map_err(map_err)?;
        let row = sqlx::query(
            "SELECT state, fingerprint, status, body, headers FROM idempotency_record_v2 \
             WHERE tenant = $1 AND subject = $2 AND route_id = $3 AND client_key = $4",
        )
        .bind(&key.tenant)
        .bind(&key.subject)
        .bind(&key.route_id)
        .bind(&key.client_key)
        .fetch_optional(connection.as_mut())
        .await
        .map_err(map_err)?;

        let Some(row) = row else {
            return Ok(IdempotencyOutcome::ConcurrentInFlight);
        };
        let existing_fingerprint: Vec<u8> = row.try_get("fingerprint").map_err(map_err)?;
        if existing_fingerprint.as_slice() != fingerprint.0 {
            return Ok(IdempotencyOutcome::FingerprintConflict);
        }
        let state: i16 = row.try_get("state").map_err(map_err)?;
        if state == STATE_IN_FLIGHT {
            return Ok(IdempotencyOutcome::ConcurrentInFlight);
        }
        if state != STATE_COMPLETED {
            return Err(IdempotencyError::new("corrupt idempotency record"));
        }
        let status: Option<i32> = row.try_get("status").map_err(map_err)?;
        let status = status
            .and_then(|value| u16::try_from(value).ok())
            .filter(|value| (100..=599).contains(value))
            .ok_or_else(|| IdempotencyError::new("corrupt idempotency record"))?;
        let body: Option<Vec<u8>> = row.try_get("body").map_err(map_err)?;
        let headers: Option<Vec<u8>> = row.try_get("headers").map_err(map_err)?;
        Ok(IdempotencyOutcome::Replay(StoredResponse {
            status,
            body: body.ok_or_else(|| IdempotencyError::new("corrupt idempotency record"))?,
            headers: serde_json::from_slice(
                &headers.ok_or_else(|| IdempotencyError::new("corrupt idempotency record"))?,
            )
            .map_err(|_| IdempotencyError::new("corrupt idempotency record"))?,
        }))
    }
}

#[async_trait]
impl IdempotencyStore for PgIdempotencyStore {
    /// 业务作用: 原子竞争复合主键，并在租约过期时以新 lease 与递增 generation 接管。
    ///
    /// 参数说明：
    /// - `key`: tenant、subject、route 与 client key 组成的幂等身份。
    /// - `fingerprint`: 请求等价类 SHA-256 指纹。
    /// - `lease`: 当前执行者的随机 fencing lease。
    ///
    /// 返回: 首次/接管、重放、并发或指纹冲突裁决；非目标约束和数据库失败返回脱敏错误。
    async fn begin(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
    ) -> Result<IdempotencyOutcome, IdempotencyError> {
        validate_key(key)?;
        let now = database_epoch_ms_sql();
        let insert_sql = format!(
            "INSERT INTO idempotency_record_v2 \
             (tenant, subject, route_id, client_key, fingerprint, lease, generation, state, \
              status, body, headers, lease_expires_at_ms, created_at_ms, updated_at_ms) \
             VALUES ($1, $2, $3, $4, $5, $6, 1, $7, NULL, NULL, NULL, {now} + $8, {now}, {now}) \
             ON CONFLICT ON CONSTRAINT idempotency_record_v2_pkey DO NOTHING"
        );
        let mut connection = natx_pgsql::conn_for(&self.datasource)
            .await
            .map_err(map_err)?;
        let inserted = sqlx::query(sqlx::AssertSqlSafe(insert_sql))
            .bind(&key.tenant)
            .bind(&key.subject)
            .bind(&key.route_id)
            .bind(&key.client_key)
            .bind(fingerprint.0.as_slice())
            .bind(lease.0.as_slice())
            .bind(STATE_IN_FLIGHT)
            .bind(LEASE_MILLIS)
            .execute(connection.as_mut())
            .await
            .map_err(map_err)?;
        if inserted.rows_affected() == 1 {
            return Ok(IdempotencyOutcome::FirstExecution);
        }

        // client key 的请求等价类一经建立就不能被租约过期改写；只有相同 fingerprint 可以接管执行权。
        let takeover_sql = format!(
            "UPDATE idempotency_record_v2 SET fingerprint = $1, lease = $2, \
             generation = generation + 1, lease_expires_at_ms = {now} + $3, \
             updated_at_ms = {now}, status = NULL, body = NULL, headers = NULL \
             WHERE tenant = $4 AND subject = $5 AND route_id = $6 AND client_key = $7 \
             AND state = $8 AND fingerprint = $9 AND lease_expires_at_ms < {now}"
        );
        let takeover = sqlx::query(sqlx::AssertSqlSafe(takeover_sql))
            .bind(fingerprint.0.as_slice())
            .bind(lease.0.as_slice())
            .bind(LEASE_MILLIS)
            .bind(&key.tenant)
            .bind(&key.subject)
            .bind(&key.route_id)
            .bind(&key.client_key)
            .bind(STATE_IN_FLIGHT)
            .bind(fingerprint.0.as_slice())
            .execute(connection.as_mut())
            .await
            .map_err(map_err)?;
        drop(connection);
        if takeover.rows_affected() == 1 {
            Ok(IdempotencyOutcome::FirstExecution)
        } else {
            self.decide_existing(key, fingerprint).await
        }
    }

    /// 业务作用: 仅由仍持有 fingerprint 与 lease 的 owner 把在途记录转换为可重放完成态。
    ///
    /// 参数说明：
    /// - `key`: 幂等复合身份。
    /// - `fingerprint`: 当前请求指纹。
    /// - `lease`: begin 成功时取得的随机 owner lease。
    /// - `response`: 由治理层完成大小和 header 白名单校验的响应。
    ///
    /// 返回: 仍持权且完成成功时为 `true`；过期 owner 为 `false`；数据库失败返回脱敏错误。
    async fn complete(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
        response: StoredResponse,
    ) -> Result<bool, IdempotencyError> {
        validate_key(key)?;
        let headers = serde_json::to_vec(&response.headers).map_err(map_err)?;
        let now = database_epoch_ms_sql();
        let sql = format!(
            "UPDATE idempotency_record_v2 SET state = $1, status = $2, body = $3, headers = $4, \
             updated_at_ms = {now} WHERE tenant = $5 AND subject = $6 AND route_id = $7 \
             AND client_key = $8 AND state = $9 AND fingerprint = $10 AND lease = $11"
        );
        let mut connection = natx_pgsql::conn_for(&self.datasource)
            .await
            .map_err(map_err)?;
        // 状态、fingerprint 与 lease 必须同时命中，失权 owner 不能覆盖接管者或已完成响应。
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(STATE_COMPLETED)
            .bind(i32::from(response.status))
            .bind(&response.body)
            .bind(headers)
            .bind(&key.tenant)
            .bind(&key.subject)
            .bind(&key.route_id)
            .bind(&key.client_key)
            .bind(STATE_IN_FLIGHT)
            .bind(fingerprint.0.as_slice())
            .bind(lease.0.as_slice())
            .execute(connection.as_mut())
            .await
            .map(|result| result.rows_affected() == 1)
            .map_err(map_err)
    }

    /// 业务作用: 仅删除仍属于当前 fingerprint 与 lease 的在途记录，阻止旧 owner 清除新 owner。
    ///
    /// 参数说明：
    /// - `key`: 幂等复合身份。
    /// - `fingerprint`: 当前请求指纹。
    /// - `lease`: begin 成功时取得的随机 owner lease。
    ///
    /// 返回: 精确删除一行时为 `true`；记录已完成、失权或不存在为 `false`；数据库失败返回错误。
    async fn abort(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
    ) -> Result<bool, IdempotencyError> {
        validate_key(key)?;
        let mut connection = natx_pgsql::conn_for(&self.datasource)
            .await
            .map_err(map_err)?;
        // 删除同样携带状态、fingerprint 与 lease 门禁，避免旧 owner 清除新执行者的在途证据。
        sqlx::query(
            "DELETE FROM idempotency_record_v2 WHERE tenant = $1 AND subject = $2 \
             AND route_id = $3 AND client_key = $4 AND state = $5 \
             AND fingerprint = $6 AND lease = $7",
        )
        .bind(&key.tenant)
        .bind(&key.subject)
        .bind(&key.route_id)
        .bind(&key.client_key)
        .bind(STATE_IN_FLIGHT)
        .bind(fingerprint.0.as_slice())
        .bind(lease.0.as_slice())
        .execute(connection.as_mut())
        .await
        .map(|result| result.rows_affected() == 1)
        .map_err(map_err)
    }
}

/// 业务作用: 校验复合幂等身份可按两种数据库共同字节边界无损持久化。
///
/// 参数说明：
/// - `key`: 待写入复合主键的业务身份。
///
/// 返回: 全部分量非空、无 NUL 且不超过公共字节上限时成功；否则返回脱敏错误。
fn validate_key(key: &IdempotencyKey) -> Result<(), IdempotencyError> {
    for (value, max) in [
        (key.tenant.as_str(), 128_usize),
        (key.subject.as_str(), 190),
        (key.route_id.as_str(), 190),
        (key.client_key.as_str(), 190),
    ] {
        if value.is_empty() || value.len() > max || value.contains('\0') {
            return Err(IdempotencyError::new("invalid idempotency key"));
        }
    }
    Ok(())
}

/// 业务作用: 返回只由本 crate 固定、无外部输入的 PostgreSQL epoch 毫秒表达式。
///
/// 参数说明: 无。
///
/// 返回: 可安全嵌入固定 SQL 的数据库时钟表达式。
fn database_epoch_ms_sql() -> &'static str {
    "FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT"
}

/// 业务作用: 把 PostgreSQL 事务内核阶段映射为幂等调用方可穷举的封闭结果。
///
/// 参数说明：
/// - `error`: `natx-pgsql` 返回的业务或事务基础设施失败。
///
/// 返回: 业务错误保持原值；提交应答不确定单独映射为 `OutcomeUnknown`。
fn map_transaction_error<E>(error: TxRunError<E>) -> PgIdempotencyTransactionError<E> {
    match error {
        TxRunError::Rollback(error) => PgIdempotencyTransactionError::Business(error),
        TxRunError::RollbackOnly { .. } => PgIdempotencyTransactionError::RollbackOnly,
        TxRunError::CommitRejected { .. } => PgIdempotencyTransactionError::CommitRejected,
        TxRunError::CommitUncertain { .. } => PgIdempotencyTransactionError::OutcomeUnknown,
        TxRunError::RollbackFailed { .. } => PgIdempotencyTransactionError::RollbackFailed,
        TxRunError::Infrastructure { .. } => PgIdempotencyTransactionError::Infrastructure,
    }
}

/// 业务作用: 把底层连接、编码和数据库错误收敛为不含 SQL、凭据或业务正文的稳定错误。
///
/// 参数说明：
/// - `_error`: 仅触发分类、不进入公开文本的底层错误。
///
/// 返回: 固定数据库失败分类。
fn map_err<E>(_error: E) -> IdempotencyError {
    IdempotencyError::new("database error")
}
