//! 幂等 store 的 MySQL 后端。
//!
//! 实现 [`naidempotency::IdempotencyStore`],经 [`natx::conn`] 取连接:
//! - 在 `natx::run`（`#[transactional]`）**事务内**调用 → 幂等记录与业务写**共享同一事务**，原子提交或回滚。
//! - 事务外调用(如框架幂等中间件)→ 走连接池,得到**跨重启/跨副本**持久化的 response-cache 语义。
//!
//! `begin` 用 `INSERT`(唯一主键)竞态安全地占位:插入成功=首次;主键冲突则 `SELECT` 现有行裁决
//! 重放/并发/指纹冲突。**不回显** SQL/凭据:任何底层错误都映射为脱敏的 [`IdempotencyError`]。

#![forbid(unsafe_code)]

use async_trait::async_trait;
use naidempotency::{
    ExecutionLease, IdempotencyError, IdempotencyKey, IdempotencyOutcome, IdempotencyStore,
    RequestFingerprint, StoredResponse,
};
use sqlx::Row as _;

/// 记录状态:进行中。
const STATE_IN_FLIGHT: i8 = 0;
/// 记录状态:已完成。
const STATE_COMPLETED: i8 = 1;

/// 幂等表建表语句(部署应由迁移拥有 schema;此处便于演示环境自举)。
const CREATE_TABLE_SQL: &str = "CREATE TABLE IF NOT EXISTS idempotency_record_v2 ( \
     tenant VARCHAR(128) NOT NULL, \
     subject VARCHAR(190) NOT NULL, \
     route_id VARCHAR(190) NOT NULL, \
     client_key VARCHAR(190) NOT NULL, \
     fingerprint BINARY(32) NOT NULL, \
     lease BINARY(16) NOT NULL, \
     state TINYINT NOT NULL, \
     status SMALLINT UNSIGNED NULL, \
     body LONGBLOB NULL, \
     headers LONGBLOB NULL, \
     lease_expires_at DATETIME(6) NOT NULL, \
     created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, \
     updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
     PRIMARY KEY (tenant, subject, route_id, client_key) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

/// MySQL 幂等 store。无自身状态:每次操作经 `natx::conn()` 取连接,自动感知 ambient 事务。
#[derive(Debug, Default, Clone, Copy)]
pub struct MySqlIdempotencyStore;

impl MySqlIdempotencyStore {
    /// 业务作用：创建 store(不建连;连接在每次操作时经 natx 获取)。
    pub fn new() -> Self {
        Self
    }

    /// 业务作用：确保幂等表存在。部署由迁移拥有 schema;此方法供演示环境自举。
    ///
    /// 需先 `natx::init` 注册默认 datasource。
    pub async fn ensure_schema() -> Result<(), IdempotencyError> {
        let mut conn = natx::conn().await.map_err(map_err)?;
        sqlx::query(CREATE_TABLE_SQL)
            .execute(conn.as_mut())
            .await
            .map_err(map_err)?;
        Ok(())
    }
}

#[async_trait]
impl IdempotencyStore for MySqlIdempotencyStore {
    /// 业务作用：以唯一键 INSERT 竞争首次执行，并对冲突记录执行租约接管或已有状态裁决。
    async fn begin(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
    ) -> Result<IdempotencyOutcome, IdempotencyError> {
        // 1) 竞态安全占位:INSERT in-flight。成功=首次;主键冲突转 2) 决策。
        //    单独作用域:query 跑完即释放 Conn(事务分支持锁,不可同时持两句柄)。
        let insert = {
            let mut conn = natx::conn().await.map_err(map_err)?;
            sqlx::query(
                "INSERT INTO idempotency_record_v2 \
                 (tenant, subject, route_id, client_key, fingerprint, lease, state, status, body, headers, lease_expires_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL 5 MINUTE))",
            )
            .bind(&key.tenant)
            .bind(&key.subject)
            .bind(&key.route_id)
            .bind(&key.client_key)
            .bind(fingerprint.0.as_slice())
            .bind(lease.0.as_slice())
            .bind(STATE_IN_FLIGHT)
            .execute(conn.as_mut())
            .await
        };

        match insert {
            Ok(_) => Ok(IdempotencyOutcome::FirstExecution),
            Err(error) if is_unique_violation(&error) => {
                // 崩溃遗留的租约可在 5 分钟后由新 owner 原子接管；未过期记录只读裁决。
                let mut conn = natx::conn().await.map_err(map_err)?;
                let takeover = sqlx::query(
                    "UPDATE idempotency_record_v2 SET fingerprint = ?, lease = ?, \
                     lease_expires_at = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL 5 MINUTE), \
                     status = NULL, body = NULL, headers = NULL \
                     WHERE tenant = ? AND subject = ? AND route_id = ? AND client_key = ? \
                     AND state = ? AND lease_expires_at < CURRENT_TIMESTAMP(6)",
                )
                .bind(fingerprint.0.as_slice())
                .bind(lease.0.as_slice())
                .bind(&key.tenant)
                .bind(&key.subject)
                .bind(&key.route_id)
                .bind(&key.client_key)
                .bind(STATE_IN_FLIGHT)
                .execute(conn.as_mut())
                .await
                .map_err(map_err)?;
                drop(conn);
                if takeover.rows_affected() == 1 {
                    Ok(IdempotencyOutcome::FirstExecution)
                } else {
                    self.decide_existing(key, fingerprint).await
                }
            }
            Err(error) => Err(map_err(error)),
        }
    }

    /// 业务作用：在 fingerprint 与 lease 同时匹配时把记录原子转换为可重放完成态。
    async fn complete(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
        response: StoredResponse,
    ) -> Result<bool, IdempotencyError> {
        // 只更新仍 in-flight 的记录(state 谓词)防越权覆盖;非本记录/已完成 → 0 行影响,忽略。
        let mut conn = natx::conn().await.map_err(map_err)?;
        sqlx::query(
            "UPDATE idempotency_record_v2 SET state = ?, status = ?, body = ?, headers = ? \
             WHERE tenant = ? AND subject = ? AND route_id = ? AND client_key = ? \
             AND state = ? AND fingerprint = ? AND lease = ?",
        )
        .bind(STATE_COMPLETED)
        .bind(response.status)
        .bind(&response.body)
        .bind(serde_json::to_vec(&response.headers).map_err(map_err)?)
        .bind(&key.tenant)
        .bind(&key.subject)
        .bind(&key.route_id)
        .bind(&key.client_key)
        .bind(STATE_IN_FLIGHT)
        .bind(fingerprint.0.as_slice())
        .bind(lease.0.as_slice())
        .execute(conn.as_mut())
        .await
        .map(|result| result.rows_affected() == 1)
        .map_err(map_err)
    }

    /// 业务作用：删除仍属于当前 owner 的在途记录，已完成或已换 owner 时返回 false。
    async fn abort(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
    ) -> Result<bool, IdempotencyError> {
        let mut conn = natx::conn().await.map_err(map_err)?;
        sqlx::query(
            "DELETE FROM idempotency_record_v2 \
             WHERE tenant = ? AND subject = ? AND route_id = ? AND client_key = ? \
             AND state = ? AND fingerprint = ? AND lease = ?",
        )
        .bind(&key.tenant)
        .bind(&key.subject)
        .bind(&key.route_id)
        .bind(&key.client_key)
        .bind(STATE_IN_FLIGHT)
        .bind(fingerprint.0.as_slice())
        .bind(lease.0.as_slice())
        .execute(conn.as_mut())
        .await
        .map(|result| result.rows_affected() == 1)
        .map_err(map_err)
    }
}

impl MySqlIdempotencyStore {
    /// 业务作用：主键已存在时读现有行裁决:指纹不符→冲突;已完成→重放;仍进行中→并发冲突。
    async fn decide_existing(
        &self,
        key: &IdempotencyKey,
        fingerprint: RequestFingerprint,
    ) -> Result<IdempotencyOutcome, IdempotencyError> {
        let row = {
            let mut conn = natx::conn().await.map_err(map_err)?;
            sqlx::query(
                "SELECT state, fingerprint, status, body, headers FROM idempotency_record_v2 \
                 WHERE tenant = ? AND subject = ? AND route_id = ? AND client_key = ?",
            )
            .bind(&key.tenant)
            .bind(&key.subject)
            .bind(&key.route_id)
            .bind(&key.client_key)
            .fetch_optional(conn.as_mut())
            .await
            .map_err(map_err)?
        };

        // 行刚被并发删除(极罕见):按仍在进行处理,让上层重试而非误判首次。
        let Some(row) = row else {
            return Ok(IdempotencyOutcome::ConcurrentInFlight);
        };

        let existing_fingerprint: Vec<u8> = row.try_get("fingerprint").map_err(map_err)?;
        if existing_fingerprint.as_slice() != fingerprint.0 {
            return Ok(IdempotencyOutcome::FingerprintConflict);
        }

        let state: i8 = row.try_get("state").map_err(map_err)?;
        if state == STATE_COMPLETED {
            let status: Option<u16> = row.try_get("status").map_err(map_err)?;
            let body: Option<Vec<u8>> = row.try_get("body").map_err(map_err)?;
            let headers: Option<Vec<u8>> = row.try_get("headers").map_err(map_err)?;
            let status = status
                .filter(|status| (100..=599).contains(status))
                .ok_or_else(|| IdempotencyError::new("corrupt idempotency record"))?;
            let body = body.ok_or_else(|| IdempotencyError::new("corrupt idempotency record"))?;
            let headers =
                headers.ok_or_else(|| IdempotencyError::new("corrupt idempotency record"))?;
            Ok(IdempotencyOutcome::Replay(StoredResponse {
                status,
                body,
                headers: serde_json::from_slice(&headers)
                    .map_err(|_| IdempotencyError::new("corrupt idempotency record"))?,
            }))
        } else if state == STATE_IN_FLIGHT {
            Ok(IdempotencyOutcome::ConcurrentInFlight)
        } else {
            Err(IdempotencyError::new("corrupt idempotency record"))
        }
    }
}

/// 业务作用：把任意底层错误映射为脱敏的 [`IdempotencyError`](绝不回显 SQL/凭据/请求体)。
fn map_err<E>(_error: E) -> IdempotencyError {
    IdempotencyError::new("database error")
}

/// 业务作用：是否为唯一键冲突(主键已存在)。
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .map(|db| db.is_unique_violation())
        .unwrap_or(false)
}
