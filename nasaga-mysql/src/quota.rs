//! 租户在飞实例配额账本。
//!
//! 账本与创建/终态迁移**同一事务**维护:创建真实发生时同事务原子预留(+1),实例进入
//! 任一终态时同事务释放(-1)。预留用"条件 UPDATE 影响行数"裁决而不是先查后写——
//! 无锁 `COUNT(*)` 在并发创建下必然穿透上限。账本与事实漂移时由有界对账收敛回已提交事实,
//! 不在请求路径扫描整表。

use sqlx::Row as _;

use crate::error::{map_connection, map_database, SagaStoreError};
use crate::instance::require_ambient_transaction;
use crate::MySqlSagaStore;

/// quota 账本建表语句(部署应由迁移拥有 schema;此处便于演示环境自举)。
pub(crate) const CREATE_QUOTA_SQL: &str = "CREATE TABLE IF NOT EXISTS saga_tenant_quota ( \
     tenant_id VARCHAR(256) NOT NULL, \
     in_flight BIGINT UNSIGNED NOT NULL DEFAULT 0, \
     initialized TINYINT NOT NULL DEFAULT 0, \
     updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), \
     PRIMARY KEY (tenant_id) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";

/// 业务作用：区分配额预留的两种结论;拒绝携带稳定原因码,与系统故障可区分。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaReservation {
    /// 预留成功,创建可以继续。
    Reserved,
    /// 该租户在飞实例已达上限;调用方必须回滚创建事务并返回稳定拒绝。
    Exceeded,
}

impl MySqlSagaStore {
    /// 业务作用：在创建事务内为租户原子预留一个在飞名额。
    ///
    /// 顺序固定:先幂等补账本行,再执行 `in_flight < cap` 的条件自增——两步都持有该租户
    /// 行锁,并发创建被串行化在同一行上,上限不可能被穿透。`cap` 为空表示该租户只观测
    /// 不设限,自增无条件执行(用量仍精确记账)。
    ///
    /// 参数说明：
    /// - `tenant`: 租户身份。
    /// - `cap`: 该租户的在飞实例上限;`None` 表示不设限。
    ///
    /// 返回：预留成功或超限结论;事务缺失或底层失败返回错误。
    pub async fn reserve_tenant_quota(
        &self,
        tenant: &nasaga_core::TenantId,
        cap: Option<u64>,
    ) -> Result<QuotaReservation, SagaStoreError> {
        // 预留必须与实例创建同事务:创建回滚时名额一并回滚,不留幽灵占用。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        sqlx::query("INSERT IGNORE INTO saga_tenant_quota (tenant_id, in_flight) VALUES (?, 0)")
            .bind(tenant.as_str())
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
        let updated = match cap {
            // 设限租户的预留要求账本已初始化:存量非终态实例只有经受锁对账回填后才在账,
            // 否则其终态释放会扣掉新实例持有的名额,上限被静默穿透。未初始化按部署
            // 错误上报,绝不能与"配额耗尽"混为同一个稳定拒绝。
            Some(cap) => sqlx::query(
                "UPDATE saga_tenant_quota SET in_flight = in_flight + 1 \
                 WHERE tenant_id = ? AND in_flight < ? AND initialized = 1",
            )
            .bind(tenant.as_str())
            .bind(cap)
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?,
            None => sqlx::query(
                "UPDATE saga_tenant_quota SET in_flight = in_flight + 1 WHERE tenant_id = ?",
            )
            .bind(tenant.as_str())
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?,
        };
        if updated.rows_affected() == 0 {
            if cap.is_some() {
                let initialized: i8 = sqlx::query_scalar(
                    "SELECT initialized FROM saga_tenant_quota WHERE tenant_id = ?",
                )
                .bind(tenant.as_str())
                .fetch_one(connection.as_mut())
                .await
                .map_err(map_database)?;
                if initialized == 0 {
                    return Err(SagaStoreError::new(
                        "tenant quota ledger is not initialized; run reconcile_tenant_quota after every writer runs the accounting binary",
                    ));
                }
            }
            return Ok(QuotaReservation::Exceeded);
        }
        Ok(QuotaReservation::Reserved)
    }

    /// 业务作用：在终态迁移事务内释放实例占用的在飞名额。
    ///
    /// 以 saga_id 联表定位租户,单条语句在同事务内完成;`in_flight > 0` 防御账本欠账时
    /// 减成回绕。已在飞实例不受新建配额影响——释放只发生在真实终态。
    ///
    /// 参数说明：
    /// - `saga_id`: 刚进入终态的实例身份。
    ///
    /// 返回：释放完成(或账本无该租户行)返回 `Ok`;事务缺失或底层失败返回错误。
    pub(crate) async fn release_tenant_quota(
        &self,
        saga_id: &nasaga_core::SagaId,
    ) -> Result<(), SagaStoreError> {
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        sqlx::query(
            "UPDATE saga_tenant_quota quota \
             JOIN saga_instance instance ON instance.tenant_id = quota.tenant_id \
             SET quota.in_flight = quota.in_flight - 1 \
             WHERE instance.saga_id = ? AND quota.in_flight > 0",
        )
        .bind(saga_id.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        Ok(())
    }

    /// 业务作用：查询设限租户的账本是否已完成初始化回填——Ready 门禁据此拒绝
    /// "存量未入账即启用上限"的部署。
    ///
    /// 参数说明：
    /// - `tenant`: 租户身份。
    ///
    /// 返回：账本行存在且已由对账置位返回真;无行或未置位返回假。
    pub async fn tenant_quota_initialized(
        &self,
        tenant: &nasaga_core::TenantId,
    ) -> Result<bool, SagaStoreError> {
        let mut connection = natx::conn().await.map_err(map_connection)?;
        let initialized: Option<i8> =
            sqlx::query_scalar("SELECT initialized FROM saga_tenant_quota WHERE tenant_id = ?")
                .bind(tenant.as_str())
                .fetch_optional(connection.as_mut())
                .await
                .map_err(map_database)?;
        Ok(initialized == Some(1))
    }

    /// 业务作用：读取单个租户的精确在飞用量——只服务受鉴权管理查询,不进指标标签。
    ///
    /// 参数说明：
    /// - `tenant`: 租户身份。
    ///
    /// 返回：账本记录的在飞实例数;无记录返回 0。
    pub async fn tenant_quota_usage(
        &self,
        tenant: &nasaga_core::TenantId,
    ) -> Result<u64, SagaStoreError> {
        let mut connection = natx::conn().await.map_err(map_connection)?;
        let row = sqlx::query("SELECT in_flight FROM saga_tenant_quota WHERE tenant_id = ?")
            .bind(tenant.as_str())
            .fetch_optional(connection.as_mut())
            .await
            .map_err(map_database)?;
        match row {
            Some(row) => {
                let usage: u64 = row.try_get("in_flight").map_err(map_database)?;
                Ok(usage)
            }
            None => Ok(0),
        }
    }

    /// 业务作用：按已提交事实有界对账单个租户的账本——把历史数据或崩溃窗口造成的
    /// 漂移收敛回真实在飞数。
    ///
    /// 必须在 ambient 事务内调用,且**先锁账本行再计数**:创建/终态路径的每次账本变更
    /// 都持有同一把行锁,未提交的在线事务被挡在行锁外、其实例行对本次计数不可见;
    /// 先计数后覆盖的写法会让"计数与覆盖之间提交的在线变更"被旧值覆盖,对账本身
    /// 制造漂移。计数范围由租户过滤约束;不在请求路径调用,由运维显式触发。
    ///
    /// 参数说明：
    /// - `tenant`: 租户身份。
    ///
    /// 返回：对账后的在飞实例数;事务缺失或底层失败返回错误。
    pub async fn reconcile_tenant_quota(
        &self,
        tenant: &nasaga_core::TenantId,
    ) -> Result<u64, SagaStoreError> {
        // 对账与在线路径经账本行锁串行化,锁的生命周期必须覆盖"计数→覆盖"全程,
        // 因此强制运行在调用方事务内。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        sqlx::query("INSERT IGNORE INTO saga_tenant_quota (tenant_id, in_flight) VALUES (?, 0)")
            .bind(tenant.as_str())
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
        // 行锁先行:此后任何在线创建(预留)与终态(释放)都在本事务提交前被阻塞,
        // 计数窗口内不可能再有账本参与方提交新事实。
        let _locked: u64 = sqlx::query_scalar(
            "SELECT in_flight FROM saga_tenant_quota WHERE tenant_id = ? FOR UPDATE",
        )
        .bind(tenant.as_str())
        .fetch_one(connection.as_mut())
        .await
        .map_err(map_database)?;
        let actual: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM saga_instance WHERE tenant_id = ? \
             AND status NOT IN (?, ?, ?)",
        )
        .bind(tenant.as_str())
        .bind(nasaga_core::SagaStatus::Completed.as_str())
        .bind(nasaga_core::SagaStatus::Compensated.as_str())
        .bind(nasaga_core::SagaStatus::ManuallyClosed.as_str())
        .fetch_one(connection.as_mut())
        .await
        .map_err(map_database)?;
        let actual = actual.max(0) as u64;
        // 对账是初始化标记的唯一置位入口:标记表达"存量事实已在受锁窗口内入账",
        // 设限租户的预留与 Ready 门禁都以它为放行条件。
        sqlx::query(
            "UPDATE saga_tenant_quota SET in_flight = ?, initialized = 1 WHERE tenant_id = ?",
        )
        .bind(actual)
        .bind(tenant.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        Ok(actual)
    }
}
