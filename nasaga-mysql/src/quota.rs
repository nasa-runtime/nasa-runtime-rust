//! 租户在飞实例配额账本。
//!
//! 账本与创建/终态迁移**同一事务**维护:创建真实发生时同事务原子预留(+1),实例进入
//! 任一终态时同事务释放(-1)。预留用"条件 UPDATE 影响行数"裁决而不是先查后写——
//! 无锁 `COUNT(*)` 在并发创建下必然穿透上限。账本与事实漂移时由受锁对账按当前实例事实收敛,
//! 不在请求路径扫描整表。

use nasaga_backend::QuotaReservation;
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

impl MySqlSagaStore {
    /// 业务作用：在创建事务内为租户原子预留一个在飞名额。
    ///
    /// 顺序固定:先幂等补账本行并取得排他锁,再执行 `in_flight < cap` 的条件自增——
    /// 并发创建被串行化在同一行上,上限不可能被穿透。`cap` 为空表示该租户只观测
    /// 不设限,自增无条件执行(用量仍精确记账)。
    ///
    /// 参数说明：
    /// - `tenant`: 租户身份。
    /// - `cap`: 该租户的在飞实例上限;`None` 表示不设限。
    ///
    /// 返回：预留成功或已初始化账本的超限结论；设限账本未初始化、事务缺失或底层失败返回错误。
    /// 拒绝原因按当前受锁账本判断，不沿用调用方已有的一致性快照。
    pub async fn reserve_tenant_quota(
        &self,
        tenant: &nasaga_core::TenantId,
        cap: Option<u64>,
    ) -> Result<QuotaReservation, SagaStoreError> {
        // 预留必须与实例创建同事务:创建回滚时名额一并回滚,不留幽灵占用。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        // 补行直接取得排他锁,避免已有租户的并发事务先持共享锁、再相互等待锁升级。
        sqlx::query(
            "INSERT INTO saga_tenant_quota (tenant_id, in_flight) VALUES (?, 0) \
             ON DUPLICATE KEY UPDATE tenant_id = saga_tenant_quota.tenant_id",
        )
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
                // 条件更新与拒绝分类必须使用同一当前账本；已有排他锁不会刷新普通读快照，
                // 因此继续锁定读，避免把并发已完成初始化的满额账本误判为缺行或未初始化。
                let initialized: i8 = sqlx::query_scalar(
                    "SELECT initialized FROM saga_tenant_quota WHERE tenant_id = ? FOR UPDATE",
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
        let mut connection = natx::mandatory_conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
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
        let mut connection = natx::conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
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
        let mut connection = natx::conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
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

    /// 业务作用：按当前非终态实例事实有界对账单个租户的账本，包含本事务自身的变更，
    /// 使账本与实例事实随调用方事务一起提交或回滚。
    ///
    /// 必须在 ambient 事务内调用，先锁账本行，再以锁定读计数，不能沿用外层事务已有的
    /// 一致性快照。计数所需记录被其它事务锁定时立即失败，不等待或跳过记录，避免与先修改
    /// 实例再释放配额的终态路径形成反向等待。调用方须回滚整个事务后有界重试；
    /// 本方法不提交、重启或更改调用方事务的隔离级别。计数由租户索引约束，供运维显式触发。
    ///
    /// 参数说明：
    /// - `tenant`: 租户身份。
    ///
    /// 返回：对账后的在飞实例数；事务缺失、事实行锁冲突或底层失败返回错误。
    /// 计数失败不覆盖账本或置初始化标记；成功修改仍需调用方提交才对其它事务可见。
    pub async fn reconcile_tenant_quota(
        &self,
        tenant: &nasaga_core::TenantId,
    ) -> Result<u64, SagaStoreError> {
        // 对账与在线路径经账本行锁串行化,锁的生命周期必须覆盖"计数→覆盖"全程,
        // 因此强制运行在调用方事务内。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        // 对账与预留采用相同的排他补行方式,避免两个入口并发时形成共享锁升级环。
        sqlx::query(
            "INSERT INTO saga_tenant_quota (tenant_id, in_flight) VALUES (?, 0) \
             ON DUPLICATE KEY UPDATE tenant_id = saga_tenant_quota.tenant_id",
        )
        .bind(tenant.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        // 账本锁阻止其它记账事务提交；它不会刷新已有一致性快照，也不阻止终态事务
        // 先持有实例行锁，因此后续计数仍必须使用当前锁定读并拒绝实例锁等待。
        let _locked: u64 = sqlx::query_scalar(
            "SELECT in_flight FROM saga_tenant_quota WHERE tenant_id = ? FOR UPDATE",
        )
        .bind(tenant.as_str())
        .fetch_one(connection.as_mut())
        .await
        .map_err(map_database)?;
        // FOR SHARE 读取当前事实而非旧快照；NOWAIT 避免对账持账本锁等待实例、终态
        // 持实例锁等待账本的环。不得使用 SKIP LOCKED，否则漏计仍会开放额外名额。
        let actual: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM saga_instance FORCE INDEX (idx_tenant_status_saga) \
             WHERE tenant_id = ? AND status NOT IN (?, ?, ?) FOR SHARE NOWAIT",
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
