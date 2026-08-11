//! 租户管理动作速率账本。
//!
//! 变更类管理动作（pause/resume/retry/manual close）会向恢复通道注入外部副作用；单租户
//! 用重试类动作刷频会挤占其它租户的恢复能力。速率账本按**数据库时钟**的固定窗口计数，
//! 与动作事务同提交：动作回滚时预算一并退还，不产生幽灵消耗。窗口裁决用"条件 UPDATE
//! 影响行数"而不是先查后写——无锁计数在并发动作下必然穿透上限。
//!
//! 时间源固定取数据库 `NOW(6)`：多副本 Orchestrator 共享同一个窗口边界，不受各进程
//! 本地时钟漂移影响；也因此本模块不接受调用方传入的时刻参数。

use sqlx::Row as _;

use crate::error::{map_connection, map_database, SagaStoreError};
use crate::instance::require_ambient_transaction;
use crate::MySqlSagaStore;

/// 速率账本建表语句(部署应由迁移拥有 schema；此处便于演示环境自举)。
pub(crate) const CREATE_ACTION_RATE_SQL: &str =
    "CREATE TABLE IF NOT EXISTS saga_tenant_action_rate ( \
     tenant_id VARCHAR(256) NOT NULL, \
     window_start_ms BIGINT UNSIGNED NOT NULL, \
     used BIGINT UNSIGNED NOT NULL DEFAULT 0, \
     updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), \
     PRIMARY KEY (tenant_id, window_start_ms) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";

/// 业务作用：区分速率预留的两种结论；拒绝携带稳定原因码，与系统故障可区分。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionRateReservation {
    /// 当前窗口仍有预算，动作可以继续。
    Reserved,
    /// 该租户当前窗口的管理动作预算已耗尽；调用方必须回滚动作事务并返回稳定拒绝。
    Exceeded,
}

impl MySqlSagaStore {
    /// 业务作用：在管理动作事务内为租户原子预留一次当前窗口的动作预算。
    ///
    /// 顺序固定：先读数据库时钟定位当前窗口，再幂等补窗口行，最后执行 `used < max`
    /// 的条件自增——自增持有该租户窗口行锁，并发动作被串行化，上限不可能被穿透。
    /// `max_actions = 0` 表示该租户的变更类管理动作被完全封禁（条件永不满足）。
    /// 顺带清理该租户已过期的历史窗口行，账本体量与租户数同阶、不随时间累积。
    ///
    /// 参数说明：
    /// - `tenant`: 租户身份。
    /// - `max_actions`: 当前窗口允许提交的动作数上限。
    /// - `window_ms`: 窗口长度（毫秒），必须为正。
    ///
    /// 返回：预留成功或超限结论；窗口长度非法、事务缺失或底层失败返回错误。
    pub async fn reserve_tenant_action_rate(
        &self,
        tenant: &nasaga_core::TenantId,
        max_actions: u64,
        window_ms: i64,
    ) -> Result<ActionRateReservation, SagaStoreError> {
        if window_ms <= 0 {
            // 配置错误必须与"预算耗尽"可区分:窗口非法是部署问题,不能吞成稳定拒绝。
            return Err(SagaStoreError::new(
                "action rate window_ms must be positive",
            ));
        }
        // 预留必须与管理动作同事务:动作失败回滚时预算一并退还,失败尝试不烧预算。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        // 单次读取数据库时钟并在 Rust 侧对齐窗口:同一语句序列内窗口身份恒定,
        // 不会因两次 NOW() 跨越窗口边界而把自增打到不存在的行上。
        let now_ms: u64 =
            sqlx::query_scalar("SELECT CAST(ROUND(UNIX_TIMESTAMP(NOW(6)) * 1000) AS UNSIGNED)")
                .fetch_one(connection.as_mut())
                .await
                .map_err(map_database)?;
        let window = window_ms as u64;
        let window_start = now_ms - now_ms % window;
        sqlx::query(
            "INSERT IGNORE INTO saga_tenant_action_rate (tenant_id, window_start_ms, used) \
             VALUES (?, ?, 0)",
        )
        .bind(tenant.as_str())
        .bind(window_start)
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        let updated = sqlx::query(
            "UPDATE saga_tenant_action_rate SET used = used + 1 \
             WHERE tenant_id = ? AND window_start_ms = ? AND used < ?",
        )
        .bind(tenant.as_str())
        .bind(window_start)
        .bind(max_actions)
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        // 过期窗口只在该租户已有动作时顺带回收,范围被主键前缀限定,行数与窗口翻转
        // 次数同阶;删除历史行不影响当前窗口裁决。
        sqlx::query(
            "DELETE FROM saga_tenant_action_rate WHERE tenant_id = ? AND window_start_ms < ?",
        )
        .bind(tenant.as_str())
        .bind(window_start)
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        if updated.rows_affected() == 0 {
            return Ok(ActionRateReservation::Exceeded);
        }
        Ok(ActionRateReservation::Reserved)
    }

    /// 业务作用：读取单个租户当前窗口的已提交动作数——只服务受鉴权管理查询，不进指标标签。
    ///
    /// 参数说明：
    /// - `tenant`: 租户身份。
    /// - `window_ms`: 窗口长度（毫秒），必须与限速配置一致，否则读到的是另一套窗口。
    ///
    /// 返回：`(窗口起点毫秒, 已用动作数)`；当前窗口无记录时已用为 0。
    pub async fn tenant_action_rate_usage(
        &self,
        tenant: &nasaga_core::TenantId,
        window_ms: i64,
    ) -> Result<(u64, u64), SagaStoreError> {
        if window_ms <= 0 {
            return Err(SagaStoreError::new(
                "action rate window_ms must be positive",
            ));
        }
        let mut connection = natx::conn().await.map_err(map_connection)?;
        let now_ms: u64 =
            sqlx::query_scalar("SELECT CAST(ROUND(UNIX_TIMESTAMP(NOW(6)) * 1000) AS UNSIGNED)")
                .fetch_one(connection.as_mut())
                .await
                .map_err(map_database)?;
        let window = window_ms as u64;
        let window_start = now_ms - now_ms % window;
        let row = sqlx::query(
            "SELECT used FROM saga_tenant_action_rate \
             WHERE tenant_id = ? AND window_start_ms = ?",
        )
        .bind(tenant.as_str())
        .bind(window_start)
        .fetch_optional(connection.as_mut())
        .await
        .map_err(map_database)?;
        let used = match row {
            Some(row) => row.try_get::<u64, _>("used").map_err(map_database)?,
            None => 0,
        };
        Ok((window_start, used))
    }
}
