//! Saga PostgreSQL 持久结构与 standalone 自举入口。
//!
//! 生产部署应由 migration 拥有 schema；本模块只在新建 standalone 数据库中
//! 创建当前完整结构，不推断或改写历史结构。

use crate::error::{map_connection, map_database, SagaStoreError};
use crate::PgSagaStore;

const CREATE_SAGA_SCHEMA_SQL: &str = include_str!("../migrations/create_saga.sql");

impl PgSagaStore {
    /// 业务作用：在默认 PostgreSQL datasource 创建当前完整 Saga 结构。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部 DDL 明确成功时返回 `Ok`；连接或 DDL 失败返回脱敏错误。
    pub async fn ensure_schema() -> Result<(), SagaStoreError> {
        Self::ensure_schema_for(natx_pgsql::DEFAULT_DATASOURCE).await
    }

    /// 业务作用：在命名 PostgreSQL datasource 创建当前完整 Saga 结构。
    ///
    /// 参数说明：`datasource` 是已注册的 PostgreSQL qualifier。
    ///
    /// 返回：表、索引与 updated-at trigger 全部可用时返回 `Ok`；任一步失败不宣称结构可用。
    pub async fn ensure_schema_for(datasource: impl AsRef<str>) -> Result<(), SagaStoreError> {
        let datasource = natx_pgsql::DatasourceRef::new(datasource).map_err(map_connection)?;
        let mut connection = natx_pgsql::conn_for(&datasource)
            .await
            .map_err(map_connection)?;
        // 自举与生产迁移必须读取同一份结构合同，避免 DDL 在两条维护路径中漂移。
        sqlx::raw_sql(CREATE_SAGA_SCHEMA_SQL)
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
        Ok(())
    }
}
