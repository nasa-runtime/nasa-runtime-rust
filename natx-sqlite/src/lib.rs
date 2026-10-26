//! SQLite 文件数据源、受控 SQL 执行和同源本地事务。
//!
//! 数据源使用句柄与关闭 owner 分离；原生连接退出之前持续持有文件独占权。
//! 业务通过 [`SqliteConn::executor`] 执行单条 SQL，事务由框架统一提交或回滚。
//!
//! # 受管入口与执行架构
//!
//! `nasa` 的 `application,mapper-sqlite` 组合让业务继续使用 Mapper 与 SQL 方法宏，
//! napp 的同一 `db` 组件按每个 datasource 的 driver 管理文件 owner、迁移、观测和停机。
//! 独立宿主持有 [`SqliteDataSourceOwner`]，可克隆的使用句柄不拥有关闭权。
//! 请求与配置的共同截止点在独立 lease 接纳前固定，覆盖选池、空闲持有和原生监督；
//! 环境事务保持自身总期限，真实原生关闭前不提前释放容量或文件权威。
//! 仅支持 Linux/macOS 私有本地文件，不提供跨库原子事务、网络文件库或 SQLite Saga 持久后端。
mod datasource;
mod diagnostics;
pub use diagnostics::{
    classify_sqlite_error, engine_version_number, SqliteErrorCategory, SqliteMetricsSource,
};
mod executor;
mod health;
pub use health::{ConnectionCapabilities, IsolationReason};
mod migration;
mod native;
pub use migration::{MigrationRecord, MigrationSession};
mod maintenance;
mod maintenance_io;
mod open_guard;
mod ownership;
mod publication;
pub use maintenance::{
    restore_to_new, BackupReport, CheckpointMode, CheckpointResult, DatabaseInspection,
    MaintenanceError, MaintenanceErrorOrigin, SqliteMaintenance,
};
mod observation;
pub use observation::pool_metrics_source;
mod registry;
mod stream;
mod transaction;
pub use stream::SqliteStreamBudget;

pub use datasource::{
    open_owned, DataSourceConfig, JournalMode, SqliteDataSource, SqliteDataSourceOwner,
    SqliteOptions, Synchronous,
};
pub use executor::{SqliteConn, SqliteExecutor};
pub use natx_core::{
    current_driver, observability, DataSourceCatalog, DataSourceLookupError, DatabaseDriver,
    DatasourceRef, ManagedInstallationToken, ManagedRegistryOwner, TxDecision, TxEntryError,
    TxRollbackCause, TxRunError, DEFAULT_DATASOURCE,
};
pub use natx_macro::transactional_sqlite as transactional;
pub use registry::*;
pub use transaction::*;
pub use transaction::{
    conn_for as mapper_conn_for, mandatory_conn_for as mapper_mandatory_conn_for,
    never_conn_for as mapper_never_conn_for,
};
