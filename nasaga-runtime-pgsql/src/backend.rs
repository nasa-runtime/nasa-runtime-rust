//! PostgreSQL Saga 完整后端与事务执行器。

use nainbox_pgsql::PgInbox;
use naoutbox_pgsql::PgOutbox;
use nasaga_backend::{
    SagaBackend, SagaBackendError, SagaBackendErrorKind, SagaBackendFactory, SagaTransactionFuture,
    SagaTransactionRunError, SagaTransactionRunner,
};
use nasaga_pgsql::PgSagaStore;

/// 业务作用：在固定 PostgreSQL datasource 上为 Saga、Inbox 与 Outbox 建立同源事务。
#[derive(Debug, Clone)]
pub struct PgSagaTransactionRunner {
    datasource: natx_pgsql::DatasourceRef,
}

impl PgSagaTransactionRunner {
    /// 业务作用：创建绑定已校验 datasource 的 PostgreSQL Saga 事务执行器。
    ///
    /// 参数说明：`datasource` 是 store、Inbox 与 Outbox 共享的 qualifier。
    ///
    /// 返回：不建连的轻量执行器；后续操作只在该 datasource 建立事务。
    fn new(datasource: natx_pgsql::DatasourceRef) -> Self {
        Self { datasource }
    }
}

impl SagaTransactionRunner for PgSagaTransactionRunner {
    /// 业务作用：执行 PostgreSQL Saga 原子链并保留提交、回滚与结果不确定分类。
    ///
    /// 参数说明：`operation` 是必须与 Inbox claim、Saga 事实和 Outbox append 同事务的操作。
    ///
    /// 返回：提交明确成功时返回业务值；业务拒绝保留原错误，其它阶段返回封闭分类。
    fn run<'a, T, E>(
        &'a self,
        operation: SagaTransactionFuture<'a, T, E>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<T, SagaTransactionRunError<E>>> + Send + 'a>,
    >
    where
        T: Send + 'a,
        E: Send + 'a,
    {
        let datasource = self.datasource.clone();
        Box::pin(async move {
            let decision = async move {
                match operation.await {
                    Ok(value) => natx_pgsql::TxDecision::Commit(value),
                    Err(error) => natx_pgsql::TxDecision::Rollback(error),
                }
            };
            match natx_pgsql::run_decided_for_checked(&datasource, decision).await {
                Ok(value) => Ok(value),
                Err(natx_pgsql::TxEntryError::Run(natx_pgsql::TxRunError::Rollback(error))) => {
                    Err(SagaTransactionRunError::Rollback(error))
                }
                Err(natx_pgsql::TxEntryError::Run(natx_pgsql::TxRunError::CommitUncertain {
                    ..
                })) => Err(SagaTransactionRunError::CommitUncertain),
                Err(natx_pgsql::TxEntryError::Run(natx_pgsql::TxRunError::RollbackFailed {
                    ..
                })) => Err(SagaTransactionRunError::RollbackFailed),
                Err(natx_pgsql::TxEntryError::Run(natx_pgsql::TxRunError::CommitRejected {
                    ..
                })) => Err(SagaTransactionRunError::CommitRejected),
                Err(natx_pgsql::TxEntryError::Run(natx_pgsql::TxRunError::RollbackOnly {
                    ..
                })) => Err(SagaTransactionRunError::RollbackOnly),
                Err(natx_pgsql::TxEntryError::Run(natx_pgsql::TxRunError::Infrastructure {
                    ..
                }))
                | Err(natx_pgsql::TxEntryError::Lookup(_)) => {
                    Err(SagaTransactionRunError::Infrastructure)
                }
            }
        })
    }
}

/// 业务作用：冻结同一 PostgreSQL datasource 的 Saga store、Inbox、Outbox 与事务执行器。
#[derive(Debug, Clone)]
pub struct PgSagaBackend {
    datasource: natx_pgsql::DatasourceRef,
    store: PgSagaStore,
    inbox: PgInbox,
    outbox: PgOutbox,
    transaction_runner: PgSagaTransactionRunner,
}

impl Default for PgSagaBackend {
    /// 业务作用：构造绑定默认 PostgreSQL datasource 的完整 Saga 后端。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不建连且四种角色均绑定 `default` 的组合句柄。
    fn default() -> Self {
        Self::new()
    }
}

impl PgSagaBackend {
    /// 业务作用：创建绑定默认 PostgreSQL datasource 的完整 Saga 后端。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可直接交给后端中立状态机的轻量组合。
    pub fn new() -> Self {
        let datasource = natx_pgsql::DatasourceRef::default();
        Self {
            store: PgSagaStore::new(),
            inbox: PgInbox::new(),
            outbox: PgOutbox::new(),
            transaction_runner: PgSagaTransactionRunner::new(datasource.clone()),
            datasource,
        }
    }

    /// 业务作用：创建绑定命名 PostgreSQL datasource 的完整 Saga 后端并复验各角色同源。
    ///
    /// 参数说明：`datasource` 是启动期登记的 PostgreSQL datasource 名称。
    ///
    /// 返回：名称合法时返回同源组合；非法名称在数据库 I/O 前失败。
    pub fn with_datasource(datasource: impl AsRef<str>) -> anyhow::Result<Self> {
        let datasource = natx_pgsql::DatasourceRef::new(datasource)?;
        let store = PgSagaStore::with_datasource(&datasource)?;
        let inbox = PgInbox::with_datasource(&datasource)?;
        let outbox = PgOutbox::with_datasource(&datasource).map_err(anyhow::Error::new)?;
        Ok(Self {
            transaction_runner: PgSagaTransactionRunner::new(datasource.clone()),
            datasource,
            store,
            inbox,
            outbox,
        })
    }
}

impl SagaBackend for PgSagaBackend {
    type Store = PgSagaStore;
    type Inbox = PgInbox;
    type Outbox = PgOutbox;
    type TransactionRunner = PgSagaTransactionRunner;

    /// 业务作用：读取组合内的 PostgreSQL Saga store。
    fn store(&self) -> &Self::Store {
        &self.store
    }

    /// 业务作用：读取与 Saga store 同源的 PostgreSQL Inbox。
    fn inbox(&self) -> &Self::Inbox {
        &self.inbox
    }

    /// 业务作用：读取与 Saga store 同源的 PostgreSQL Outbox。
    fn outbox(&self) -> &Self::Outbox {
        &self.outbox
    }

    /// 业务作用：读取绑定相同 datasource 的 PostgreSQL 事务执行器。
    fn transaction_runner(&self) -> &Self::TransactionRunner {
        &self.transaction_runner
    }

    /// 业务作用：读取完整后端共享的 datasource qualifier。
    fn datasource_ref(&self) -> &natx_pgsql::DatasourceRef {
        &self.datasource
    }

    /// 业务作用：声明完整后端固定使用 PostgreSQL driver。
    fn driver(&self) -> natx_pgsql::DatabaseDriver {
        natx_pgsql::DatabaseDriver::PostgreSql
    }
}

impl SagaBackendFactory for PgSagaBackend {
    /// 业务作用：为 PostgreSQL 运行入口构造默认 datasource 组合。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：始终返回不建连的默认后端。
    fn default_backend() -> Result<Self, SagaBackendError> {
        Ok(Self::new())
    }

    /// 业务作用：为 PostgreSQL 运行入口构造命名 datasource 组合。
    ///
    /// 参数说明：`datasource` 是 Saga、Inbox 与 Outbox 共享的 qualifier。
    ///
    /// 返回：名称合法时返回同源后端；否则在 I/O 前返回基础设施分类。
    fn backend_for(datasource: &str) -> Result<Self, SagaBackendError> {
        Self::with_datasource(datasource).map_err(|_| {
            SagaBackendError::new(
                SagaBackendErrorKind::Infrastructure,
                "PostgreSQL Saga datasource identity is invalid",
            )
        })
    }
}
