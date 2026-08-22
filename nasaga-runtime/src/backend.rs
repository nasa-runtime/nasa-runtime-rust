//! 现有 MySQL runtime 的完整后端组合身份与事务执行器。

use nainbox_mysql::MySqlInbox;
use naoutbox_mysql::MySqlOutbox;
use nasaga_backend::{
    SagaBackend, SagaBackendError, SagaBackendErrorKind, SagaBackendFactory, SagaTransactionFuture,
    SagaTransactionRunError, SagaTransactionRunner,
};
use nasaga_mysql::MySqlSagaStore;

/// 业务作用：在固定 MySQL datasource 上为 Saga、Inbox 与 Outbox 建立同源事务。
#[derive(Debug, Clone)]
pub struct MySqlSagaTransactionRunner {
    datasource: natx::DatasourceRef,
}

impl MySqlSagaTransactionRunner {
    /// 业务作用：创建绑定已校验 datasource 的 MySQL Saga 事务执行器。
    ///
    /// 参数说明：`datasource` 是将由 store、Inbox 与 Outbox 共同使用的 qualifier。
    ///
    /// 返回：不建连的轻量执行器；后续每轮操作在该 datasource 上建立事务。
    fn new(datasource: natx::DatasourceRef) -> Self {
        Self { datasource }
    }
}

impl SagaTransactionRunner for MySqlSagaTransactionRunner {
    /// 业务作用：执行 MySQL Saga 原子链，并把提交、回滚与结果不确定阶段封闭分类。
    ///
    /// 参数说明：`operation` 是必须与 Inbox claim 和 Outbox append 同事务的组合操作。
    ///
    /// 返回：提交明确成功时返回业务值；业务拒绝保留原错误，其它事务阶段转换为后端错误。
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
                    Ok(value) => natx::TxDecision::Commit(value),
                    Err(error) => natx::TxDecision::Rollback(error),
                }
            };
            match natx::run_decided_for_checked(&datasource, decision).await {
                Ok(value) => Ok(value),
                // 业务闭包主动拒绝提交时物理回滚已确认，保留原领域分类供 transport 裁决。
                Err(natx::TxEntryError::Run(natx::TxRunError::Rollback(error))) => {
                    Err(SagaTransactionRunError::Rollback(error))
                }
                Err(natx::TxEntryError::Run(natx::TxRunError::CommitUncertain { .. })) => {
                    // COMMIT 已发出后无法证明服务端结果，禁止直接重执可能含外部副作用的闭包。
                    Err(SagaTransactionRunError::CommitUncertain)
                }
                Err(natx::TxEntryError::Run(natx::TxRunError::RollbackFailed { .. })) => {
                    // 物理回滚未确认时不能向消息层证明零副作用，只能依靠 Inbox 持续收敛。
                    Err(SagaTransactionRunError::RollbackFailed)
                }
                Err(natx::TxEntryError::Run(natx::TxRunError::CommitRejected { .. })) => {
                    Err(SagaTransactionRunError::CommitRejected)
                }
                Err(natx::TxEntryError::Run(natx::TxRunError::RollbackOnly { .. })) => {
                    Err(SagaTransactionRunError::RollbackOnly)
                }
                Err(natx::TxEntryError::Run(natx::TxRunError::Infrastructure { .. })) => {
                    Err(SagaTransactionRunError::Infrastructure)
                }
                Err(natx::TxEntryError::Lookup(_)) => Err(SagaTransactionRunError::Infrastructure),
            }
        })
    }
}

/// 业务作用：把同一 MySQL datasource 的 Saga store、Inbox、Outbox 与事务执行器冻结成组合。
#[derive(Debug, Clone)]
pub struct MySqlSagaBackend {
    datasource: natx::DatasourceRef,
    store: MySqlSagaStore,
    inbox: MySqlInbox,
    outbox: MySqlOutbox,
    transaction_runner: MySqlSagaTransactionRunner,
}

impl Default for MySqlSagaBackend {
    /// 业务作用：以兼容语义构造绑定默认 datasource 的 MySQL Saga 完整后端。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不建连、四种角色均绑定 `default` 的组合句柄。
    fn default() -> Self {
        Self::new()
    }
}

impl MySqlSagaBackend {
    /// 业务作用：创建绑定默认 datasource 的 MySQL Saga 完整后端。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不建连、可直接交给运行核心的组合句柄。
    pub fn new() -> Self {
        let datasource = natx::DatasourceRef::default();
        Self {
            store: MySqlSagaStore::new(),
            inbox: MySqlInbox::new(),
            outbox: MySqlOutbox::new(),
            transaction_runner: MySqlSagaTransactionRunner::new(datasource.clone()),
            datasource,
        }
    }

    /// 业务作用：创建绑定命名 datasource 的 MySQL Saga 完整后端并复验四种角色同源。
    ///
    /// 参数说明：`datasource` 是启动期已注册的 MySQL datasource 名称。
    ///
    /// 返回：名称合法时返回同源组合；任一角色不能接受名称时在任何数据库 I/O 前失败。
    pub fn with_datasource(datasource: impl AsRef<str>) -> anyhow::Result<Self> {
        let datasource = natx::DatasourceRef::new(datasource)?;
        let store = MySqlSagaStore::with_datasource(&datasource)?;
        let inbox = MySqlInbox::with_datasource(&datasource)?;
        let outbox = MySqlOutbox::with_datasource(&datasource).map_err(anyhow::Error::new)?;
        Ok(Self {
            transaction_runner: MySqlSagaTransactionRunner::new(datasource.clone()),
            datasource,
            store,
            inbox,
            outbox,
        })
    }
}

impl SagaBackend for MySqlSagaBackend {
    type Store = MySqlSagaStore;
    type Inbox = MySqlInbox;
    type Outbox = MySqlOutbox;
    type TransactionRunner = MySqlSagaTransactionRunner;

    /// 业务作用：读取组合内的 MySQL Saga store。
    fn store(&self) -> &Self::Store {
        &self.store
    }

    /// 业务作用：读取与 Saga store 同源的 MySQL Inbox。
    fn inbox(&self) -> &Self::Inbox {
        &self.inbox
    }

    /// 业务作用：读取与 Saga store 同源的 MySQL Outbox。
    fn outbox(&self) -> &Self::Outbox {
        &self.outbox
    }

    /// 业务作用：读取绑定相同 datasource 的 MySQL 事务执行器。
    fn transaction_runner(&self) -> &Self::TransactionRunner {
        &self.transaction_runner
    }

    /// 业务作用：读取完整后端共享的 datasource qualifier。
    fn datasource_ref(&self) -> &natx::DatasourceRef {
        &self.datasource
    }

    /// 业务作用：声明完整后端固定使用 MySQL driver。
    fn driver(&self) -> natx::DatabaseDriver {
        natx::DatabaseDriver::MySql
    }
}

impl SagaBackendFactory for MySqlSagaBackend {
    /// 业务作用：为既有 MySQL 运行入口构造默认 datasource 组合。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：始终返回不建连的默认后端。
    fn default_backend() -> Result<Self, SagaBackendError> {
        Ok(Self::new())
    }

    /// 业务作用：为既有 MySQL 运行入口构造命名 datasource 组合。
    ///
    /// 参数说明：`datasource` 是 Saga、Inbox 与 Outbox 共享的 qualifier。
    ///
    /// 返回：名称合法时返回同源后端；否则在 I/O 前返回基础设施分类。
    fn backend_for(datasource: &str) -> Result<Self, SagaBackendError> {
        Self::with_datasource(datasource).map_err(|_| {
            SagaBackendError::new(
                SagaBackendErrorKind::Infrastructure,
                "MySQL Saga datasource identity is invalid",
            )
        })
    }
}
