//! Saga 运行时的显式本地事务裁决适配。

use std::future::Future;

use natx::{TxDecision, TxRunError};

/// 业务作用：保留 Saga 本地事务的封闭失败阶段，使 transport 不解析错误文本决定 ACK。
///
/// `CommitRejected`/`CommitUncertain`/`RollbackFailed` 均禁止进入有界 DLT 预算：
/// 前者需要原消息重试完成业务，后两者则无法声称数据库已回滚。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SagaTransactionError {
    /// 内层失败把事务标记为 rollback-only，最外层已确认回滚。
    RollbackOnly,
    /// 数据库明确拒绝 COMMIT，事务未提交但原输入不得 ACK。
    CommitRejected,
    /// COMMIT 请求后无法确定是否已持久。
    CommitUncertain,
    /// 物理回滚失败，不能声称本次无副作用。
    RollbackFailed,
    /// 事务开始前或执行内核的基础设施故障。
    Infrastructure,
}

impl SagaTransactionError {
    /// 业务作用：判断该阶段是否必须保留原输入且不消耗 DLT 预算。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：COMMIT 拒绝/不确定或回滚失败返回真；已确认回滚的其他基础故障返回假。
    pub fn requires_unbounded_redelivery(self) -> bool {
        matches!(
            self,
            Self::CommitRejected | Self::CommitUncertain | Self::RollbackFailed
        )
    }
}

impl std::fmt::Display for SagaTransactionError {
    /// 业务作用：输出不含 SQL、连接串或 payload 的稳定事务阶段。
    ///
    /// 参数说明：
    /// - `formatter`: 标准格式化输出目标。
    ///
    /// 返回：文本写入成功返回 `Ok`；格式化失败返回对应错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::RollbackOnly => "Saga transaction rollback-only",
            Self::CommitRejected => "Saga transaction commit rejected",
            Self::CommitUncertain => "Saga transaction commit uncertain",
            Self::RollbackFailed => "Saga transaction rollback failed",
            Self::Infrastructure => "Saga transaction infrastructure failed",
        })
    }
}

impl std::error::Error for SagaTransactionError {}

/// 业务作用：在默认 datasource 上执行 Saga 原子步骤，并保留 COMMIT 不确定与回滚失败分类。
///
/// Saga 的 Inbox、业务事实、状态迁移和 Outbox 必须共享此边界。普通领域错误会显式选择
/// `Rollback`；只有完整得到可提交结果才选择 `Commit`，避免依赖错误类型 downcast 决定事务。
///
/// 参数说明：
/// - `body`: 在 ambient transaction 内执行并返回领域结果的 Future。
///
/// 返回：数据库确认提交后返回领域值；领域失败保留原始错误；提交拒绝、提交不确定、
/// rollback-only、回滚失败和事务基础设施失败返回带稳定分类前缀的错误，调用方不得 ACK。
pub(crate) async fn run<T, F>(body: F) -> anyhow::Result<T>
where
    F: Future<Output = anyhow::Result<T>>,
{
    natx::run_decided(async move {
        match body.await {
            Ok(value) => TxDecision::Commit(value),
            Err(error) => TxDecision::Rollback(error),
        }
    })
    .await
    .map_err(map_error)
}

/// 业务作用：把 natx 的封闭事务阶段映射成 Saga 对外错误，同时保留领域回滚原始错误。
///
/// 参数说明：
/// - `error`: natx 返回的显式事务阶段错误。
///
/// 返回：领域回滚返回原错误；其余分支返回可 downcast 的封闭
/// [`SagaTransactionError`]，供消费循环精确决定不 ACK。
fn map_error(error: TxRunError<anyhow::Error>) -> anyhow::Error {
    match error {
        TxRunError::Rollback(error) => error,
        TxRunError::RollbackOnly { .. } => SagaTransactionError::RollbackOnly.into(),
        TxRunError::CommitRejected { .. } => SagaTransactionError::CommitRejected.into(),
        TxRunError::CommitUncertain { .. } => SagaTransactionError::CommitUncertain.into(),
        TxRunError::RollbackFailed { .. } => SagaTransactionError::RollbackFailed.into(),
        TxRunError::Infrastructure { .. } => SagaTransactionError::Infrastructure.into(),
    }
}
