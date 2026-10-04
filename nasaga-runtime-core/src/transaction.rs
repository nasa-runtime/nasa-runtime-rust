//! Saga 运行时的显式本地事务裁决适配。

use std::future::Future;

use nasaga_backend::{SagaBackend, SagaTransactionRunError, SagaTransactionRunner};

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

/// 业务作用：在指定 datasource 上执行 Saga 原子步骤，并保留 COMMIT 不确定与回滚失败分类。
///
/// 参数说明：
/// - `datasource`: Saga store、Inbox 与 Outbox 共同绑定的数据源身份。
/// - `body`: 在同源 ambient transaction 内执行并返回领域结果的 Future。
///
/// 返回：数据库确认提交后返回领域值；跨源、回滚、提交不确定或基础设施失败返回封闭分类。
pub(crate) async fn run_for<B, T, F>(backend: &B, body: F) -> anyhow::Result<T>
where
    B: SagaBackend,
    F: Future<Output = anyhow::Result<T>> + Send,
    T: Send,
{
    backend
        .transaction_runner()
        .run(Box::pin(body))
        .await
        .map_err(map_error)
}

/// 业务作用：在事务体开始、每次恢复轮询及交还提交裁决前复验执行资格，等待期间失权则回滚全部事务事实。
///
/// 参数说明：`backend` 固定事务域，`authorize` 同步验证本次操作的冻结资格，`body` 只能产生同事务数据库写入。
///
/// 返回：资格持续有效且提交确认后返回领域值；失权使事务体返回错误并走正常回滚。
/// 已在有效资格下发出的 COMMIT 仍按数据库收据裁决，不因确认回包较晚而宣称回滚。
pub(crate) async fn run_authorized_for<B, T, F>(
    backend: &B,
    authorize: &(dyn Fn() -> anyhow::Result<()> + Send + Sync),
    body: F,
) -> anyhow::Result<T>
where
    B: SagaBackend,
    F: Future<Output = anyhow::Result<T>> + Send,
    T: Send,
{
    run_for(backend, async {
        let outcome = {
            let mut body = Box::pin(body);
            std::future::poll_fn(|context| {
                // BEGIN、连接池或行锁都可能消耗租期；恢复数据库操作前必须重新取得同一资格。
                if let Err(error) = authorize() {
                    return std::task::Poll::Ready(Err(error));
                }
                body.as_mut().poll(context)
            })
            .await
        }?;
        // 先释放事务体的连接借用，再决定提交；失权不能留下 Inbox、journal、实例、Outbox 或 timer 的半份事实。
        authorize()?;
        Ok(outcome)
    })
    .await
}

/// 业务作用：把 natx 的封闭事务阶段映射成 Saga 对外错误，同时保留领域回滚原始错误。
///
/// 参数说明：
/// - `error`: natx 返回的显式事务阶段错误。
///
/// 返回：领域回滚返回原错误；其余分支返回可 downcast 的封闭
/// [`SagaTransactionError`]，供消费循环精确决定不 ACK。
fn map_error(error: SagaTransactionRunError<anyhow::Error>) -> anyhow::Error {
    match error {
        SagaTransactionRunError::Rollback(error) => error,
        SagaTransactionRunError::RollbackOnly => SagaTransactionError::RollbackOnly.into(),
        SagaTransactionRunError::CommitRejected => SagaTransactionError::CommitRejected.into(),
        SagaTransactionRunError::CommitUncertain => SagaTransactionError::CommitUncertain.into(),
        SagaTransactionRunError::RollbackFailed => SagaTransactionError::RollbackFailed.into(),
        SagaTransactionRunError::Infrastructure => SagaTransactionError::Infrastructure.into(),
    }
}
