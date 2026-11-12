//! MySQL Saga 宏与 hosted transport 保持的参与方分发合同。

use nasaga_core::ServiceIdentity;
use natelemetry::TraceContext;

use crate::{ParticipantHandled, ParticipantRuntime, SagaCommandEnvelope};

/// 业务作用：抽象宏生成的类型化步骤分发入口，供 MySQL hosted command consumer 绑定 Service。
///
/// 实现只能由 `#[saga]` 按已发布 descriptor 生成；它必须先执行精确
/// workflow/version/step 门禁，再进入 [`ParticipantRuntime`] 的唯一核心状态机。
pub trait SagaCommandService: Send + Sync + 'static {
    /// 业务作用：把一条已由 transport 认证并绑定 route 的命令交给完整参与方事务。
    ///
    /// 参数说明：
    /// - `runtime`：是 MySQL 参与方运行时
    /// - `envelope`：是命令
    /// - `producer`：是已认证 Orchestrator 身份。
    ///
    /// 返回：本地事务 COMMIT 后返回可 ACK 结论；合同、业务瞬态或基础设施错误返回错误。
    fn handle_saga_command<'a>(
        &'a self,
        runtime: &'a ParticipantRuntime,
        envelope: &'a SagaCommandEnvelope,
        producer: &'a ServiceIdentity,
    ) -> impl std::future::Future<Output = anyhow::Result<ParticipantHandled>> + Send + 'a;

    /// 业务作用：提供携带收据因果上下文的参与方入口；默认兼容实现委托无 trace 入口，生成适配可覆盖以传播上下文。
    ///
    /// 参数说明：
    /// - `runtime`：绑定 MySQL 本地事务能力的参与方运行时。
    /// - `envelope`：已认证且仍需业务合同复验的命令 envelope。
    /// - `producer`：transport 从可信凭据映射的编排服务身份。
    /// - `receipt_trace`：可选且已验证的收据因果上下文。
    ///
    /// 返回：默认实现忽略 receipt_trace，直接返回无 trace 入口的 future；需要传播因果上下文的实现必须覆盖本方法，事务结果由实际 Service 入口保证。
    fn handle_saga_command_traced<'a>(
        &'a self,
        runtime: &'a ParticipantRuntime,
        envelope: &'a SagaCommandEnvelope,
        producer: &'a ServiceIdentity,
        receipt_trace: Option<&'a TraceContext>,
    ) -> impl std::future::Future<Output = anyhow::Result<ParticipantHandled>> + Send + 'a {
        let _ = receipt_trace;
        self.handle_saga_command(runtime, envelope, producer)
    }
}
