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
    /// 参数说明：`runtime` 是 MySQL 参与方运行时，`envelope` 是命令，`producer` 是已认证 Orchestrator 身份。
    ///
    /// 返回：本地事务 COMMIT 后返回可 ACK 结论；合同、业务瞬态或基础设施错误返回错误。
    fn handle_saga_command<'a>(
        &'a self,
        runtime: &'a ParticipantRuntime,
        envelope: &'a SagaCommandEnvelope,
        producer: &'a ServiceIdentity,
    ) -> impl std::future::Future<Output = anyhow::Result<ParticipantHandled>> + Send + 'a;

    /// 业务作用：在同一参与方事务中携带已验证收据链路上下文。
    ///
    /// 默认实现保留旧宏展开的行为；当前宏会覆写本方法并把 trace 交给核心。
    ///
    /// 参数说明：`runtime`、`envelope`、`producer` 与无 trace 入口一致，`receipt_trace` 是可选受信收据上下文。
    ///
    /// 返回：语义与 [`handle_saga_command`](Self::handle_saga_command) 一致。
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
