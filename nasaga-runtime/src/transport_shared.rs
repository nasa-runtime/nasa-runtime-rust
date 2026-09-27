//! MySQL `#[saga]` Service 与后端中立 hosted transport handler 的适配。

use std::sync::Arc;

use nasaga_core::ServiceIdentity;
use nasaga_runtime_core::{ParticipantHandled, SagaCommandEnvelope, SagaCommandHandler};
use natelemetry::TraceContext;

use crate::{ParticipantRuntime, SagaCommandService};

/// 业务作用：把 MySQL Participant runtime 与宏生成 Service 组装为后端中立 command handler。
pub struct ParticipantCommandHandler<S> {
    runtime: Arc<ParticipantRuntime>,
    service: Arc<S>,
}

impl<S> ParticipantCommandHandler<S> {
    /// 业务作用：绑定 MySQL 参与方运行时与类型化 Saga Service，不启动消费任务。
    ///
    /// 参数说明：`runtime` 持有本地事务能力，`service` 持有精确步骤分发合同。
    ///
    /// 返回：可交给 Kafka、Redis Streams 或 gRPC connector 的轻量 handler。
    pub fn new(runtime: Arc<ParticipantRuntime>, service: Arc<S>) -> Self {
        Self { runtime, service }
    }
}

impl<S: SagaCommandService> SagaCommandHandler for ParticipantCommandHandler<S> {
    /// 业务作用：把已认证 command 交给 MySQL 参与方的完整本地事务。
    async fn handle_authenticated_command(
        &self,
        envelope: &SagaCommandEnvelope,
        producer: &ServiceIdentity,
    ) -> anyhow::Result<ParticipantHandled> {
        self.service
            .handle_saga_command(&self.runtime, envelope, producer)
            .await
    }

    /// 业务作用：把已验证收据 trace 与 command 一并交给 MySQL 参与方事务。
    async fn handle_authenticated_command_traced(
        &self,
        envelope: &SagaCommandEnvelope,
        producer: &ServiceIdentity,
        receipt_trace: Option<&TraceContext>,
    ) -> anyhow::Result<ParticipantHandled> {
        self.service
            .handle_saga_command_traced(&self.runtime, envelope, producer, receipt_trace)
            .await
    }
}
