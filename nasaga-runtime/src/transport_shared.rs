//! Saga transport 共享合同:与具体 broker/stream 实现无关的 handler 抽象。
//!
//! Kafka 与 Redis Streams connector 共用同一组 handler trait 与适配器,保证两种
//! transport 对同一错误给出一致的前移结论;各自的 ACK/offset/PEL 专用语义留在
//! 各自模块内。

use std::future::Future;
use std::sync::Arc;

use nasaga_core::ServiceIdentity;
use natelemetry::TraceContext;

use crate::{
    HandleOutcome, Orchestrator, ParticipantHandled, ParticipantRuntime, SagaCommandEnvelope,
    SagaCommandService, SagaResultEnvelope,
};

/// 业务作用：抽象“认证且精确绑定 route 后处理 command”的本地提交边界。
///
/// transport 只会在 envelope 身份派生复验、topic owner 认证和 workflow/version/step
/// 精确匹配全部通过后调用本 trait；实现返回成功必须意味着 Participant 本地事务已 COMMIT。
pub trait SagaCommandHandler: Send + Sync + 'static {
    /// 业务作用：在受信 Orchestrator 来源下执行一条已绑定目标步骤的 Saga command。
    ///
    /// 参数说明：
    /// - `envelope`: 已复验身份并精确匹配 topic route 的 command envelope。
    /// - `producer`: 来源 topic 在冻结信任表中绑定的 Orchestrator 逻辑身份。
    ///
    /// 返回：Inbox、业务/gate 与结果 Outbox 提交后返回可 ACK 结论；未提交返回错误。
    fn handle_authenticated_command(
        &self,
        envelope: &SagaCommandEnvelope,
        producer: &ServiceIdentity,
    ) -> impl Future<Output = anyhow::Result<ParticipantHandled>> + Send;

    /// 业务作用：携带收据链路上下文的命令处理入口；transport 把消息收据中显式解析出的
    /// trace 交给实现，参与方写 result Outbox 时据此派生子上下文。
    ///
    /// 默认实现忽略收据上下文并委托给
    /// [`handle_authenticated_command`](Self::handle_authenticated_command)，保证旧实现在
    /// 滚动升级期间继续满足本 trait；具备 trace 能力的实现应覆写本方法。
    ///
    /// 参数说明：
    /// - `envelope`: 已复验身份并精确匹配 topic route 的 command envelope。
    /// - `producer`: 来源 topic 在冻结信任表中绑定的 Orchestrator 逻辑身份。
    /// - `receipt_trace`: 收据中已校验的链路上下文；`None` 表示收据未携带。
    ///
    /// 返回：语义与 [`handle_authenticated_command`](Self::handle_authenticated_command) 一致。
    fn handle_authenticated_command_traced(
        &self,
        envelope: &SagaCommandEnvelope,
        producer: &ServiceIdentity,
        receipt_trace: Option<&TraceContext>,
    ) -> impl Future<Output = anyhow::Result<ParticipantHandled>> + Send {
        let _ = receipt_trace;
        self.handle_authenticated_command(envelope, producer)
    }
}

/// 业务作用：把单个 `#[saga]` Service 与 Participant runtime 组装成 hosted command handler。
///
/// 单服务进程可直接使用本适配器；同一进程托管多个步骤时可实现 [`SagaCommandHandler`]
/// 做显式类型化路由，并继续复用同一个 [`crate::SagaKafkaCommandConsumer`]。
pub struct ParticipantCommandHandler<S> {
    /// 持有本地 Inbox/gate/Outbox 事务能力的参与方运行时。
    runtime: Arc<ParticipantRuntime>,
    /// 宏生成了精确 descriptor 与 phase 分发入口的业务 Service。
    service: Arc<S>,
}

impl<S> ParticipantCommandHandler<S> {
    /// 业务作用：绑定参与方运行时与类型化 Saga Service，不启动消费任务。
    ///
    /// 参数说明：
    /// - `runtime`: 本服务唯一的 Participant runtime。
    /// - `service`: 带 `#[saga]` descriptor/adapter 的业务 Service。
    ///
    /// 返回：可交给 hosted command consumer 的轻量 handler。
    pub fn new(runtime: Arc<ParticipantRuntime>, service: Arc<S>) -> Self {
        Self { runtime, service }
    }
}

impl<S: SagaCommandService> SagaCommandHandler for ParticipantCommandHandler<S> {
    /// 业务作用：把已认证 command 转交给宏生成的精确步骤 adapter 与本地事务 wrapper。
    ///
    /// 参数说明：
    /// - `envelope`: 已由 transport 认证并绑定 route 的 command。
    /// - `producer`: 已认证 Orchestrator 身份；Participant runtime 会再次与启动白名单交叉校验。
    ///
    /// 返回：Participant 事务提交后返回可 ACK 结论；否则透传类型化或瞬态错误。
    async fn handle_authenticated_command(
        &self,
        envelope: &SagaCommandEnvelope,
        producer: &ServiceIdentity,
    ) -> anyhow::Result<ParticipantHandled> {
        self.service
            .handle_saga_command(&self.runtime, envelope, producer)
            .await
    }

    /// 业务作用：把收据 trace 显式转交宏生成的类型化分发入口，供结果事件派生子上下文。
    ///
    /// 参数说明：
    /// - `envelope`: 已由 transport 认证并绑定 route 的 command。
    /// - `producer`: 已认证 Orchestrator 身份。
    /// - `receipt_trace`: 收据中已校验的链路上下文。
    ///
    /// 返回：Participant 事务提交后返回可 ACK 结论；否则透传类型化或瞬态错误。
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

/// 业务作用：抽象“认证后处理结果”的提交边界，使 Kafka transport 不依赖具体数据库实现。
///
/// 默认实现由 [`Orchestrator`] 提供；自定义实现只能替代本地提交动作，不能改变 ACK/DLT 规则。
pub trait SagaResultHandler: Send + Sync + 'static {
    /// 业务作用：校验 producer owner，并在单一本地事务内吸收或推进一条 Saga 结果。
    ///
    /// 参数说明：
    /// - `envelope`: 已由 Kafka JSON codec 解码的结果 envelope。
    /// - `producer`: 来源 topic 经冻结信任表映射出的逻辑服务身份。
    /// - `now_ms`: 当前 epoch 毫秒。
    ///
    /// 返回：事务提交确认后返回可 ACK 结论；认证、合同或事务失败返回错误。
    fn handle_authenticated_result(
        &self,
        envelope: &SagaResultEnvelope,
        producer: &ServiceIdentity,
        now_ms: i64,
    ) -> impl Future<Output = anyhow::Result<HandleOutcome>> + Send;

    /// 业务作用：携带收据链路上下文的结果处理入口；transport 把消息收据中显式解析出的
    /// trace 交给实现，实现据此续接同一 trace。
    ///
    /// 默认实现忽略收据上下文并委托给
    /// [`handle_authenticated_result`](Self::handle_authenticated_result)，保证旧实现
    /// 在滚动升级期间继续满足本 trait；具备 trace 能力的实现应覆写本方法。
    ///
    /// 参数说明：
    /// - `envelope`: 已由 Kafka JSON codec 解码的结果 envelope。
    /// - `producer`: 来源 topic 经冻结信任表映射出的逻辑服务身份。
    /// - `receipt_trace`: 收据中已校验的链路上下文；`None` 表示收据未携带。
    /// - `now_ms`: 当前 epoch 毫秒。
    ///
    /// 返回：语义与 [`handle_authenticated_result`](Self::handle_authenticated_result) 一致。
    fn handle_authenticated_result_traced(
        &self,
        envelope: &SagaResultEnvelope,
        producer: &ServiceIdentity,
        receipt_trace: Option<&TraceContext>,
        now_ms: i64,
    ) -> impl Future<Output = anyhow::Result<HandleOutcome>> + Send {
        let _ = receipt_trace;
        self.handle_authenticated_result(envelope, producer, now_ms)
    }
}

impl SagaResultHandler for Orchestrator {
    /// 业务作用：把 transport 抽象转交给 Orchestrator 的 producer 认证与事务推进入口。
    ///
    /// 参数说明：
    /// - `envelope`: 参与方结果。
    /// - `producer`: transport 已认证逻辑身份。
    /// - `now_ms`: 当前 epoch 毫秒。
    ///
    /// 返回：透传 Orchestrator 的提交确认或脱敏错误。
    async fn handle_authenticated_result(
        &self,
        envelope: &SagaResultEnvelope,
        producer: &ServiceIdentity,
        now_ms: i64,
    ) -> anyhow::Result<HandleOutcome> {
        Orchestrator::handle_authenticated_result(self, envelope, producer, now_ms).await
    }

    /// 业务作用：把收据 trace 显式转交 Orchestrator——结果推进事务据此更新实例因果上下文。
    ///
    /// 参数说明：
    /// - `envelope`: 参与方结果。
    /// - `producer`: transport 已认证逻辑身份。
    /// - `receipt_trace`: 收据中已校验的链路上下文。
    /// - `now_ms`: 当前 epoch 毫秒。
    ///
    /// 返回：透传 Orchestrator 的提交确认或脱敏错误。
    async fn handle_authenticated_result_traced(
        &self,
        envelope: &SagaResultEnvelope,
        producer: &ServiceIdentity,
        receipt_trace: Option<&TraceContext>,
        now_ms: i64,
    ) -> anyhow::Result<HandleOutcome> {
        Orchestrator::handle_authenticated_result_traced(
            self,
            envelope,
            producer,
            receipt_trace,
            now_ms,
        )
        .await
    }
}
